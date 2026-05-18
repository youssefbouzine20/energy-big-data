"""
storage/query_patterns.py
=========================

Module de requêtes pré-construites et optimisées pour le dashboard P4
et le module ML P5. Chaque fonction encapsule un pattern d'accès fréquent
avec les indexes appropriés, garantissant des latences <50ms pour le
dashboard (exigence P3 README §2, tableau des 5V).

Les requêtes utilisent systématiquement les indexes composites définis
dans init-mongo.js et extend_init.py, dans le bon ordre (field d'égalité
d'abord, puis range/tri).

MISE À JOUR 2026-05-17: Aligné sur les documents ACTUELLEMENT produits
par P2 (Spark). Les champs théoriques du README (correlated_anomalies,
correlated_voltage_drops, nlp_sentiment, zone_balance_ratio) sont ABSENTS
du output P2 actuel et donc retirés des projections/requêtes.

Références:
- P3 README §6: "Compound key order matters"
- P3 README §5: Document schemas exacts
- P3 README §11 pitfall #4: "Index not used by query"
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple

from pymongo import ASCENDING, DESCENDING

from storage.mongo_client import (
    get_meters_aggregated_collection,
    get_meters_raw_collection,
    get_incidents_enriched_collection,
    get_feedback_nlp_collection,
    get_data_quality_collection,
    get_weather_collection,
    get_market_prices_collection,
    get_ml_predictions_collection,
)


# ============================================================================
# STEP 1: Patterns Dashboard P4 — Heatmap & KPI Cards
# ============================================================================
# Ces requêtes alimentent les widgets temps réel du dashboard.
# Elles doivent être exécutées sur SECONDARY en réplica set.

def get_latest_zone_aggregates(
    zones: Optional[List[str]] = None,
    window_count: int = 4,
    as_of: Optional[datetime] = None
) -> List[Dict[str, Any]]:
    """
    Récupère les dernières fenêtres 15min agrégées par zone.

    Utilisé par: KPI cards (consommation moyenne, taux d'anomalie)
    et heatmap (couleur par zone).

    Index utilisé: (zone, window_start) — DESCENDING pour latest first.
    Latence attendue: <20ms avec index.

    Args:
        zones: Liste de zones ["A", "B", "C", "D"] ou None pour toutes.
        window_count: Nombre de fenêtres temporelles à retourner par zone.
        as_of: Timestamp limite (défaut: maintenant).

    Returns:
        Liste de documents meters_aggregated_15min enrichis.

    Exemple:
        >>> latest = get_latest_zone_aggregates(zones=["A", "B"], window_count=2)
        >>> # Retourne 4 documents (2 zones × 2 fenêtres)
    """
    coll = get_meters_aggregated_collection()

    if as_of is None:
        as_of = datetime.utcnow()

    # Fenêtre la plus récente complète avant as_of
    # Les fenêtres sont alignées sur 15min: 00, 15, 30, 45
    latest_window = as_of.replace(minute=(as_of.minute // 15) * 15, second=0, microsecond=0)

    match_stage = {"window_start": {"$lte": latest_window}}
    if zones:
        match_stage["zone"] = {"$in": zones}

    # Pipeline d'agrégation: $sort + $limit par zone (efficace avec index composite)
    pipeline = [
        {"$match": match_stage},
        {"$sort": {"zone": ASCENDING, "window_start": DESCENDING}},
        {
            "$group": {
                "_id": "$zone",
                "latest_windows": {
                    "$push": {
                        "window_start": "$window_start",
                        "avg_consumption": "$avg_consumption",
                        "anomaly_rate_pct": "$anomaly_rate_pct",
                        "active_incidents": "$active_incidents",
                        "weather_temperature_c": "$weather_temperature_c",
                        "weather_severity": "$weather_severity",
                        "meter_count": "$meter_count",
                        "zone_lat": "$zone_lat",
                        "zone_lon": "$zone_lon"
                    }
                }
            }
        },
        {"$project": {
            "zone": "$_id",
            "latest_windows": {"$slice": ["$latest_windows", window_count]},
            "_id": 0
        }}
    ]

    return list(coll.aggregate(pipeline))


def get_zone_timeseries(
    zone: str,
    start: datetime,
    end: datetime,
    metric: str = "avg_consumption"
) -> List[Dict[str, Any]]:
    """
    Série temporelle d'une métrique pour une zone donnée.

    Utilisé par: Graphique linéaire "Consommation sur 24h" du dashboard.

    Index utilisé: (zone, window_start) — match exact sur zone + range sur window_start.

    Args:
        zone: Zone unique ("A" | "B" | "C" | "D").
        start: Début de la fenêtre temporelle (inclus).
        end: Fin de la fenêtre temporelle (inclus).
        metric: Champ numérique à extraire (avg_consumption, anomaly_rate_pct, etc.).

    Returns:
        Liste chronologique [{window_start, value}, ...].
    """
    coll = get_meters_aggregated_collection()

    # Projection dynamique du champ demandé
    projection = {"window_start": 1, metric: 1, "_id": 0}

    cursor = coll.find(
        {
            "zone": zone,
            "window_start": {"$gte": start, "$lte": end}
        },
        projection
    ).sort("window_start", ASCENDING)

    return list(cursor)


def get_peak_consumption_alert(zones: Optional[List[str]] = None,
                                threshold_multiplier: float = 1.5,
                                lookback_windows: int = 4) -> List[Dict[str, Any]]:
    """
    Détecte les zones en surconsommation par rapport à leur historique récent.

    Utilisé par: Widget "Alertes actives" du dashboard.

    Logique: Si avg_consumption > (moyenne des N dernières fenêtres × threshold),
    alors alerte PEAK_CONSUMPTION.

    Args:
        zones: Zones à surveiller ou None pour toutes.
        threshold_multiplier: Multiplicateur de détection (1.5 = +50%).
        lookback_windows: Nombre de fenêtres historiques pour la baseline.

    Returns:
        Liste des alertes avec zone, current_value, baseline, ratio.
    """
    coll = get_meters_aggregated_collection()

    match = {}
    if zones:
        match["zone"] = {"$in": zones}

    pipeline = [
        {"$match": match},
        {"$sort": {"zone": ASCENDING, "window_start": DESCENDING}},
        {
            "$group": {
                "_id": "$zone",
                "windows": {
                    "$push": {
                        "window_start": "$window_start",
                        "avg_consumption": "$avg_consumption"
                    }
                }
            }
        },
        {"$project": {
            "zone": "$_id",
            "current": {"$arrayElemAt": ["$windows", 0]},
            "history": {"$slice": ["$windows", 1, lookback_windows]},
            "_id": 0
        }},
        {"$project": {
            "zone": 1,
            "current_value": "$current.avg_consumption",
            "baseline": {"$avg": "$history.avg_consumption"},
            "window_start": "$current.window_start"
        }},
        {"$match": {"baseline": {"$gt": 0}}},
        {"$project": {
            "zone": 1,
            "current_value": 1,
            "baseline": 1,
            "ratio": {"$divide": ["$current_value", "$baseline"]},
            "window_start": 1
        }},
        {"$match": {"ratio": {"$gte": threshold_multiplier}}},
        {"$sort": {"ratio": DESCENDING}}
    ]

    return list(coll.aggregate(pipeline))


# ============================================================================
# STEP 2: Patterns P5 ML — Features & Training Data
# ============================================================================
# Ces requêtes extraient des datasets structurés pour l'entraînement
# et l'inférence des modèles ML.

def get_ml_training_features(
    zone: str,
    start: datetime,
    end: datetime,
    include_weather: bool = True,
    include_market: bool = True
) -> List[Dict[str, Any]]:
    """
    Extrait un dataset ML complet (features + target) pour entraînement.

    Utilisé par: P5 ML — entraînement modèle RandomForest/Regression.

    Features extraites:
    - avg_consumption (target: prédire prochaine fenêtre)
    - weather_temperature_c, weather_severity
    - active_incidents, anomaly_rate_pct
    - hour_of_day, day_of_week (dérivés de window_start)
    - market price (si include_market=True)

    Index utilisé: (zone, window_start) — range query optimisé.

    Args:
        zone: Zone d'entraînement.
        start: Début de la période d'entraînement.
        end: Fin de la période.
        include_weather: Jointure avec collection weather (broadcast).
        include_market: Jointure avec collection market_prices.

    Returns:
        Liste de documents feature-rich pour pandas.DataFrame().
    """
    agg_coll = get_meters_aggregated_collection()

    # Base query sur meters_aggregated_15min
    base_features = list(agg_coll.find(
        {"zone": zone, "window_start": {"$gte": start, "$lte": end}},
        {
            "window_start": 1,
            "avg_consumption": 1,
            "max_consumption": 1,
            "anomaly_rate_pct": 1,
            "active_incidents": 1,
            "weather_temperature_c": 1,
            "weather_severity": 1,
            "meter_count": 1,
            "_id": 0
        }
    ).sort("window_start", ASCENDING))

    if not include_weather and not include_market:
        return base_features

    # Enrichissement weather (optionnel)
    if include_weather:
        weather_coll = get_weather_collection()
        # Récupère les lectures météo dans la même fenêtre temporelle
        weather_data = {
            w["timestamp"].replace(minute=(w["timestamp"].minute // 15) * 15, second=0, microsecond=0): w
            for w in weather_coll.find(
                {"timestamp": {"$gte": start, "$lte": end}},
                {"timestamp": 1, "temperature_c": 1, "humidity_pct": 1, 
                 "solar_irradiance_wm2": 1, "wind_speed_ms": 1, "_id": 0}
            )
        }

        for feat in base_features:
            window = feat["window_start"]
            if window in weather_data:
                w = weather_data[window]
                feat["weather_humidity"] = w.get("humidity_pct")
                feat["weather_solar"] = w.get("solar_irradiance_wm2")
                feat["weather_wind"] = w.get("wind_speed_ms")

    # Enrichissement market prices (optionnel)
    if include_market:
        market_coll = get_market_prices_collection()
        # Agrégation horaire des prix pour matcher les fenêtres 15min
        market_data = {
            m["timestamp"].replace(minute=0, second=0, microsecond=0): m
            for m in market_coll.find(
                {"timestamp": {"$gte": start, "$lte": end}},
                {"timestamp": 1, "price_mad_mwh": 1, "price_eur_mwh": 1,
                 "renewable_share_pct": 1, "trend": 1, "_id": 0}
            )
        }

        for feat in base_features:
            hour_key = feat["window_start"].replace(minute=0, second=0, microsecond=0)
            if hour_key in market_data:
                m = market_data[hour_key]
                feat["market_price_mad"] = m.get("price_mad_mwh")
                feat["market_renewable_pct"] = m.get("renewable_share_pct")
                feat["market_trend"] = m.get("trend")

    return base_features


def get_raw_meter_window(
    zone: str,
    window_start: datetime,
    window_end: datetime
) -> List[Dict[str, Any]]:
    """
    Récupère les lectures brutes individuelles dans une fenêtre 15min.

    Utilisé par: P5 ML — feature engineering granulaire, ou debug P3/P4.

    Index utilisé: (zone, timestamp) — range query.

    Args:
        zone: Zone cible.
        window_start: Début de la fenêtre (inclus).
        window_end: Fin de la fenêtre (exclus).

    Returns:
        Liste des documents meters_raw dans la fenêtre.
    """
    coll = get_meters_raw_collection()

    return list(coll.find(
        {
            "zone": zone,
            "timestamp": {"$gte": window_start, "$lt": window_end}
        },
        {"_id": 0}  # Exclure _id pour alléger le payload
    ).sort("timestamp", ASCENDING))


# ============================================================================
# STEP 3: Patterns NLP & Sentiment — Word Cloud & Charts
# ============================================================================

def get_nlp_keyword_frequency(
    zones: Optional[List[str]] = None,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    top_k: int = 20
) -> List[Dict[str, Any]]:
    """
    Agrégation des mots-clés NLP par fréquence d'apparition.

    Utilisé par: Widget "Word Cloud" du dashboard (incidents).

    Index utilisé: (nlp_keywords, 1) — index dédié pour cette requête.

    NOTE: P2 écrit les mots-clés dans le champ "nlp_keywords" (slice des tokens).
    Les tokens bruts sont dans "tokens" mais ne sont pas indexés.

    Args:
        zones: Filtre par zone ou None pour toutes.
        start: Filtre temporel optionnel.
        end: Filtre temporel optionnel.
        top_k: Nombre de mots-clés à retourner.

    Returns:
        Liste [{keyword, count, zones}, ...] triée par count DESC.
    """
    coll = get_incidents_enriched_collection()

    match = {}
    if zones:
        match["zone"] = {"$in": zones}
    if start and end:
        match["timestamp"] = {"$gte": start, "$lte": end}

    pipeline = [
        {"$match": match} if match else {"$match": {}},
        {"$unwind": "$nlp_keywords"},
        {"$group": {
            "_id": "$nlp_keywords",
            "count": {"$sum": 1},
            "zones": {"$addToSet": "$zone"}
        }},
        {"$project": {
            "keyword": "$_id",
            "count": 1,
            "zones": 1,
            "_id": 0
        }},
        {"$sort": {"count": DESCENDING}},
        {"$limit": top_k}
    ]

    return list(coll.aggregate(pipeline))


def get_sentiment_distribution(
    zones: Optional[List[str]] = None,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    group_by: str = "overall"  # "overall" | "zone" | "channel"
) -> List[Dict[str, Any]]:
    """
    Distribution des sentiments prédits sur les feedbacks utilisateurs.

    Utilisé par: Widget "Sentiment Analysis" du dashboard.

    Index utilisé: (sentiment_predicted, 1, timestamp: -1) ou (zone, timestamp).

    NOTE: P2 écrit "sentiment_input" (ground truth) et "sentiment_predicted" (NLP).
    Le champ "sentiment" original est renommé par streams_feedback.py.

    Args:
        zones: Filtre par zone.
        start: Filtre temporel.
        end: Filtre temporel.
        group_by: Niveau d'agrégation (global, par zone, ou par canal).

    Returns:
        Liste de buckets sentiment avec count et pourcentage.
    """
    coll = get_feedback_nlp_collection()

    match = {}
    if zones:
        match["zone"] = {"$in": zones}
    if start and end:
        match["timestamp"] = {"$gte": start, "$lte": end}

    group_id = "$sentiment_predicted"
    if group_by == "zone":
        group_id = {"sentiment": "$sentiment_predicted", "zone": "$zone"}
    elif group_by == "channel":
        group_id = {"sentiment": "$sentiment_predicted", "channel": "$channel"}

    pipeline = [
        {"$match": match} if match else {"$match": {}},
        {"$group": {
            "_id": group_id,
            "count": {"$sum": 1}
        }},
        {"$sort": {"count": DESCENDING}}
    ]

    results = list(coll.aggregate(pipeline))

    # Calcul des pourcentages
    total = sum(r["count"] for r in results)
    if total > 0:
        for r in results:
            r["pct"] = round((r["count"] / total) * 100, 2)

    return results


# ============================================================================
# STEP 4: Patterns Data Quality — Section E Badge
# ============================================================================

def get_latest_quality_metrics(
    topics: Optional[List[str]] = None,
    window_count: int = 1
) -> List[Dict[str, Any]]:
    """
    Récupère les dernières métriques de qualité par topic.

    Utilisé par: Badge "Data Quality: 98%" du dashboard.

    Index utilisé: (topic, window_start) — DESCENDING.

    NOTE: P2 écrit: temporal_coverage_pct, completeness_pct, noise_rate_pct,
    anomaly_rate_pct, topic, window_start, alert, computed_at.
    Le champ "zone_balance_ratio" du README §2.7 est ABSENT du output P2.

    Args:
        topics: Liste de topics ou None pour tous.
        window_count: Nombre de fenêtres récentes par topic.

    Returns:
        Liste des documents data_quality_metrics avec alertes.
    """
    coll = get_data_quality_collection()

    match = {}
    if topics:
        match["topic"] = {"$in": topics}

    pipeline = [
        {"$match": match},
        {"$sort": {"topic": ASCENDING, "window_start": DESCENDING}},
        {
            "$group": {
                "_id": "$topic",
                "latest": {
                    "$push": {
                        "window_start": "$window_start",
                        "completeness_pct": "$completeness_pct",
                        "noise_rate_pct": "$noise_rate_pct",
                        "anomaly_rate_pct": "$anomaly_rate_pct",
                        "temporal_coverage_pct": "$temporal_coverage_pct",
                        "alert": "$alert"
                    }
                }
            }
        },
        {"$project": {
            "topic": "$_id",
            "metrics": {"$slice": ["$latest", window_count]},
            "_id": 0
        }}
    ]

    return list(coll.aggregate(pipeline))


def get_quality_trend(
    topic: str,
    metric: str = "completeness_pct",
    hours: int = 24
) -> List[Dict[str, Any]]:
    """
    Série temporelle d'une métrique de qualité sur N heures.

    Utilisé par: Graphique "Quality Trend" du dashboard.

    Args:
        topic: Topic Kafka (smart-meters, weather, etc.).
        metric: Champ numérique à tracer.
        hours: Fenêtre temporelle en heures.

    Returns:
        Liste [{window_start, value}, ...] chronologique.
    """
    coll = get_data_quality_collection()

    start = datetime.utcnow() - timedelta(hours=hours)

    projection = {"window_start": 1, metric: 1, "_id": 0}

    return list(coll.find(
        {"topic": topic, "window_start": {"$gte": start}},
        projection
    ).sort("window_start", ASCENDING))


# ============================================================================
# STEP 5: Patterns Cross-Collection — Correlation & Audit
# ============================================================================

def get_incident_meter_correlation(
    incident_id: str
) -> Dict[str, Any]:
    """
    Corrèle un incident enrichi avec les lectures brutes des compteurs
    affectés pendant la fenêtre de l'incident.

    Utilisé par: Page "Incident Detail" du dashboard — drill-down.

    NOTE: P2 n'écrit PAS correlated_anomalies ni correlated_voltage_drops
    dans incidents_enriched (contrairement au README §5.3).
    Cette fonction calcule ces corrélations à la volée depuis meters_raw.

    Args:
        incident_id: ID de l'incident (INC-YYYYMMDD-NNN).

    Returns:
        Dict avec incident + lectures corrélées + anomalies calculées.
    """
    inc_coll = get_incidents_enriched_collection()
    meter_coll = get_meters_raw_collection()

    # Récupération de l'incident
    incident = inc_coll.find_one({"incident_id": incident_id})
    if not incident:
        return {"error": f"Incident {incident_id} non trouvé"}

    # Fenêtre temporelle: 15min avant l'incident → durée estimée après
    incident_time = incident["timestamp"]
    duration = incident.get("estimated_duration_min", 60)

    start_window = incident_time - timedelta(minutes=15)
    end_window = incident_time + timedelta(minutes=duration)

    # Récupération des compteurs affectés
    affected_meters = incident.get("affected_meters", [])

    # Récupération des lectures brutes
    meter_readings = list(meter_coll.find({
        "meter_id": {"$in": affected_meters},
        "timestamp": {"$gte": start_window, "$lte": end_window}
    }).sort("timestamp", ASCENDING))

    # Calcul des corrélations (P2 ne les écrit pas, on les calcule ici)
    correlated_anomalies = sum(1 for r in meter_readings if r.get("is_anomaly"))
    correlated_voltage_drops = sum(
        1 for r in meter_readings 
        if r.get("anomaly_reason") in ["VOLTAGE_LOW", "VOLTAGE_HIGH"]
    )

    return {
        "incident": {
            "incident_id": incident["incident_id"],
            "zone": incident["zone"],
            "severity": incident["severity"],
            "type": incident["type"],
            "description": incident["description"],
            "timestamp": incident["timestamp"],
            "estimated_duration_min": duration,
            "correlated_anomalies": correlated_anomalies,       # ← Calculé à la volée
            "correlated_voltage_drops": correlated_voltage_drops  # ← Calculé à la volée
        },
        "affected_meters": affected_meters,
        "reading_count": len(meter_readings),
        "readings": meter_readings[:100]  # Limite pour éviter les payloads massifs
    }


def get_prediction_vs_actual(
    zone: str,
    model_name: Optional[str] = None,
    windows: int = 10
) -> List[Dict[str, Any]]:
    """
    Comparaison prédictions P5 vs réalité pour évaluation du modèle.

    Utilisé par: Widget "Prediction vs Actual" du dashboard.

    Jointure implicite sur (zone, forecast_for) ≈ (zone, window_start).

    Args:
        zone: Zone cible.
        model_name: Filtre par modèle ou None pour tous.
        windows: Nombre de fenêtres à comparer.

    Returns:
        Liste de dicts {window, actual, predicted, error_pct}.
    """
    pred_coll = get_ml_predictions_collection()
    agg_coll = get_meters_aggregated_collection()

    # Récupère les dernières prédictions
    pred_filter = {"zone": zone}
    if model_name:
        pred_filter["model_name"] = model_name

    predictions = list(pred_coll.find(
        pred_filter,
        {"forecast_for": 1, "consumption_forecast": 1, "model_name": 1, "_id": 0}
    ).sort("forecast_for", DESCENDING).limit(windows))

    # Récupère les valeurs réelles correspondantes
    forecast_dates = [p["forecast_for"] for p in predictions]

    actuals = {
        a["window_start"]: a["avg_consumption"]
        for a in agg_coll.find(
            {"zone": zone, "window_start": {"$in": forecast_dates}},
            {"window_start": 1, "avg_consumption": 1, "_id": 0}
        )
    }

    # Assemblage
    results = []
    for p in predictions:
        actual = actuals.get(p["forecast_for"])
        predicted = p["consumption_forecast"]

        result = {
            "window": p["forecast_for"],
            "model": p.get("model_name", "unknown"),
            "predicted": predicted,
            "actual": actual,
            "error_pct": None
        }

        if actual is not None and actual > 0:
            result["error_pct"] = round(abs((predicted - actual) / actual) * 100, 2)

        results.append(result)

    return sorted(results, key=lambda x: x["window"])


# ============================================================================
# STEP 6: Point d'entrée CLI — Tests rapides
# ============================================================================

if __name__ == "__main__":
    from datetime import datetime, timedelta

    print(f"\n{'='*60}")
    print("QUERY PATTERNS — Test rapide (aligné sur output P2 actuel)")
    print(f"{'='*60}\n")

    # Test 1: Dernières agrégations zone
    print("Test 1: get_latest_zone_aggregates(zones=['A', 'B'], window_count=1)")
    try:
        latest = get_latest_zone_aggregates(zones=["A", "B"], window_count=1)
        print(f"  ✅ Retourné {len(latest)} zones")
        for z in latest:
            print(f"     Zone {z['zone']}: {len(z.get('latest_windows', []))} fenêtres")
    except Exception as e:
        print(f"  ❌ Erreur: {e}")

    # Test 2: Série temporelle
    print("\nTest 2: get_zone_timeseries('A', last 24h)")
    try:
        end = datetime.utcnow()
        start = end - timedelta(hours=24)
        ts = get_zone_timeseries("A", start, end)
        print(f"  ✅ Retourné {len(ts)} points")
    except Exception as e:
        print(f"  ❌ Erreur: {e}")

    # Test 3: Qualité
    print("\nTest 3: get_latest_quality_metrics()")
    try:
        quality = get_latest_quality_metrics()
        print(f"  ✅ Retourné {len(quality)} topics")
        for q in quality:
            print(f"     Topic {q['topic']}: {len(q.get('metrics', []))} fenêtres")
    except Exception as e:
        print(f"  ❌ Erreur: {e}")

    # Test 4: Word cloud (vérifie nlp_keywords présent)
    print("\nTest 4: get_nlp_keyword_frequency(top_k=5)")
    try:
        keywords = get_nlp_keyword_frequency(top_k=5)
        print(f"  ✅ Retourné {len(keywords)} mots-clés")
        for kw in keywords[:3]:
            print(f"     {kw['keyword']}: {kw['count']}x")
    except Exception as e:
        print(f"  ❌ Erreur: {e}")

    print(f"\n{'='*60}\n")