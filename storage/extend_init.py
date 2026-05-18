"""
storage/extend_init.py
======================

Script idempotent pour étendre l'initialisation MongoDB avec les
collections 6-11 et leurs indexes/TTL, après que P2 ait commencé
à écrire les nouvelles collections.

Ce script est conçu pour être réexécuté sans effet de bord (no-op
si les collections/indexes existent déjà). Il complète le script
init-mongo.js qui tourne au premier démarrage du conteneur.

MISE À JOUR 2026-05-17: Aligné sur les documents ACTUELLEMENT produits
par P2 (Spark), pas sur le README théorique. Les différences:
- incidents_enriched: P2 écrit "ingested_at" (pas "computed_at"),
  n'écrit PAS correlated_anomalies/correlated_voltage_drops/nlp_sentiment.
- data_quality_metrics: P2 n'écrit PAS zone_balance_ratio.

Références:
- P3 README §1: "You will extend this with extra collections"
- P3 README §4: Collection design table (collections 6-11)
- P3 README §6: Index strategy
- P3 README §7 Task 3: Idempotent script
"""

import sys
from datetime import datetime
from pathlib import Path

from storage.mongo_client import get_db, get_client

# ============================================================================
# STEP 1: Définition des collections à créer avec leurs configurations
# ============================================================================
# Chaque entrée définit: nom, TTL field, TTL seconds, indexes, validator optionnel.
# Conforme au tableau de la Section 4 du README P3.
#
# ⚠️  IMPORTANT: Les champs "required" des validateurs doivent matcher EXACTEMENT
#    ce que P2 (Spark) écrit. Vérifier avec:
#    db.collection.findOne() après que P2 ait produit des données.

COLLECTION_CONFIGS = {
    # Collection 6: incidents_enriched — Spark NLP output
    # ACTUELLEMENT écrit par P2 (streams_incidents.py):
    #   schema_version, incident_id, timestamp, zone, severity, type,
    #   description, affected_meters, estimated_duration_min, resolved,
    #   ingested_at, tokens, nlp_keywords
    # NOTE: P2 n'écrit PAS computed_at, correlated_anomalies, 
    #       correlated_voltage_drops, nlp_sentiment (README §5.3 est théorique)
    "incidents_enriched": {
        "ttl_field": "ingested_at",           # ← P2 écrit ingested_at, pas computed_at
        "ttl_seconds": 730 * 24 * 3600,       # 730 jours = 2 ans
        "indexes": [
            # Index composite pour les requêtes dashboard par zone+temps
            [("zone", 1), ("timestamp", -1)],
            # Index pour les recherches par mots-clés NLP (word-cloud)
            [("nlp_keywords", 1)],
            # Index TTL (doit être single-field, date type)
            [("ingested_at", 1)],              # ← Aligné sur P2
        ],
        "validator": {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["incident_id", "timestamp", "zone", "severity", "type"],
                "properties": {
                    "schema_version": {"bsonType": "string"},
                    "incident_id": {
                        "bsonType": "string",
                        "pattern": "^INC-[0-9]{8}-[0-9]{3}$"
                    },
                    "timestamp": {"bsonType": "date"},
                    "zone": {"enum": ["A", "B", "C", "D"]},
                    "severity": {"enum": ["LOW", "MEDIUM", "HIGH", "CRITICAL"]},
                    "type": {"bsonType": "string"},
                    "description": {"bsonType": "string"},
                    "affected_meters": {
                        "bsonType": "array",
                        "items": {"bsonType": "string", "pattern": "^SM-[0-9]{3}$"}
                    },
                    "estimated_duration_min": {
                        "bsonType": "int",
                        "minimum": 1,
                        "maximum": 480
                    },
                    "resolved": {"bsonType": "bool"},
                    "ingested_at": {"bsonType": "date"},
                    "tokens": {"bsonType": "array", "items": {"bsonType": "string"}},
                    "nlp_keywords": {"bsonType": "array", "items": {"bsonType": "string"}}
                    # NOTE: correlated_anomalies, correlated_voltage_drops,
                    # nlp_sentiment, computed_at sont ABSENTS du output P2 actuel.
                }
            }
        },
    },

    # Collection 7: feedback_nlp — Sentiment analysis
    # ACTUELLEMENT écrit par P2 (streams_feedback.py):
    #   schema_version, feedback_id, timestamp, user_id, zone, channel,
    #   sentiment_input, category, text, resolved, computed_at,
    #   sentiment_predicted, tokens
    "feedback_nlp": {
        "ttl_field": "computed_at",           # P2 écrit bien computed_at ici
        "ttl_seconds": 365 * 24 * 3600,       # 365 jours = 1 an
        "indexes": [
            [("zone", 1), ("timestamp", -1)],
            [("sentiment_predicted", 1), ("timestamp", -1)],
            [("computed_at", 1)],              # TTL index base
        ],
        "validator": {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["feedback_id", "timestamp", "zone", "sentiment_input"],
                "properties": {
                    "schema_version": {"bsonType": "string"},
                    "feedback_id": {"bsonType": "string"},
                    "timestamp": {"bsonType": "date"},
                    "user_id": {"bsonType": "string"},
                    "zone": {"enum": ["A", "B", "C", "D"]},
                    "channel": {"enum": ["WEB", "MOBILE", "CALL_CENTER", "EMAIL"]},
                    "sentiment_input": {"enum": ["POSITIVE", "NEGATIVE", "NEUTRAL"]},
                    "category": {"enum": ["OUTAGE", "BILLING", "QUALITY", "GENERAL"]},
                    "text": {"bsonType": "string"},
                    "resolved": {"bsonType": "bool"},
                    "computed_at": {"bsonType": "date"},
                    "sentiment_predicted": {"enum": ["POSITIVE", "NEGATIVE", "NEUTRAL"]},
                    "tokens": {"bsonType": "array", "items": {"bsonType": "string"}}
                }
            }
        },
    },

    # Collection 8: rss_feeds — News passthrough
    # ACTUELLEMENT écrit par P2 (streams_external.py):
    #   schema_version, feed_id, timestamp, source, category, title,
    #   summary, impact_score, keywords, ingested_at
    "rss_feeds": {
        "ttl_field": "ingested_at",
        "ttl_seconds": 365 * 24 * 3600,
        "indexes": [
            [("timestamp", -1)],
            [("category", 1), ("timestamp", -1)],
            [("ingested_at", 1)],
        ],
        "validator": {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["feed_id", "timestamp", "source", "category"],
                "properties": {
                    "schema_version": {"bsonType": "string"},
                    "feed_id": {"bsonType": "string"},
                    "timestamp": {"bsonType": "date"},
                    "source": {"bsonType": "string"},
                    "category": {"enum": ["RENEWABLE", "INDUSTRIAL_NEWS", "POLICY", "MARKET", "TECHNOLOGY"]},
                    "title": {"bsonType": "string"},
                    "summary": {"bsonType": "string"},
                    "impact_score": {"bsonType": "double", "minimum": 0, "maximum": 1},
                    "keywords": {"bsonType": "array", "items": {"bsonType": "string"}},
                    "ingested_at": {"bsonType": "date"}
                }
            }
        },
    },

    # Collection 9: market_prices — Price features
    # ACTUELLEMENT écrit par P2 (streams_external.py):
    #   schema_version, timestamp, market, price_mad_mwh, price_eur_mwh,
    #   demand_forecast_mw, renewable_share_pct, trend, ingested_at
    "market_prices": {
        "ttl_field": "ingested_at",
        "ttl_seconds": 365 * 24 * 3600,
        "indexes": [
            [("timestamp", -1)],
            [("market", 1), ("timestamp", -1)],
            [("ingested_at", 1)],
        ],
        "validator": {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["timestamp", "market", "price_mad_mwh"],
                "properties": {
                    "schema_version": {"bsonType": "string"},
                    "timestamp": {"bsonType": "date"},
                    "market": {"enum": ["MASEN", "EU_SPOT", "DAY_AHEAD"]},
                    "price_mad_mwh": {"bsonType": "double"},
                    "price_eur_mwh": {"bsonType": "double"},
                    "demand_forecast_mw": {"bsonType": "double"},
                    "renewable_share_pct": {"bsonType": "double"},
                    "trend": {"enum": ["RISING", "FALLING", "STABLE"]},
                    "ingested_at": {"bsonType": "date"}
                }
            }
        },
    },

    # Collection 10: data_quality_metrics — Section E reporting
    # ACTUELLEMENT écrit par P2 (data_quality.py):
    #   temporal_coverage_pct, completeness_pct, noise_rate_pct,
    #   anomaly_rate_pct, topic, window_start, alert, computed_at
    # NOTE: P2 n'écrit PAS zone_balance_ratio (README §2.7 est théorique)
    "data_quality_metrics": {
        "ttl_field": "window_start",
        "ttl_seconds": 365 * 24 * 3600,
        "indexes": [
            [("topic", 1), ("window_start", -1)],
            [("window_start", 1)],
        ],
        "validator": {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["topic", "window_start", "temporal_coverage_pct"],
                "properties": {
                    "topic": {"bsonType": "string"},
                    "window_start": {"bsonType": "date"},
                    "temporal_coverage_pct": {"bsonType": "double"},
                    "completeness_pct": {"bsonType": "double"},
                    "noise_rate_pct": {"bsonType": "double"},
                    "anomaly_rate_pct": {"bsonType": "double"},
                    # "zone_balance_ratio": {"bsonType": "double"},  # ← ABSENT de P2
                    "alert": {"bsonType": ["string", "null"]},
                    "computed_at": {"bsonType": "date"}
                }
            }
        },
    },

    # Collection 11: dashboard_alerts — P4 alert log (éthique/audit)
    # Écrit par P4 (Dashboard) — pas encore de données dans votre output
    "dashboard_alerts": {
        "ttl_field": "triggered_at",
        "ttl_seconds": 365 * 24 * 3600,
        "indexes": [
            [("alert_level", 1), ("triggered_at", -1)],
            [("zone", 1), ("triggered_at", -1)],
            [("triggered_at", 1)],
        ],
        "validator": {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["alert_level", "triggered_at", "message"],
                "properties": {
                    "alert_level": {"enum": ["INFO", "WARNING", "CRITICAL"]},
                    "triggered_at": {"bsonType": "date"},
                    "zone": {"enum": ["A", "B", "C", "D"]},
                    "message": {"bsonType": "string"},
                    "source_metric": {"bsonType": "string"},
                    "threshold_value": {"bsonType": "double"},
                    "actual_value": {"bsonType": "double"}
                }
            }
        },
    },
}


# ============================================================================
# STEP 2: Fonctions d'initialisation idempotentes
# ============================================================================

def ensure_collection_exists(db, coll_name: str, config: dict) -> bool:
    """
    Crée une collection si elle n'existe pas, avec validation optionnelle.

    Idempotent: ne fait rien si la collection existe déjà.

    Args:
        db: Database pymongo.
        coll_name: Nom de la collection.
        config: Dict avec ttl_field, ttl_seconds, indexes, validator.

    Returns:
        True si créée ou déjà existante, False en erreur.
    """
    existing = db.list_collection_names()

    if coll_name in existing:
        print(f"  [SKIP] Collection '{coll_name}' existe déjà.")
        return True

    try:
        # Création avec validateur si fourni
        if config.get("validator"):
            db.create_collection(
                coll_name,
                validator=config["validator"],
                validationLevel="moderate",   # ← moderate (pas strict) pour tolérer évolutions P2
                validationAction="warn"       # ← warn (pas error) pour ne pas bloquer Spark
            )
            print(f"  [CREATE] Collection '{coll_name}' créée avec validator.")
        else:
            db.create_collection(coll_name)
            print(f"  [CREATE] Collection '{coll_name}' créée.")
        return True
    except Exception as e:
        print(f"  [ERROR] Échec création '{coll_name}': {e}")
        return False


def ensure_index_exists(db, coll_name: str, index_keys: list, ttl_config: dict = None) -> bool:
    """
    Crée un index de manière idempotente.

    Gère les indexes TTL (single-field sur date) et les indexes composites.
    Si l'index existe déjà avec une spec différente, un warning est émis.

    Args:
        db: Database pymongo.
        coll_name: Nom de la collection.
        index_keys: Liste de tuples [(field, direction), ...].
        ttl_config: Dict avec 'field' et 'seconds' pour TTL, ou None.

    Returns:
        True si créé ou déjà existant, False en erreur.
    """
    coll = db[coll_name]

    # Construction du nom d'index pour vérification d'existence
    index_name = "_".join([f"{k}_{v}" for k, v in index_keys])

    # Vérification si l'index existe déjà
    existing_indexes = coll.list_indexes()
    existing_names = [idx["name"] for idx in existing_indexes]

    # Si c'est un index TTL, on vérifie spécifiquement la clé et l'option expireAfterSeconds
    if ttl_config:
        ttl_field = ttl_config["field"]
        ttl_seconds = ttl_config["seconds"]

        # Un TTL index est un single-field index avec expireAfterSeconds
        for idx in existing_indexes:
            if idx.get("key") == {ttl_field: 1} and idx.get("expireAfterSeconds") == ttl_seconds:
                print(f"  [SKIP] TTL index sur '{ttl_field}' ({ttl_seconds}s) existe déjà.")
                return True
            elif idx.get("key") == {ttl_field: 1} and "expireAfterSeconds" in idx:
                print(f"  [WARN] TTL index sur '{ttl_field}' existe mais avec "
                      f"expireAfterSeconds={idx.get('expireAfterSeconds')} "
                      f"(attendu: {ttl_seconds}). Utiliser collMod pour modifier.")
                return True

        # Création du TTL index
        try:
            coll.create_index(
                [(ttl_field, 1)],
                name=f"{ttl_field}_ttl",
                expireAfterSeconds=ttl_seconds,
                background=True
            )
            print(f"  [CREATE] TTL index '{ttl_field}_ttl' ({ttl_seconds}s) sur '{coll_name}'.")
            return True
        except Exception as e:
            print(f"  [ERROR] Échec TTL index sur '{coll_name}': {e}")
            return False

    else:
        # Index non-TTL (composite ou simple)
        for idx in existing_indexes:
            if idx.get("name") == index_name or idx.get("key") == dict(index_keys):
                print(f"  [SKIP] Index '{index_name}' existe déjà sur '{coll_name}'.")
                return True

        try:
            coll.create_index(
                index_keys,
                name=index_name,
                background=True
            )
            print(f"  [CREATE] Index '{index_name}' sur '{coll_name}'.")
            return True
        except Exception as e:
            print(f"  [ERROR] Échec index '{index_name}' sur '{coll_name}': {e}")
            return False


def extend_database_init():
    """
    Point d'entrée principal: étend l'init MongoDB avec les collections 6-11.

    Idempotent: peut être réexécuté après chaque déploiement P2 sans risque.
    Référence: P3 README §10: "After editing init script, down -v && up -d".
    Alternative: exécuter ce script Python sans recréer le volume.
    """
    print(f"\n{'='*70}")
    print("STORAGE EXTEND INIT — MongoDB Collections 6-11 + Indexes + TTL")
    print(f"{'='*70}")
    print(f"Timestamp: {datetime.utcnow().isoformat()}Z")
    print(f"Aligné sur le output ACTUEL de P2 (Spark)")
    print(f"{'='*70}\n")

    db = get_db(role="admin")

    success_count = 0
    total_ops = 0

    for coll_name, config in COLLECTION_CONFIGS.items():
        print(f"\n--- Collection: {coll_name} ---")

        # STEP 2a: Création de la collection
        total_ops += 1
        if ensure_collection_exists(db, coll_name, config):
            success_count += 1

        # STEP 2b: Création des indexes (y compris TTL)
        for idx_keys in config["indexes"]:
            total_ops += 1

            # Détection si c'est l'index TTL (single-field sur ttl_field)
            is_ttl = len(idx_keys) == 1 and idx_keys[0][0] == config["ttl_field"]

            if is_ttl:
                ttl_cfg = {"field": config["ttl_field"], "seconds": config["ttl_seconds"]}
                if ensure_index_exists(db, coll_name, idx_keys, ttl_config=ttl_cfg):
                    success_count += 1
            else:
                if ensure_index_exists(db, coll_name, idx_keys):
                    success_count += 1

    # ============================================================================
    # STEP 3: Vérification finale et rapport
    # ============================================================================
    print(f"\n{'='*70}")
    print("RAPPORT D'INITIALISATION")
    print(f"{'='*70}")

    all_collections = db.list_collection_names()
    print(f"Collections présentes: {sorted(all_collections)}")

    # Vérification que tous les indexes TTL sont actifs
    print("\nVérification des indexes TTL:")
    for coll_name, config in COLLECTION_CONFIGS.items():
        if coll_name in all_collections:
            coll = db[coll_name]
            indexes = list(coll.list_indexes())
            ttl_found = any(
                idx.get("expireAfterSeconds") is not None 
                for idx in indexes
            )
            status = "✅ OK" if ttl_found else "❌ MANQUANT"
            print(f"  {coll_name}: TTL {status}")
        else:
            print(f"  {coll_name}: ❌ Collection absente")

    print(f"\nOpérations réussies: {success_count}/{total_ops}")
    print(f"{'='*70}\n")

    if success_count < total_ops:
        print("WARNING: Certaines opérations ont échoué. Vérifier les logs ci-dessus.")
        return 1
    return 0


# ============================================================================
# STEP 4: Point d'entrée CLI
# ============================================================================

if __name__ == "__main__":
    sys.exit(extend_database_init())