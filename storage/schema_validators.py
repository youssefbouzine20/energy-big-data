"""
storage/schema_validators.py
============================

Module Python pour la gestion programmatique des validateurs de schéma
MongoDB. Complète le script JS init-mongo.js en permettant aux
administrateurs (P3) de modifier les règles de validation sans recréer
le conteneur.

MISE À JOUR 2026-05-17: Les validateurs pour les collections 6-11
(incidents_enriched, feedback_nlp, etc.) sont alignés sur le output
ACTUEL de P2 (Spark), pas sur le README théorique.

Références:
- P3 README §12: Data quality contribution (Section E)
- P3 README §11 pitfall: Schema validation catches impossible data
- MongoDB docs: collMod command pour modifier validators à chaud
"""

import json
from datetime import datetime
from typing import Dict, Any, Optional

from storage.mongo_client import get_db


# ============================================================================
# STEP 1: Définition des validateurs JSON Schema (mirror de init-mongo.js)
# ============================================================================
# Ces validateurs sont dupliqués ici en Python pour maintenance centralisée.
# Toute modification doit être reflétée dans init-mongo.js.
#
# ⚠️  IMPORTANT: Les champs "required" doivent matcher EXACTEMENT ce que
#    P2 (Spark) écrit. Vérifier avec db.collection.findOne() après ingestion.

VALIDATORS: Dict[str, Dict[str, Any]] = {
    # Collections 1-5: Définies dans init-mongo.js (meters_raw, meters_aggregated_15min,
    # weather, incidents, ml_predictions). On les duplique ici pour maintenance
    # centralisée et application programmatique.

    "meters_raw": {
        "bsonType": "object",
        "required": ["meter_id", "timestamp", "zone", "consumption_kwh", "voltage_v"],
        "properties": {
            "meter_id": {
                "bsonType": "string",
                "pattern": "^SM-[0-9]{3}$",
                "description": "Format SM-NNN (ex: SM-007)"
            },
            "timestamp": {"bsonType": "date"},
            "zone": {"enum": ["A", "B", "C", "D"]},
            "consumption_kwh": {
                "bsonType": "double",
                "minimum": 0.001,
                "maximum": 0.12
            },
            "voltage_v": {
                "bsonType": "double",
                "minimum": 200,
                "maximum": 260
            },
            "frequency_hz": {"bsonType": "double", "minimum": 48, "maximum": 52},
            "power_factor": {"bsonType": "double", "minimum": 0, "maximum": 1},
            "is_anomaly": {"bsonType": "bool"},
            "anomaly_reason": {
                "enum": [None, "VOLTAGE_LOW", "VOLTAGE_HIGH", "OVERCONSUMPTION", "FREQUENCY_DEVIATION"]
            },
            "hour_of_day": {"bsonType": "int", "minimum": 0, "maximum": 23},
            "day_of_week": {"bsonType": "int", "minimum": 0, "maximum": 6},
            "is_peak_hour": {"bsonType": "bool"},
            "zone_lat": {"bsonType": "double"},
            "zone_lon": {"bsonType": "double"},
            "ingested_at": {"bsonType": "date"}
        }
    },

    "meters_aggregated_15min": {
        "bsonType": "object",
        "required": ["zone", "window_start", "window_end", "avg_consumption"],
        "properties": {
            "zone": {"enum": ["A", "B", "C", "D"]},
            "window_start": {"bsonType": "date"},
            "window_end": {"bsonType": "date"},
            "avg_consumption": {"bsonType": "double"},
            "max_consumption": {"bsonType": "double"},
            "min_consumption": {"bsonType": "double"},
            "total_consumption_kwh": {"bsonType": "double"},
            "anomaly_count": {"bsonType": "int"},
            "anomaly_rate_pct": {"bsonType": "double"},
            "voltage_min": {"bsonType": "double"},
            "voltage_max": {"bsonType": "double"},
            "voltage_avg": {"bsonType": "double"},
            "frequency_stddev": {"bsonType": "double"},
            "meter_count": {"bsonType": "int"},
            "weather_temperature_c": {"bsonType": "double"},
            "weather_severity": {"bsonType": "string"},
            "active_incidents": {"bsonType": "int"},
            "zone_lat": {"bsonType": "double"},
            "zone_lon": {"bsonType": "double"},
            "computed_at": {"bsonType": "date"}
        }
    },

    "weather": {
        "bsonType": "object",
        "required": ["station_id", "timestamp", "temperature_c"],
        "properties": {
            "station_id": {
                "bsonType": "string",
                "pattern": "^WS-[A-Z]+$"
            },
            "timestamp": {"bsonType": "date"},
            "temperature_c": {"bsonType": "double"},
            "feels_like_c": {"bsonType": "double"},
            "humidity_pct": {"bsonType": "double"},
            "solar_irradiance_wm2": {"bsonType": "double"},
            "wind_speed_ms": {"bsonType": "double"},
            "weather_severity": {
                "enum": ["NORMAL", "HOT", "COLD", "STORM", "EXTREME"]
            },
            "ingested_at": {"bsonType": "date"}
        }
    },

    "incidents": {
        "bsonType": "object",
        "required": ["incident_id", "timestamp", "zone", "severity", "type"],
        "properties": {
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
                "items": {
                    "bsonType": "string",
                    "pattern": "^SM-[0-9]{3}$"
                }
            },
            "estimated_duration_min": {
                "bsonType": "int",
                "minimum": 1,
                "maximum": 480
            },
            "resolved": {"bsonType": "bool"},
            "ingested_at": {"bsonType": "date"}
        }
    },

    "ml_predictions": {
        "bsonType": "object",
        "required": ["zone", "model_name", "prediction_time", "forecast_for"],
        "properties": {
            "zone": {"enum": ["A", "B", "C", "D"]},
            "model_name": {"bsonType": "string"},
            "prediction_time": {"bsonType": "date"},
            "forecast_for": {"bsonType": "date"},
            "consumption_forecast": {"bsonType": "double"},
            "anomaly_proba": {"bsonType": "double"},
            "alert_level": {
                "enum": ["INFO", "WARNING", "CRITICAL", "NORMAL"]
            },
            "ratio_to_peak": {"bsonType": "double"}
        }
    },

    # ============================================================================
    # Collections 6-11: Validateurs alignés sur le output ACTUEL de P2
    # ============================================================================
    # NOTE: Les champs théoriques du README (correlated_anomalies, 
    # correlated_voltage_drops, nlp_sentiment, zone_balance_ratio) sont ABSENTS
    # du output P2 actuel. Les validateurs ci-dessous ne les requirent PAS.

    "incidents_enriched": {
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
            # NOTE: correlated_anomalies, correlated_voltage_drops, nlp_sentiment,
            # computed_at sont ABSENTS du output P2 actuel (streams_incidents.py)
        }
    },

    "feedback_nlp": {
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
    },

    "rss_feeds": {
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
    },

    "market_prices": {
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
    },

    "data_quality_metrics": {
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
    },

    "dashboard_alerts": {
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
}


# ============================================================================
# STEP 2: Fonctions de gestion des validateurs
# ============================================================================

def apply_validator(collection_name: str, 
                    validator: Optional[Dict[str, Any]] = None,
                    validation_level: str = "moderate",
                    validation_action: str = "warn") -> bool:
    """
    Applique ou met à jour un validateur JSON Schema sur une collection.

    Idempotent: si le validateur existe déjà avec la même spec, c'est un no-op.
    Utilise la commande collMod pour modifier à chaud sans downtime.

    Référence: P3 README §12 — validationAction: "warn" ne bloque pas Spark.

    Args:
        collection_name: Nom de la collection cible.
        validator: Dict JSON Schema (utilise VALIDATORS par défaut).
        validation_level: "off" | "strict" | "moderate".
        validation_action: "error" (rejette) | "warn" (log uniquement).

    Returns:
        True si appliqué avec succès, False sinon.
    """
    db = get_db(role="admin")  # Nécessite dbAdmin privileges

    if validator is None:
        if collection_name not in VALIDATORS:
            print(f"[ERROR] Aucun validateur défini pour '{collection_name}'")
            return False
        validator = VALIDATORS[collection_name]

    try:
        # Commande collMod: modification à chaud du validateur
        # Voir MongoDB docs: db.runCommand({ collMod: ..., validator: ... })
        result = db.command({
            "collMod": collection_name,
            "validator": {"$jsonSchema": validator},
            "validationLevel": validation_level,
            "validationAction": validation_action
        })

        print(f"[OK] Validateur appliqué sur '{collection_name}' "
              f"(level={validation_level}, action={validation_action})")
        return True

    except Exception as e:
        print(f"[ERROR] Échec application validateur sur '{collection_name}': {e}")
        return False


def remove_validator(collection_name: str) -> bool:
    """
    Supprime le validateur d'une collection (mode validationLevel="off").

    Utile pour les migrations de schéma ou le débogage P1/P2.

    Args:
        collection_name: Nom de la collection.

    Returns:
        True si supprimé avec succès.
    """
    db = get_db(role="admin")

    try:
        db.command({
            "collMod": collection_name,
            "validator": {},
            "validationLevel": "off"
        })
        print(f"[OK] Validateur supprimé de '{collection_name}'")
        return True
    except Exception as e:
        print(f"[ERROR] Échec suppression validateur: {e}")
        return False


def get_current_validator(collection_name: str) -> Optional[Dict[str, Any]]:
    """
    Récupère le validateur actuellement appliqué sur une collection.

    Args:
        collection_name: Nom de la collection.

    Returns:
        Dict JSON Schema ou None si aucun validateur.
    """
    db = get_db(role="admin")

    try:
        # listCollections retourne les options incluant le validator
        cursor = db.list_collections(filter={"name": collection_name})
        info = next(cursor, None)

        if info and "options" in info and "validator" in info["options"]:
            return info["options"]["validator"]
        return None
    except Exception as e:
        print(f"[ERROR] Impossible de lire le validateur: {e}")
        return None


def validate_document(collection_name: str, document: Dict[str, Any]) -> Dict[str, Any]:
    """
    Valide un document Python contre le schéma défini sans l'insérer.

    Utilise la logique côté client (pas de commande MongoDB).
    Préférable pour les tests unitaires P3 avant déploiement.

    Args:
        collection_name: Nom de la collection cible.
        document: Document Python à valider.

    Returns:
        Dict avec valid (bool), errors (list), warnings (list).
    """
    validator = VALIDATORS.get(collection_name)
    if not validator:
        return {"valid": True, "errors": [], "warnings": ["Aucun validateur défini"]}

    errors = []
    warnings = []

    # Vérification des champs requis
    required = validator.get("required", [])
    for field in required:
        if field not in document:
            errors.append(f"Champ requis manquant: '{field}'")

    # Vérification des types et contraintes
    properties = validator.get("properties", {})
    for field, value in document.items():
        if field in properties:
            prop = properties[field]

            # Vérification enum
            if "enum" in prop and value not in prop["enum"]:
                errors.append(f"'{field}': valeur '{value}' hors enum {prop['enum']}")

            # Vérification pattern (regex)
            if "pattern" in prop and isinstance(value, str):
                import re
                if not re.match(prop["pattern"], value):
                    errors.append(f"'{field}': '{value}' ne match pas '{prop['pattern']}'")

            # Vérification min/max
            if "minimum" in prop and value < prop["minimum"]:
                errors.append(f"'{field}': {value} < minimum {prop['minimum']}")
            if "maximum" in prop and value > prop["maximum"]:
                errors.append(f"'{field}': {value} > maximum {prop['maximum']}")

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings
    }


# ============================================================================
# STEP 3: Application en masse (pour les déploiements initiaux)
# ============================================================================

def apply_all_validators(validation_level: str = "moderate", 
                         validation_action: str = "warn") -> Dict[str, bool]:
    """
    Applique les validateurs définis sur TOUTES les collections connues.

    À exécuter après extend_init.py pour activer la validation de qualité
    sur les collections 1-5 (init-mongo.js) et 6-11 (extend_init.py).

    Référence: P3 README §12 — "catches impossible data that slips through".

    Args:
        validation_level: Niveau de validation (moderate par défaut).
        validation_action: Action en cas d'échec (warn par défaut).

    Returns:
        Dict {collection_name: success_bool} pour rapport.
    """
    results = {}

    print(f"\n{'='*60}")
    print("SCHEMA VALIDATORS — Application en masse")
    print(f"{'='*60}")
    print(f"Timestamp: {datetime.utcnow().isoformat()}Z")
    print(f"Level: {validation_level}, Action: {validation_action}")
    print(f"Aligné sur le output ACTUEL de P2 (Spark)")
    print(f"{'='*60}\n")

    for coll_name in VALIDATORS:
        success = apply_validator(coll_name, validation_level=validation_level, 
                                  validation_action=validation_action)
        results[coll_name] = success

    # Résumé
    success_count = sum(1 for v in results.values() if v)
    total = len(results)
    print(f"\nRésultat: {success_count}/{total} validateurs appliqués avec succès.")

    return results


# ============================================================================
# STEP 4: Point d'entrée CLI
# ============================================================================

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--apply-all":
        results = apply_all_validators()
        sys.exit(0 if all(results.values()) else 1)
    else:
        print("Usage: python -m storage.schema_validators --apply-all")
        print("Applique tous les validateurs JSON Schema définis sur les collections.")
        print("\nValidateurs disponibles:")
        for name in VALIDATORS:
            required = VALIDATORS[name].get("required", [])
            print(f"  - {name}: required={required}")