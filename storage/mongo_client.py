"""
storage/mongo_client.py
=======================

Module client MongoDB partagé utilisé par P4 (Dashboard) et P5 (ML).
Implémente le pattern Singleton avec gestion de pool de connexions
pour éviter l'épuisement des ressources réseau.

Conforme aux exigences de la Section 4 du README P3 (connection details)
et aux best practices PyMongo 4.7+ pour le pooling.

Références:
- P3 README §8: Starter snippet
- P3 README §7 Task 2: Shared get_client() / get_db()
- Pymongo docs: Connection pool tuning (maxPoolSize, minPoolSize, retryWrites)
"""

import os
import atexit
from pathlib import Path
from typing import Optional

from pymongo import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv

# ============================================================================
# STEP 1: Chargement des variables d'environnement depuis le .env du projet
# ============================================================================
# On remonte deux niveaux depuis storage/mongo_client.py pour atteindre
# le .env à la racine du projet (structure: projet/storage/mongo_client.py)
_ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(_ENV_PATH)


# ============================================================================
# STEP 2: Singleton client avec gestion de cycle de vie
# ============================================================================
# Le client MongoDB est thread-safe et doit être réutilisé globalement.
# Créer un nouveau MongoClient par requête épuise le pool et cause
# des latences sévères (voir P3 README §11 pitfall #1).
_client_instance: Optional[MongoClient] = None
_client_role: Optional[str] = None


def _build_uri(role: str = "reader") -> str:
    """
    Construit l'URI de connexion MongoDB avec les credentials appropriés.

    Args:
        role: "reader"  -> compte admin/lecture (P4/P5 local dev)
              "spark_writer" -> compte service limité (Spark uniquement)
              "admin"   -> root pour opérations DBA (réplica, init)

    Returns:
        URI MongoDB complète avec authSource et options de connexion.

    Référence: P3 README §4 (connection details) et §8 (starter snippet).
    """
    if role == "spark_writer":
        user = os.getenv("MONGO_SPARK_USER", "spark_writer")
        pwd = os.getenv("MONGO_SPARK_PASS", "change-me-before-deploy")
    elif role == "admin":
        user = os.getenv("MONGO_USERNAME", "energy_admin")
        pwd = os.getenv("MONGO_PASSWORD", "change-me-before-deploy")
    else:  # role == "reader" ou défaut
        user = os.getenv("MONGO_USERNAME", "energy_admin")
        pwd = os.getenv("MONGO_PASSWORD", "change-me-before-deploy")

    host = os.getenv("MONGO_HOST", "localhost")
    port = int(os.getenv("MONGO_PORT", 27017))
    db_name = os.getenv("MONGO_DB_NAME", "energy_db")

    # Construction URI avec authSource obligatoire pour l'authentification SCRAM
    # retryWrites=True pour la durabilité, w=majority pour le quorum en réplica
    uri = (
        f"mongodb://{user}:{pwd}@{host}:{port}/{db_name}"
        f"?authSource=admin"
        f"&retryWrites=true"
        f"&w=majority"
    )
    return uri


def get_client(role: str = "reader") -> MongoClient:
    """
    Retourne l'instance singleton MongoClient avec pool configuré.

    Le pool est dimensionné pour supporter la concurrence P4 (dashboard
    multi-thread) + P5 (ML training batch) sans épuisement.

    Configuration pool (Pymongo 4.7 best practices):
    - maxPoolSize=50:  pic de concurrence estimé * 1.5
    - minPoolSize=5:   connexions chaudes persistantes
    - maxIdleTimeMS=60000: évite les déconnexions firewall/proxy
    - waitQueueTimeoutMS=10000: surface rapidement l'épuisement

    Args:
        role: Rôle déterminant les credentials (reader | spark_writer | admin)

    Returns:
        MongoClient: instance thread-safe partagée.

    Raises:
        ConnectionFailure: si MongoDB est inaccessible après timeout.

    Exemple:
        >>> from storage.mongo_client import get_client
        >>> client = get_client("reader")
        >>> db = client["energy_db"]
    """
    global _client_instance, _client_role

    # Si le rôle change, on doit reconstruire le client (credentials différents)
    if _client_instance is not None and _client_role != role:
        _client_instance.close()
        _client_instance = None

    if _client_instance is None:
        uri = _build_uri(role)

        # STEP 2a: Configuration du pool de connexions
        # Voir P3 README §11 pitfall #1: "Never create a new client per request"
        _client_instance = MongoClient(
            uri,
            maxPoolSize=50,          # Capacité: ~30 req concurrentes dashboard + ML
            minPoolSize=5,           # Warm pool pour latence initiale faible
            maxIdleTimeMS=60000,     # Fermeture idle après 60s (firewall-friendly)
            waitQueueTimeoutMS=10000, # Fail fast si pool épuisé
            connectTimeoutMS=10000,  # Évite d'attendre indéfiniment au boot
            socketTimeoutMS=45000,   # Doit dépasser la requête la plus lente
            serverSelectionTimeoutMS=5000,
            # readPreference="secondaryPreferred",  # Décommenter en réplica set
        )
        _client_role = role

        # STEP 2b: Enregistrement du cleanup au shutdown Python
        # Garantit la libération propre des sockets TCP.
        atexit.register(_cleanup_client)

        # STEP 2c: Vérification de connexion immédiate (fail-fast)
        # Ping le serveur pour valider credentials et réseau dès l'import.
        _client_instance.admin.command("ping")
        _password_for_mask = os.getenv("MONGO_PASSWORD", "***")
        _safe_uri = uri.replace(f":{_password_for_mask}@", ":***@")
        print(f"[mongo_client] Pool connecté à {_safe_uri} (role={role})")

    return _client_instance


def get_db(role: str = "reader"):
    """
    Retourne l'objet Database (energy_db) prêt à l'emploi.

    C'est l'interface principale pour P4 et P5. Toute opération de
    lecture/écriture passe par cette fonction.

    Args:
        role: Rôle pour la connexion sous-jacente.

    Returns:
        pymongo.database.Database: base energy_db avec indexes TTL déjà créés.
    """
    client = get_client(role)
    db_name = os.getenv("MONGO_DB_NAME", "energy_db")
    return client[db_name]


def _cleanup_client():
    """Handler atexit: ferme proprement le pool de connexions."""
    global _client_instance
    if _client_instance is not None:
        _client_instance.close()
        _client_instance = None
        print("[mongo_client] Pool fermé proprement.")


# ============================================================================
# STEP 3: Helpers spécifiques pour les collections fréquemment accédées
# ============================================================================
# Ces helpers encapsulent les noms de collections pour éviter les typos
# et centraliser les changements de schéma.

def get_meters_raw_collection(role: str = "reader"):
    """Collection des lectures brutes des compteurs (TTL 90j)."""
    return get_db(role)["meters_raw"]


def get_meters_aggregated_collection(role: str = "reader"):
    """Collection des agrégats 15min par zone (TTL 1an). Clé composite: (zone, window_start)."""
    return get_db(role)["meters_aggregated_15min"]


def get_weather_collection(role: str = "reader"):
    """Collection météo passthrough (TTL 1an)."""
    return get_db(role)["weather"]


def get_incidents_collection(role: str = "reader"):
    """Collection incidents bruts (TTL 2ans)."""
    return get_db(role)["incidents"]


def get_incidents_enriched_collection(role: str = "reader"):
    """Collection incidents enrichis NLP (TTL 2ans)."""
    return get_db(role)["incidents_enriched"]


def get_feedback_nlp_collection(role: str = "reader"):
    """Collection feedback avec sentiment NLP (TTL 1an)."""
    return get_db(role)["feedback_nlp"]


def get_rss_feeds_collection(role: str = "reader"):
    """Collection flux RSS (TTL 1an)."""
    return get_db(role)["rss_feeds"]


def get_market_prices_collection(role: str = "reader"):
    """Collection prix du marché (TTL 1an)."""
    return get_db(role)["market_prices"]


def get_data_quality_collection(role: str = "reader"):
    """Collection métriques de qualité par topic/fenêtre (TTL 1an)."""
    return get_db(role)["data_quality_metrics"]


def get_ml_predictions_collection(role: str = "reader"):
    """Collection prédictions P5 ML (TTL 6mois)."""
    return get_db(role)["ml_predictions"]


def get_dashboard_alerts_collection(role: str = "reader"):
    """Collection log des alertes déclenchées par P4 (TTL 1an)."""
    return get_db(role)["dashboard_alerts"]


# ============================================================================
# STEP 4: Diagnostic et monitoring du pool (pour healthcheck.py)
# ============================================================================

def get_pool_status() -> dict:
    """
    Retourne les métriques du pool de connexions.

    Utile pour le healthcheck et le monitoring ops.
    Voir P3 README §11 pitfall #4: vérifier que l'index est utilisé.

    Returns:
        dict avec current, available, totalCreated connections.
    """
    client = get_client()
    status = client.admin.command("serverStatus")
    conn = status.get("connections", {})
    return {
        "current": conn.get("current", -1),
        "available": conn.get("available", -1),
        "total_created": conn.get("totalCreated", -1),
        "pool_address": str(client.address),
    }


def check_index_usage(collection_name: str, query_filter: dict) -> dict:
    """
    Vérifie qu'une requête utilise bien l'index attendu (évite COLLSCAN).

    Référence: P3 README §11 pitfall #4: "Index not used by query".

    Args:
        collection_name: Nom de la collection à tester.
        query_filter: Filtre de la requête (ex: {"zone": "B", "timestamp": {"$gte": ...}}).

    Returns:
        dict avec winningPlan.stage (IXSCAN vs COLLSCAN) et index utilisé.
    """
    db = get_db()
    coll = db[collection_name]
    explain = coll.find(query_filter).explain()
    winning_plan = explain["queryPlanner"]["winningPlan"]

    return {
        "collection": collection_name,
        "stage": winning_plan["stage"],
        "index_name": winning_plan.get("inputStage", {}).get("indexName", "NONE"),
        "is_index_scan": winning_plan["stage"] == "IXSCAN" or 
                        winning_plan.get("inputStage", {}).get("stage") == "IXSCAN",
    }


# ============================================================================
# STEP 5: Point d'entrée CLI pour test rapide
# ============================================================================

if __name__ == "__main__":
    # Test de connexion et affichage des collections disponibles
    db = get_db()
    collections = db.list_collection_names()
    print(f"\n{'='*60}")
    print("STORAGE MODULE — MongoDB Connection Test")
    print(f"{'='*60}")
    print(f"Database: {db.name}")
    print(f"Collections: {collections}")

    # Comptages rapides pour validation
    for coll_name in ["meters_raw", "meters_aggregated_15min", "weather", 
                      "incidents", "data_quality_metrics"]:
        if coll_name in collections:
            count = db[coll_name].estimated_document_count()
            print(f"  • {coll_name}: ~{count} documents")

    # État du pool
    pool = get_pool_status()
    print(f"\nPool status: {pool}")
    print(f"{'='*60}\n")