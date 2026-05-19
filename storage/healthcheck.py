"""
storage/healthcheck.py
======================

Module de diagnostic et de monitoring opérationnel pour l'infrastructure
MongoDB du projet Big Data. Fournit un tableau de bord textuel des
métriques critiques: tailles de collections, statistiques d'indexes,
compte à rebours TTL, et état du pool de connexions.

Références:
- P3 README §7 Task 5: Optional healthcheck script
- P3 README §11 pitfall #3: TTL not deleting documents
- P3 README §11 pitfall #4: Index not used by query
- Pymongo CMAP events pour monitoring du pool
"""

import sys
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple

from storage.mongo_client import get_db, get_client, get_pool_status, check_index_usage


# ============================================================================
# STEP 1: Configuration des collections à surveiller
# ============================================================================
# Ordre cohérent avec le tableau de la Section 4 du README P3.

MONITORED_COLLECTIONS = [
    ("meters_raw", "TTL 90j", "ingested_at"),
    ("meters_aggregated_15min", "TTL 1an", "computed_at"),
    ("weather", "TTL 1an", "ingested_at"),
    ("incidents", "TTL 2ans", "timestamp"),
    ("incidents_enriched", "TTL 2ans", "computed_at"),
    ("feedback_nlp", "TTL 1an", "computed_at"),
    ("rss_feeds", "TTL 1an", "ingested_at"),
    ("market_prices", "TTL 1an", "ingested_at"),
    ("data_quality_metrics", "TTL 1an", "window_start"),
    ("ml_predictions", "TTL 6mois", "prediction_time"),
    ("dashboard_alerts", "TTL 1an", "triggered_at"),
]


# ============================================================================
# STEP 2: Collecte des métriques par collection
# ============================================================================

def get_collection_stats(db, coll_name: str) -> Dict[str, Any]:
    """
    Récupère les statistiques opérationnelles d'une collection.

    Args:
        db: Database pymongo.
        coll_name: Nom de la collection.

    Returns:
        Dict avec count, size, indexes, ttl_status.
    """
    coll = db[coll_name]

    # Comptage estimé (rapide, basé sur les métadonnées)
    estimated_count = coll.estimated_document_count()

    # Taille physique (data + indexes)
    try:
        stats = db.command("collStats", coll_name)
        size_mb = round(stats.get("size", 0) / (1024 * 1024), 2)
        index_size_mb = round(stats.get("totalIndexSize", 0) / (1024 * 1024), 2)
    except Exception:
        size_mb = -1
        index_size_mb = -1

    # Liste des indexes
    indexes = list(coll.list_indexes())
    index_names = [idx["name"] for idx in indexes]

    # Détection TTL
    ttl_indexes = [
        idx for idx in indexes 
        if idx.get("expireAfterSeconds") is not None
    ]
    ttl_status = "✅ Actif" if ttl_indexes else "❌ Aucun"
    ttl_details = ""
    if ttl_indexes:
        ttl = ttl_indexes[0]
        days = round(ttl["expireAfterSeconds"] / 86400, 1)
        ttl_details = f"{ttl['key']} → {days}j"

    return {
        "collection": coll_name,
        "documents": estimated_count,
        "size_mb": size_mb,
        "index_size_mb": index_size_mb,
        "index_count": len(indexes),
        "index_names": ", ".join(index_names),
        "ttl_status": ttl_status,
        "ttl_details": ttl_details,
    }


def get_ttl_expiry_forecast(db, coll_name: str, ttl_field: str) -> Dict[str, Any]:
    """
    Estime quand les prochains documents expireront (TTL countdown).

    Référence: P3 README §11 pitfall #3 — TTL background thread runs every 60s.

    Args:
        db: Database pymongo.
        coll_name: Nom de la collection.
        ttl_field: Champ date utilisé par le TTL index.

    Returns:
        Dict avec oldest_doc, youngest_doc, next_expiry_estimate.
    """
    coll = db[coll_name]

    try:
        # Document le plus ancien (prochain à expirer)
        oldest = coll.find({}, {ttl_field: 1}).sort(ttl_field, 1).limit(1)
        oldest_list = list(oldest)

        # Document le plus récent (dernier à expirer dans la fenêtre TTL)
        youngest = coll.find({}, {ttl_field: 1}).sort(ttl_field, -1).limit(1)
        youngest_list = list(youngest)

        if not oldest_list:
            return {
                "collection": coll_name,
                "oldest_doc": "N/A",
                "youngest_doc": "N/A",
                "next_expiry": "Collection vide",
            }

        oldest_date = oldest_list[0].get(ttl_field)
        youngest_date = youngest_list[0].get(ttl_field)

        # Récupération de la durée TTL depuis l'index
        indexes = list(coll.list_indexes())
        ttl_idx = next(
            (i for i in indexes if i.get("expireAfterSeconds")), 
            None
        )
        ttl_seconds = ttl_idx["expireAfterSeconds"] if ttl_idx else 0

        # Estimation: oldest_date + ttl_seconds
        if oldest_date and ttl_seconds:
            expiry = oldest_date + timedelta(seconds=ttl_seconds)
            time_to_expiry = expiry - datetime.utcnow()
            expiry_str = (
                f"{expiry.isoformat()}Z "
                f"({max(0, time_to_expiry.days)}j "
                f"{max(0, time_to_expiry.seconds//3600)}h restants)"
            )
        else:
            expiry_str = "N/A"

        return {
            "collection": coll_name,
            "oldest_doc": oldest_date.isoformat() + "Z" if oldest_date else "N/A",
            "youngest_doc": youngest_date.isoformat() + "Z" if youngest_date else "N/A",
            "next_expiry": expiry_str,
        }
    except Exception as e:
        return {
            "collection": coll_name,
            "oldest_doc": "ERROR",
            "youngest_doc": "ERROR",
            "next_expiry": str(e),
        }


def get_server_health() -> Dict[str, Any]:
    """
    État général du serveur MongoDB.

    Returns:
        Dict avec version, uptime, connections, memory.
    """
    client = get_client()

    # Server status
    status = client.admin.command("serverStatus")

    # Host info
    host_info = client.admin.command("hostInfo")

    return {
        "version": status["version"],
        "uptime_hours": round(status["uptime"] / 3600, 1),
        "process": status["process"],
        "host": host_info["system"]["hostname"],
        "cpu_cores": host_info["system"]["numCores"],
        "mem_resident_mb": round(status["mem"]["resident"] / 1024, 1) if "mem" in status else -1,
    }


# ============================================================================
# STEP 3: Rendu du tableau de bord
# ============================================================================

def _simple_table(rows, headers=None):
    """Simple table formatter without tabulate dependency."""
    if not rows:
        return "No data"

    # Calculate column widths
    if headers:
        all_rows = [headers] + rows
    else:
        all_rows = rows

    num_cols = len(all_rows[0])
    widths = [0] * num_cols

    for row in all_rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))

    # Build table
    lines = []

    if headers:
        header_row = " | ".join(str(h).ljust(widths[i]) for i, h in enumerate(headers))
        lines.append(header_row)
        lines.append("-" * len(header_row))

    for row in rows:
        lines.append(" | ".join(str(cell).ljust(widths[i]) for i, cell in enumerate(row)))

    return "\n".join(lines)


def print_health_dashboard():
    """
    Affiche le tableau de bord complet de santé MongoDB.

    Format optimisé pour la lisibilité en CLI et les logs Docker.
    """
    db = get_db()

    print(f"\n{'='*80}")
    print(f"  MONGODB HEALTHCHECK — {datetime.utcnow().isoformat()}Z")
    print(f"{'='*80}")

    # ------------------------------------------------------------------------
    # SECTION A: État du serveur
    # ------------------------------------------------------------------------
    print("\n📊 SERVER STATUS")
    print("-" * 80)
    server = get_server_health()
    server_rows = [
        ["Version", server["version"]],
        ["Uptime", f"{server['uptime_hours']}h"],
        ["Process", server["process"]],
        ["Host", server["host"]],
        ["CPU Cores", server["cpu_cores"]],
        ["Resident Memory", f"{server['mem_resident_mb']} MB"],
    ]
    print(_simple_table(server_rows))

    # ------------------------------------------------------------------------
    # SECTION B: Pool de connexions
    # ------------------------------------------------------------------------
    print("\n🔌 CONNECTION POOL")
    print("-" * 80)
    pool = get_pool_status()
    pool_rows = [
        ["Current Connections", pool["current"]],
        ["Available", pool["available"]],
        ["Total Created", pool["total_created"]],
        ["Pool Address", pool["pool_address"]],
    ]
    print(_simple_table(pool_rows))

    # ------------------------------------------------------------------------
    # SECTION C: Collections — Tailles et Indexes
    # ------------------------------------------------------------------------
    print("\n📦 COLLECTIONS — Sizes & Indexes")
    print("-" * 80)

    coll_rows = []
    for coll_name, ttl_label, ttl_field in MONITORED_COLLECTIONS:
        if coll_name in db.list_collection_names():
            stats = get_collection_stats(db, coll_name)
            coll_rows.append([
                coll_name,
                stats["documents"],
                f"{stats['size_mb']} MB",
                f"{stats['index_size_mb']} MB",
                stats["index_count"],
                ttl_label,
                stats["ttl_status"],
            ])
        else:
            coll_rows.append([coll_name, "—", "—", "—", "—", ttl_label, "❌ Absente"])

    headers = ["Collection", "Docs", "Data Size", "Index Size", "Idx", "Policy", "TTL"]
    print(_simple_table(coll_rows, headers))

    # ------------------------------------------------------------------------
    # SECTION D: TTL Expiry Forecast
    # ------------------------------------------------------------------------
    print("\n⏳ TTL EXPIRY FORECAST")
    print("-" * 80)

    ttl_rows = []
    for coll_name, ttl_label, ttl_field in MONITORED_COLLECTIONS:
        if coll_name in db.list_collection_names():
            forecast = get_ttl_expiry_forecast(db, coll_name, ttl_field)
            ttl_rows.append([
                coll_name,
                forecast["oldest_doc"],
                forecast["next_expiry"],
            ])
        else:
            ttl_rows.append([coll_name, "N/A", "Collection absente"])

    ttl_headers = ["Collection", "Oldest Document", "Next Expiry Estimate"]
    print(_simple_table(ttl_rows, ttl_headers))

    # ------------------------------------------------------------------------
    # SECTION E: Index Usage Verification (Sample Queries)
    # ------------------------------------------------------------------------
    print("\n🔍 INDEX USAGE VERIFICATION (Sample Queries)")
    print("-" * 80)

    test_queries = [
        ("meters_aggregated_15min", {"zone": "B", "window_start": {"$gte": datetime.utcnow() - timedelta(days=1)}}),
        ("incidents_enriched", {"zone": "A", "timestamp": {"$gte": datetime.utcnow() - timedelta(days=7)}}),
        ("feedback_nlp", {"sentiment_predicted": "NEGATIVE", "timestamp": {"$gte": datetime.utcnow() - timedelta(days=1)}}),
    ]

    usage_rows = []
    for coll_name, query in test_queries:
        if coll_name in db.list_collection_names():
            result = check_index_usage(coll_name, query)
            usage_rows.append([
                coll_name,
                str(query)[:50] + "..." if len(str(query)) > 50 else str(query),
                result["stage"],
                result["index_name"],
                "✅" if result["is_index_scan"] else "⚠️ COLLSCAN!",
            ])
        else:
            usage_rows.append([coll_name, str(query)[:50], "N/A", "N/A", "Collection absente"])

    usage_headers = ["Collection", "Query Filter", "Plan Stage", "Index Used", "Status"]
    print(_simple_table(usage_rows, usage_headers))

    # ------------------------------------------------------------------------
    # SECTION F: Résumé et alertes
    # ------------------------------------------------------------------------
    print(f"\n{'='*80}")
    print("  RÉSUMÉ")
    print(f"{'='*80}")

    warnings = []
    for coll_name, _, _ in MONITORED_COLLECTIONS:
        if coll_name not in db.list_collection_names():
            warnings.append(f"  ⚠️  Collection manquante: {coll_name}")

    pool = get_pool_status()
    if pool["available"] < 5:
        warnings.append(f"  🚨 Pool presque épuisé! Available: {pool['available']}")

    if warnings:
        print("\nALERTES:")
        for w in warnings:
            print(w)
    else:
        print("\n✅ Tous les contrôles sont verts.")

    print(f"\n{'='*80}\n")


# ============================================================================
# STEP 4: Point d'entrée CLI
# ============================================================================

if __name__ == "__main__":
    try:
        print_health_dashboard()
    except Exception as e:
        print(f"\n❌ HEALTHCHECK FAILED: {e}\n")
        sys.exit(1)
        