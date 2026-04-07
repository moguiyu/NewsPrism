"""Repository layer — SQLite persistence.

Public API re-exported here so callers import from newsprism.repo,
not newsprism.repo.db, keeping the internal layout flexible.
"""
from newsprism.repo.db import (
    DB_PATH,
    delete_clusters_for_date,
    get_articles_by_ids,
    get_article_id_by_url,
    get_report_article_ids,
    get_clusters_for_date,
    get_recent_clusters,
    get_unclustered_articles,
    init_db,
    insert_article,
    insert_cluster,
    insert_search_request_event,
    mark_articles_clustered,
    mark_cluster_published,
    reset_articles_clustered,
    update_article_embedding,
)

__all__ = [
    "DB_PATH",
    "init_db",
    "delete_clusters_for_date",
    "insert_article",
    "get_article_id_by_url",
    "get_unclustered_articles",
    "get_articles_by_ids",
    "get_report_article_ids",
    "get_recent_clusters",
    "mark_articles_clustered",
    "update_article_embedding",
    "insert_cluster",
    "insert_search_request_event",
    "get_clusters_for_date",
    "mark_cluster_published",
    "reset_articles_clustered",
]
