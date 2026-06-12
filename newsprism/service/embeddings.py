"""Shared multilingual sentence-embedding model — single loader for all services.

Layer: service (imports nothing from the project above types/config)
"""
from __future__ import annotations

from sentence_transformers import SentenceTransformer

_MODEL: SentenceTransformer | None = None

# paraphrase-multilingual-mpnet covers zh/en/ja/ko/ru/de/nl/pl and more —
# load once per process; dedup, clustering, history, and seeker all share it.
MODEL_NAME = "paraphrase-multilingual-mpnet-base-v2"


def get_model() -> SentenceTransformer:
    global _MODEL
    if _MODEL is None:
        _MODEL = SentenceTransformer(MODEL_NAME)
    return _MODEL
