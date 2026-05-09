import os
import json
import logging
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Dict, Any

import psycopg2
from psycopg2.extras import execute_values
from pgvector.psycopg2 import register_vector
import numpy as np

from .embeddings import EmbeddingGenerator

logger = logging.getLogger(__name__)

_MIGRATION_PATH = Path(__file__).parent / "migrations" / "001_create_fraud_patterns_table.sql"


def _load_create_table_sql() -> str:
    try:
        sql = _MIGRATION_PATH.read_text()
        sql = sql.replace("BEGIN;", "").replace("COMMIT;", "")
        return sql.strip()
    except FileNotFoundError:
        logger.warning("Migration file not found at %s, using fallback schema", _MIGRATION_PATH)
        return _FALLBACK_SQL


_FALLBACK_SQL = """
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS fraud_patterns (
    id SERIAL PRIMARY KEY,
    pattern_embedding vector(1536),
    embedding_provider VARCHAR(32),
    advertiser_id VARCHAR(64),
    campaign_id VARCHAR(64),
    fraud_type VARCHAR(128),
    confidence_score FLOAT,
    pattern_description TEXT,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    last_seen TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fraud_patterns_embedding ON fraud_patterns
    USING ivfflat (pattern_embedding vector_cosine_ops) WITH (lists = 100);

CREATE INDEX IF NOT EXISTS idx_fraud_patterns_advertiser ON fraud_patterns (advertiser_id);
CREATE INDEX IF NOT EXISTS idx_fraud_patterns_fraud_type ON fraud_patterns (fraud_type);
CREATE INDEX IF NOT EXISTS idx_fraud_patterns_last_seen ON fraud_patterns (last_seen);
CREATE INDEX IF NOT EXISTS idx_fraud_patterns_provider ON fraud_patterns (embedding_provider);
"""

CREATE_TABLE_SQL = _load_create_table_sql()


@dataclass
class FraudPattern:
    advertiser_id: str
    campaign_id: str
    fraud_type: str
    confidence_score: float
    pattern_description: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)
    pattern_embedding: Optional[List[float]] = None
    embedding_provider: Optional[str] = None
    id: Optional[int] = None
    created_at: Optional[str] = None
    last_seen: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d.pop("pattern_embedding", None)
        return d


class PatternStore:
    def __init__(
        self,
        dsn: Optional[str] = None,
        embedding_generator: Optional[EmbeddingGenerator] = None,
    ):
        self.dsn = dsn or os.getenv("DATABASE_URL")
        if not self.dsn:
            raise ValueError(
                "DATABASE_URL environment variable is required. "
                "Set it to your PostgreSQL connection string."
            )
        self._conn = None
        self._embedding_gen = embedding_generator or EmbeddingGenerator()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def connect(self) -> None:
        self._conn = psycopg2.connect(self.dsn)
        register_vector(self._conn)
        self._conn.autocommit = True
        logger.info("Connected to PostgreSQL with pgvector support")

    def close(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()
            logger.info("Closed PostgreSQL connection")

    def initialize_schema(self) -> None:
        if not self._conn or self._conn.closed:
            self.connect()
        with self._conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        logger.info("Schema initialized: fraud_patterns table ready")

    def _ensure_connected(self) -> None:
        if not self._conn or self._conn.closed:
            self.connect()

    def _get_embedding(self, text: str) -> tuple:
        embedding, provider = self._embedding_gen.generate_with_provider(text)
        return embedding, provider

    def store_pattern(self, pattern: FraudPattern) -> int:
        self._ensure_connected()
        if pattern.pattern_embedding is None:
            text = (
                pattern.pattern_description
                or f"{pattern.fraud_type} advertiser={pattern.advertiser_id} campaign={pattern.campaign_id}"
            )
            pattern.pattern_embedding, pattern.embedding_provider = self._get_embedding(text)

        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO fraud_patterns
                    (pattern_embedding, embedding_provider, advertiser_id, campaign_id, fraud_type,
                     confidence_score, pattern_description, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    np.array(pattern.pattern_embedding),
                    pattern.embedding_provider,
                    pattern.advertiser_id,
                    pattern.campaign_id,
                    pattern.fraud_type,
                    pattern.confidence_score,
                    pattern.pattern_description,
                    json.dumps(pattern.metadata),
                ),
            )
            row_id = cur.fetchone()[0]
        logger.info(
            "Stored fraud pattern id=%s fraud_type=%s advertiser=%s provider=%s",
            row_id,
            pattern.fraud_type,
            pattern.advertiser_id,
            pattern.embedding_provider,
        )
        return row_id

    def store_patterns(self, patterns: List[FraudPattern]) -> List[int]:
        self._ensure_connected()
        rows = []
        for p in patterns:
            if p.pattern_embedding is None:
                text = (
                    p.pattern_description
                    or f"{p.fraud_type} advertiser={p.advertiser_id} campaign={p.campaign_id}"
                )
                p.pattern_embedding, p.embedding_provider = self._get_embedding(text)
            rows.append(
                (
                    np.array(p.pattern_embedding),
                    p.embedding_provider,
                    p.advertiser_id,
                    p.campaign_id,
                    p.fraud_type,
                    p.confidence_score,
                    p.pattern_description,
                    json.dumps(p.metadata),
                )
            )

        with self._conn.cursor() as cur:
            result = execute_values(
                cur,
                """
                INSERT INTO fraud_patterns
                    (pattern_embedding, embedding_provider, advertiser_id, campaign_id, fraud_type,
                     confidence_score, pattern_description, metadata)
                VALUES %s
                RETURNING id
                """,
                rows,
                fetch=True,
            )
            ids = [r[0] for r in result]
        logger.info("Stored %d fraud patterns, ids=%s", len(ids), ids)
        return ids

    def find_similar_patterns(
        self,
        query_embedding: List[float],
        k: int = 5,
        min_similarity: float = 0.7,
        fraud_type: Optional[str] = None,
        advertiser_id: Optional[str] = None,
        embedding_provider: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        self._ensure_connected()
        query_vec = np.array(query_embedding)

        if embedding_provider is None:
            embedding_provider = self._embedding_gen.provider_name

        conditions = [
            "1 - (pattern_embedding <=> %s) >= %s",
            "embedding_provider = %s",
        ]
        params: List[Any] = [query_vec, min_similarity, embedding_provider]

        if fraud_type:
            conditions.append("fraud_type = %s")
            params.append(fraud_type)
        if advertiser_id:
            conditions.append("advertiser_id = %s")
            params.append(advertiser_id)

        where_clause = " AND ".join(conditions)
        params.extend([query_vec, query_vec, k])

        sql = f"""
            SELECT
                id, advertiser_id, campaign_id, fraud_type,
                confidence_score, pattern_description, metadata,
                created_at, last_seen,
                1 - (pattern_embedding <=> %s) AS similarity
            FROM fraud_patterns
            WHERE {where_clause}
            ORDER BY pattern_embedding <=> %s
            LIMIT %s
        """

        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            columns = [
                "id", "advertiser_id", "campaign_id", "fraud_type",
                "confidence_score", "pattern_description", "metadata",
                "created_at", "last_seen", "similarity",
            ]
            rows = cur.fetchall()

        results = []
        for row in rows:
            r = dict(zip(columns, row))
            if isinstance(r.get("metadata"), str):
                r["metadata"] = json.loads(r["metadata"])
            results.append(r)

        logger.info(
            "Found %d similar patterns (k=%d, min_sim=%.2f, provider=%s)",
            len(results), k, min_similarity, embedding_provider,
        )
        return results

    def find_similar_by_description(
        self,
        description: str,
        k: int = 5,
        min_similarity: float = 0.7,
        fraud_type: Optional[str] = None,
        advertiser_id: Optional[str] = None,
        embedding_provider: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        query_embedding = self._embedding_gen.generate(description)
        return self.find_similar_patterns(
            query_embedding, k=k, min_similarity=min_similarity,
            fraud_type=fraud_type, advertiser_id=advertiser_id,
            embedding_provider=embedding_provider,
        )

    def update_last_seen(self, pattern_id: int) -> None:
        self._ensure_connected()
        with self._conn.cursor() as cur:
            cur.execute(
                "UPDATE fraud_patterns SET last_seen = NOW() WHERE id = %s",
                (pattern_id,),
            )

    def get_patterns_by_advertiser(
        self, advertiser_id: str, limit: int = 100
    ) -> List[Dict[str, Any]]:
        self._ensure_connected()
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, advertiser_id, campaign_id, fraud_type,
                       confidence_score, pattern_description, metadata,
                       created_at, last_seen
                FROM fraud_patterns
                WHERE advertiser_id = %s
                ORDER BY last_seen DESC
                LIMIT %s
                """,
                (advertiser_id, limit),
            )
            columns = [
                "id", "advertiser_id", "campaign_id", "fraud_type",
                "confidence_score", "pattern_description", "metadata",
                "created_at", "last_seen",
            ]
            rows = cur.fetchall()
        results = []
        for row in rows:
            r = dict(zip(columns, row))
            if isinstance(r.get("metadata"), str):
                r["metadata"] = json.loads(r["metadata"])
            results.append(r)
        return results

    def get_pattern_stats(self) -> Dict[str, Any]:
        self._ensure_connected()
        with self._conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*) AS total_patterns,
                    COUNT(DISTINCT advertiser_id) AS unique_advertisers,
                    COUNT(DISTINCT fraud_type) AS unique_fraud_types,
                    AVG(confidence_score) AS avg_confidence,
                    MAX(last_seen) AS most_recent
                FROM fraud_patterns
            """)
            row = cur.fetchone()
            columns = [
                "total_patterns", "unique_advertisers", "unique_fraud_types",
                "avg_confidence", "most_recent",
            ]
            return dict(zip(columns, row))
