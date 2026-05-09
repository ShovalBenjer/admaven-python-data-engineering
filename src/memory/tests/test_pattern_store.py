import unittest
from unittest.mock import patch, MagicMock
import numpy as np
from memory.embeddings import EmbeddingGenerator, TARGET_DIMENSION
from memory.pattern_store import FraudPattern, PatternStore, CREATE_TABLE_SQL


class TestEmbeddingGenerator(unittest.TestCase):
    def setUp(self):
        self.gen = EmbeddingGenerator(api_key=None)

    def test_empty_text_returns_zeros(self):
        result = self.gen.generate("")
        self.assertEqual(len(result), TARGET_DIMENSION)
        self.assertTrue(all(v == 0.0 for v in result))

    def test_whitespace_text_returns_zeros(self):
        result = self.gen.generate("   ")
        self.assertEqual(len(result), TARGET_DIMENSION)
        self.assertTrue(all(v == 0.0 for v in result))

    @patch("memory.embeddings._requests.post")
    def test_zai_embedding_success(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "data": [{"embedding": [0.1] * TARGET_DIMENSION, "index": 0}]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        gen = EmbeddingGenerator(api_key="test-key")
        result = gen.generate("click fraud pattern")
        self.assertEqual(len(result), TARGET_DIMENSION)
        mock_post.assert_called_once()

    @patch("memory.embeddings._requests.post")
    def test_zai_failure_falls_back_to_st(self, mock_post):
        mock_post.side_effect = Exception("API down")
        gen = EmbeddingGenerator(api_key="test-key")
        with patch.object(gen, "_generate_st", return_value=[0.0] * TARGET_DIMENSION) as mock_st:
            result = gen.generate("test pattern")
            mock_st.assert_called_once()
            self.assertEqual(len(result), TARGET_DIMENSION)

    @patch("memory.embeddings._requests.post")
    def test_batch_embedding(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "data": [
                {"embedding": [0.1] * TARGET_DIMENSION, "index": 0},
                {"embedding": [0.2] * TARGET_DIMENSION, "index": 1},
            ]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        gen = EmbeddingGenerator(api_key="test-key")
        results = gen.generate_batch(["text1", "text2"])
        self.assertEqual(len(results), 2)
        self.assertEqual(len(results[0]), TARGET_DIMENSION)


class TestFraudPattern(unittest.TestCase):
    def test_to_dict_excludes_embedding(self):
        p = FraudPattern(
            advertiser_id="601040",
            campaign_id="653344",
            fraud_type="click_flood",
            confidence_score=0.95,
            pattern_description="400% traffic spike near-zero conversions",
            pattern_embedding=[0.1] * 100,
        )
        d = p.to_dict()
        self.assertNotIn("pattern_embedding", d)
        self.assertEqual(d["advertiser_id"], "601040")
        self.assertEqual(d["fraud_type"], "click_flood")

    def test_default_metadata(self):
        p = FraudPattern(
            advertiser_id="A1",
            campaign_id="C1",
            fraud_type="fraud",
            confidence_score=0.5,
        )
        self.assertEqual(p.metadata, {})


class TestPatternStore(unittest.TestCase):
    def test_create_table_sql_contains_required_columns(self):
        self.assertIn("pattern_embedding vector(1536)", CREATE_TABLE_SQL)
        self.assertIn("advertiser_id", CREATE_TABLE_SQL)
        self.assertIn("campaign_id", CREATE_TABLE_SQL)
        self.assertIn("fraud_type", CREATE_TABLE_SQL)
        self.assertIn("confidence_score", CREATE_TABLE_SQL)
        self.assertIn("created_at", CREATE_TABLE_SQL)
        self.assertIn("last_seen", CREATE_TABLE_SQL)

    def test_create_table_sql_has_indexes(self):
        self.assertIn("idx_fraud_patterns_embedding", CREATE_TABLE_SQL)
        self.assertIn("idx_fraud_patterns_advertiser", CREATE_TABLE_SQL)
        self.assertIn("idx_fraud_patterns_fraud_type", CREATE_TABLE_SQL)

    @patch("memory.pattern_store.psycopg2.connect")
    @patch("memory.pattern_store.register_vector")
    def test_connect(self, mock_register, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        store = PatternStore(dsn="postgresql://test:test@localhost/db")
        store.connect()
        mock_connect.assert_called_once_with("postgresql://test:test@localhost/db")
        mock_register.assert_called_once_with(mock_conn)

    @patch("memory.pattern_store.psycopg2.connect")
    @patch("memory.pattern_store.register_vector")
    def test_initialize_schema(self, mock_register, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.closed = 0
        mock_connect.return_value = mock_conn
        store = PatternStore(dsn="postgresql://test:test@localhost/db")
        store.connect()
        store.initialize_schema()
        mock_cursor.execute.assert_called_once_with(CREATE_TABLE_SQL)

    @patch("memory.pattern_store.psycopg2.connect")
    @patch("memory.pattern_store.register_vector")
    def test_store_pattern(self, mock_register, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [42]
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.closed = 0
        mock_connect.return_value = mock_conn

        store = PatternStore(dsn="postgresql://test:test@localhost/db")
        store.connect()

        pattern = FraudPattern(
            advertiser_id="601040",
            campaign_id="653344",
            fraud_type="click_flood",
            confidence_score=0.95,
            pattern_embedding=[0.1] * TARGET_DIMENSION,
        )
        row_id = store.store_pattern(pattern)
        self.assertEqual(row_id, 42)
        mock_cursor.execute.assert_called_once()

    @patch("memory.pattern_store.psycopg2.connect")
    @patch("memory.pattern_store.register_vector")
    def test_find_similar_patterns(self, mock_register, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (1, "601040", "653344", "click_flood", 0.95, "desc", "{}", "2025-01-01", "2025-01-02", 0.98)
        ]
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.closed = 0
        mock_connect.return_value = mock_conn

        store = PatternStore(dsn="postgresql://test:test@localhost/db")
        store.connect()

        results = store.find_similar_patterns([0.1] * TARGET_DIMENSION, k=5)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["advertiser_id"], "601040")
        self.assertAlmostEqual(results[0]["similarity"], 0.98)

    @patch("memory.pattern_store.psycopg2.connect")
    @patch("memory.pattern_store.register_vector")
    def test_get_pattern_stats(self, mock_register, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (10, 3, 4, 0.75, "2025-01-02")
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.closed = 0
        mock_connect.return_value = mock_conn

        store = PatternStore(dsn="postgresql://test:test@localhost/db")
        store.connect()

        stats = store.get_pattern_stats()
        self.assertEqual(stats["total_patterns"], 10)
        self.assertEqual(stats["unique_advertisers"], 3)


if __name__ == "__main__":
    unittest.main()
