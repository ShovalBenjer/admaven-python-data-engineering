import os
import logging
from typing import List, Optional

import numpy as np
import requests as _requests

logger = logging.getLogger(__name__)

ZAI_EMBEDDING_DIMENSION = 1536
SENTENCE_TRANSFORMER_DIMENSION = 384
TARGET_DIMENSION = 1536


class EmbeddingGenerator:
    def __init__(
        self,
        model: Optional[str] = None,
        api_key: Optional[str] = None,
        api_base: Optional[str] = None,
    ):
        self.model = model or os.getenv("ZAI_EMBEDDING_MODEL", "z-ai/glm-5.1")
        self.api_key = api_key or os.getenv("ZAI_API_KEY")
        self.api_base = api_base or os.getenv(
            "ZAI_API_BASE", "https://api.z-ai/v1"
        )
        self._st_model = None
        self._use_zai = bool(self.api_key)

    def _generate_zai(self, text: str) -> List[float]:
        resp = _requests.post(
            f"{self.api_base}/embeddings",
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            json={"model": self.model, "input": text},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        embedding = data["data"][0]["embedding"]
        logger.debug("Generated z-ai embedding dim=%d", len(embedding))
        return embedding

    def _get_st_model(self):
        if self._st_model is None:
            from sentence_transformers import SentenceTransformer

            self._st_model = SentenceTransformer("all-MiniLM-L6-v2")
            logger.info("Loaded sentence-transformers model all-MiniLM-L6-v2")
        return self._st_model

    def _generate_st(self, text: str) -> List[float]:
        model = self._get_st_model()
        vec = model.encode(text, normalize_embeddings=True)
        padded = np.zeros(TARGET_DIMENSION, dtype=np.float32)
        dim = min(len(vec), TARGET_DIMENSION)
        padded[:dim] = vec[:dim]
        return padded.tolist()

    def generate(self, text: str) -> List[float]:
        if not text or not text.strip():
            return [0.0] * TARGET_DIMENSION

        if self._use_zai:
            try:
                return self._generate_zai(text)
            except Exception as e:
                logger.warning(
                    "z-ai embedding failed (%s), falling back to sentence-transformers", e
                )

        return self._generate_st(text)

    def generate_batch(self, texts: List[str]) -> List[List[float]]:
        if self._use_zai:
            try:
                resp = _requests.post(
                    f"{self.api_base}/embeddings",
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json",
                    },
                    json={"model": self.model, "input": texts},
                    timeout=60,
                )
                resp.raise_for_status()
                data = resp.json()
                embeddings = [d["embedding"] for d in sorted(data["data"], key=lambda x: x["index"])]
                logger.debug("Generated %d z-ai embeddings", len(embeddings))
                return embeddings
            except Exception as e:
                logger.warning(
                    "z-ai batch embedding failed (%s), falling back to sentence-transformers", e
                )

        model = self._get_st_model()
        vecs = model.encode(texts, normalize_embeddings=True)
        results = []
        for vec in vecs:
            padded = np.zeros(TARGET_DIMENSION, dtype=np.float32)
            dim = min(len(vec), TARGET_DIMENSION)
            padded[:dim] = vec[:dim]
            results.append(padded.tolist())
        return results
