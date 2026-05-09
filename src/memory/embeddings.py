import os
import logging
from typing import List, Optional, Tuple

import numpy as np
import requests as _requests

logger = logging.getLogger(__name__)

ZAI_EMBEDDING_DIMENSION = 1536
SENTENCE_TRANSFORMER_DIMENSION = 384
TARGET_DIMENSION = 1536

PROVIDER_ZAI = "z-ai"
PROVIDER_ST = "sentence-transformers"


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
            "ZAI_API_BASE", "https://api.z-ai.xyz/v1"
        )
        self._st_model = None
        self._use_zai = bool(self.api_key)

    @property
    def provider_name(self) -> str:
        return PROVIDER_ZAI if self._use_zai else PROVIDER_ST

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
        embedding, _ = self.generate_with_provider(text)
        return embedding

    def generate_with_provider(self, text: str) -> Tuple[List[float], str]:
        if not text or not text.strip():
            return [0.0] * TARGET_DIMENSION, "none"

        if self._use_zai:
            try:
                return self._generate_zai(text), PROVIDER_ZAI
            except Exception as e:
                raise RuntimeError(
                    f"z-ai embedding failed: {e}. "
                    "Falling back to sentence-transformers would mix incompatible "
                    "embedding spaces. Either fix the z-ai connection or set "
                    "ZAI_API_KEY to empty to use sentence-transformers exclusively."
                ) from e

        return self._generate_st(text), PROVIDER_ST

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
                raise RuntimeError(
                    f"z-ai batch embedding failed: {e}. "
                    "Cannot fall back to sentence-transformers as it would mix "
                    "incompatible embedding spaces."
                ) from e

        model = self._get_st_model()
        vecs = model.encode(texts, normalize_embeddings=True)
        results = []
        for vec in vecs:
            padded = np.zeros(TARGET_DIMENSION, dtype=np.float32)
            dim = min(len(vec), TARGET_DIMENSION)
            padded[:dim] = vec[:dim]
            results.append(padded.tolist())
        return results
