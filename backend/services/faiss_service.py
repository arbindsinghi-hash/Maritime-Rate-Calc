import faiss
import numpy as np
import os
from typing import List
from backend.core.config import settings
from backend.core.llm_clients import get_embedding_client


class FAISSService:
    """FAISS-backed semantic clause retrieval using llama-3.2-nv-embedqa-1b-v2."""

    def __init__(self, dimension: int = 2048):
        self.dimension = dimension
        self.index_path = os.path.join(settings.FAISS_INDEX_DIR, "tariff_clauses.index")
        self._metadata: list[dict] = []  # parallel list: index position → clause metadata

        if os.path.exists(self.index_path):
            self.index = faiss.read_index(self.index_path)
        else:
            self.index = faiss.IndexFlatL2(self.dimension)

    # ── Embedding helper ────────────────────────────────────────
    def _embed(self, texts: List[str]) -> np.ndarray:
        """Call the embedding endpoint and return vectors as np array."""
        client = get_embedding_client()
        response = client.embeddings.create(
            model=settings.EMBEDDING_MODEL,
            input=texts,
        )
        vectors = [item.embedding for item in response.data]
        return np.array(vectors, dtype="float32")

    # ── Public API ──────────────────────────────────────────────
    def add_texts(self, texts: List[str], metadata: List[dict] | None = None):
        """Embed texts and add to FAISS index."""
        embeddings = self._embed(texts)
        self.index.add(embeddings)
        if metadata:
            self._metadata.extend(metadata)
        self.save_index()

    def search(self, query: str, k: int = 5):
        """Semantic search: return (distances, indices, metadata)."""
        query_vec = self._embed([query])
        distances, indices = self.index.search(query_vec, k)
        results_meta = [
            self._metadata[i] if i < len(self._metadata) else {}
            for i in indices[0]
        ]
        return distances, indices, results_meta

    def save_index(self):
        os.makedirs(settings.FAISS_INDEX_DIR, exist_ok=True)
        faiss.write_index(self.index, self.index_path)


faiss_service = FAISSService()
