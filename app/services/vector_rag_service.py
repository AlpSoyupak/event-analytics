"""Semantic code-search RAG using LangChain FAISS + HuggingFace embeddings.

Replaces BM25 keyword matching in rag_service with cosine-similarity vector
search over the same function/class-boundary chunks. Falls back silently if
build_index() has not been called yet.

The embedding model (all-MiniLM-L6-v2, ~90 MB) is downloaded on first use
from HuggingFace Hub and cached locally by sentence-transformers.
"""
import re
from pathlib import Path
from typing import Optional

from langchain_community.vectorstores import FAISS
from langchain_core.documents import Document
from langchain_huggingface import HuggingFaceEmbeddings

_DEF_RE = re.compile(r'\n(?=(?:async )?def |class )')
_SKIP_DIRS = {"__pycache__", "alembic", "migrations", ".git"}
_EMBEDDING_MODEL = "all-MiniLM-L6-v2"


class VectorRAGService:
    """Semantic search over project Python source using FAISS + sentence-transformers.

    Call build_index() once at startup or after code changes. retrieve() then
    uses cosine similarity instead of BM25 keyword overlap, returning better
    results for natural-language queries like "how does the retention query work?".
    """

    def __init__(self) -> None:
        self._store: Optional[FAISS] = None
        self._embeddings: Optional[HuggingFaceEmbeddings] = None

    def _get_embeddings(self) -> HuggingFaceEmbeddings:
        if self._embeddings is None:
            self._embeddings = HuggingFaceEmbeddings(model_name=_EMBEDDING_MODEL)
        return self._embeddings

    def build_index(self, root: str = "app") -> int:
        """Walk *root*, chunk each .py file by definition boundaries, build FAISS index.

        Returns the number of chunks indexed.
        """
        docs: list[Document] = []

        for path in sorted(Path(root).rglob("*.py")):
            if any(part in _SKIP_DIRS for part in path.parts):
                continue
            try:
                source = path.read_text(errors="ignore")
            except OSError:
                continue

            for part in _DEF_RE.split(source):
                part = part.strip()
                if len(part) < 40:
                    continue
                first_line = part.split("\n", 1)[0]
                docs.append(Document(
                    page_content=part[:1500],
                    metadata={
                        "file": str(path),
                        "label": f"{path}: {first_line[:80]}",
                    },
                ))

        if docs:
            self._store = FAISS.from_documents(docs, self._get_embeddings())

        return len(docs)

    def retrieve(self, query: str, top_k: int = 3) -> str:
        """Return the top-k semantically similar code chunks for *query*."""
        if not self._store:
            return ""

        results = self._store.similarity_search(query, k=top_k)
        parts = [
            f"# {doc.metadata['label']}\n{doc.page_content}"
            for doc in results
        ]
        return "\n\n---\n\n".join(parts)


vector_rag_service = VectorRAGService()
