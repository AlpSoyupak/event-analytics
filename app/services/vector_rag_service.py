"""Semantic code-search RAG using LangChain FAISS + HuggingFace embeddings.

This service is **optional** — it requires four extra packages that are not
installed by default (see requirements.txt). Call build_index() only after
uncommenting and reinstalling those deps; it will raise ImportError with a
clear message otherwise.

When the index is built, retrieve() uses cosine similarity instead of BM25.
"""
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from langchain_community.vectorstores import FAISS
    from langchain_huggingface import HuggingFaceEmbeddings

_DEF_RE = re.compile(r'\n(?=(?:async )?def |class )')
_SKIP_DIRS = {"__pycache__", "alembic", "migrations", ".git"}
_EMBEDDING_MODEL = "all-MiniLM-L6-v2"

_MISSING_DEPS_MSG = (
    "Vector RAG dependencies are not installed. "
    "Uncomment the four optional lines in requirements.txt "
    "(langchain-community, langchain-huggingface, faiss-cpu, sentence-transformers) "
    "and rebuild the Docker image."
)


class VectorRAGService:
    """Semantic search over project Python source using FAISS + sentence-transformers.

    All heavy imports are deferred to build_index() so the module loads
    cleanly even when the optional packages are absent.
    """

    def __init__(self) -> None:
        self._store: Optional[Any] = None
        self._embeddings: Optional[Any] = None

    def _get_embeddings(self) -> Any:
        if self._embeddings is None:
            try:
                from langchain_huggingface import HuggingFaceEmbeddings
            except ImportError:
                raise ImportError(_MISSING_DEPS_MSG)
            self._embeddings = HuggingFaceEmbeddings(model_name=_EMBEDDING_MODEL)
        return self._embeddings

    def build_index(self, root: str = "app") -> int:
        """Walk *root*, chunk each .py file by definition boundaries, build FAISS index.

        Raises ImportError if the optional dependencies are not installed.
        Returns the number of chunks indexed.
        """
        try:
            from langchain_community.vectorstores import FAISS
            from langchain_core.documents import Document
        except ImportError:
            raise ImportError(_MISSING_DEPS_MSG)

        docs: list[Any] = []

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
        """Return the top-k semantically similar code chunks for *query*.

        Returns an empty string if the index has not been built yet.
        """
        if not self._store:
            return ""

        results = self._store.similarity_search(query, k=top_k)
        parts = [
            f"# {doc.metadata['label']}\n{doc.page_content}"
            for doc in results
        ]
        return "\n\n---\n\n".join(parts)


vector_rag_service = VectorRAGService()
