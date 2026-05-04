import re
from pathlib import Path

from rank_bm25 import BM25Okapi

_DEF_RE = re.compile(r'\n(?=(?:async )?def |class )')
_WORD_RE = re.compile(r'\w+')
_SKIP_DIRS = {"__pycache__", "alembic", "migrations", ".git"}


class RAGService:
    """BM25 index over the project's Python source files.

    Chunks files by function/class boundaries so retrieved snippets map
    to meaningful units of code rather than arbitrary line windows.
    """

    def __init__(self) -> None:
        self._bm25: BM25Okapi | None = None
        self._chunks: list[dict] = []

    def _tokenize(self, text: str) -> list[str]:
        return _WORD_RE.findall(text.lower())

    def build_index(self, root: str = "app") -> int:
        """Walk *root*, split each .py file by definition boundaries, build BM25 index.

        Returns the number of chunks indexed.
        """
        self._chunks = []

        for path in sorted(Path(root).rglob("*.py")):
            if any(part in _SKIP_DIRS for part in path.parts):
                continue
            try:
                source = path.read_text(errors="ignore")
            except OSError:
                continue

            parts = _DEF_RE.split(source)
            for part in parts:
                part = part.strip()
                if len(part) < 40:
                    continue
                first_line = part.split("\n", 1)[0]
                self._chunks.append({
                    "file": str(path),
                    "content": part[:1500],
                    "label": f"{path}: {first_line[:80]}",
                })

        if self._chunks:
            corpus = [self._tokenize(c["content"]) for c in self._chunks]
            self._bm25 = BM25Okapi(corpus)

        return len(self._chunks)

    def retrieve(self, query: str, top_k: int = 3) -> str:
        """Return the top-k most relevant code chunks for *query*, formatted as a string."""
        if not self._bm25 or not self._chunks:
            return ""

        tokens = self._tokenize(query)
        scores = self._bm25.get_scores(tokens)
        top_idx = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)[:top_k]
        parts = [
            f"# {self._chunks[i]['label']}\n{self._chunks[i]['content']}"
            for i in top_idx
        ]
        return "\n\n---\n\n".join(parts)


rag_service = RAGService()
