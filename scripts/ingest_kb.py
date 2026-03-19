#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path


CHUNK_SIZE = 500
CHUNK_OVERLAP = 50
COLLECTION_NAME = "upi_knowledge"
EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest markdown KB files into ChromaDB")
    parser.add_argument(
        "--kb-dir",
        default="backend/data/kb",
        help="Directory containing markdown knowledge base files",
    )
    parser.add_argument(
        "--persist-dir",
        default="backend/data/chroma",
        help="Directory where ChromaDB will persist data",
    )
    return parser.parse_args(argv)


def split_text(text: str, *, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> list[str]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    if overlap < 0 or overlap >= chunk_size:
        raise ValueError("overlap must be >= 0 and < chunk_size")

    text = text.strip()
    if not text:
        return []

    chunks: list[str] = []
    start = 0
    step = chunk_size - overlap
    while start < len(text):
        end = min(len(text), start + chunk_size)
        chunks.append(text[start:end])
        if end >= len(text):
            break
        start += step
    return chunks


def load_markdown_files(kb_dir: Path) -> list[Path]:
    if not kb_dir.exists():
        raise FileNotFoundError(f"KB directory not found: {kb_dir}")
    return sorted(path for path in kb_dir.glob("*.md") if path.is_file())


def build_documents(paths: list[Path]) -> tuple[list[str], list[dict[str, object]], list[str]]:
    texts: list[str] = []
    metadatas: list[dict[str, object]] = []
    ids: list[str] = []

    for path in paths:
        content = path.read_text(encoding="utf-8").strip()
        for index, chunk in enumerate(split_text(content), start=1):
            texts.append(chunk)
            metadatas.append(
                {
                    "source": str(path),
                    "filename": path.name,
                    "chunk_index": index,
                }
            )
            ids.append(f"{path.stem}:{index}")
    return texts, metadatas, ids


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    kb_dir = Path(args.kb_dir)
    persist_dir = Path(args.persist_dir)

    try:
        from langchain_community.embeddings import HuggingFaceEmbeddings
        from langchain_community.vectorstores import Chroma
    except Exception as exc:
        print(
            "Missing dependencies for KB ingestion. Install required packages first:\n"
            "  python3 -m pip install -U langchain-community sentence-transformers chromadb\n"
            f"Original error: {exc}",
            file=sys.stderr,
        )
        return 2

    markdown_files = load_markdown_files(kb_dir)
    if not markdown_files:
        print(f"No markdown files found in {kb_dir}", file=sys.stderr)
        return 2

    texts, metadatas, ids = build_documents(markdown_files)
    if not texts:
        print("No chunkable content found in KB markdown files", file=sys.stderr)
        return 2

    if persist_dir.exists():
        shutil.rmtree(persist_dir)
    persist_dir.mkdir(parents=True, exist_ok=True)

    embeddings = HuggingFaceEmbeddings(model_name=EMBEDDING_MODEL)
    vectorstore = Chroma(
        collection_name=COLLECTION_NAME,
        embedding_function=embeddings,
        persist_directory=str(persist_dir),
    )
    vectorstore.add_texts(texts=texts, metadatas=metadatas, ids=ids)

    print(len(texts))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

