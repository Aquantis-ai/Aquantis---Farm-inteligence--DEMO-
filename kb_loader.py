import os
from pathlib import Path

def load_txt_files(kb_root: str):
    kb_path = Path(kb_root)
    docs = []
    for path in kb_path.rglob("*.txt"):
        text = path.read_text(encoding="utf-8", errors="ignore")
        # domain = složka nad souborem (RAS, POND, ...)
        domain = path.parent.name
        source = str(path.relative_to(kb_path))
        docs.append({"text": text, "source": source, "domain": domain})
    return docs

def chunk_text(text: str, chunk_size: int = 1200, overlap: int = 200):
    """
    Jednoduché chunkování po znacích.
    Pro začátek stačí. Později lze dělat chunking po odstavcích.
    """
    chunks = []
    i = 0
    n = len(text)
    while i < n:
        chunk = text[i:i+chunk_size]
        chunks.append(chunk)
        i += max(1, chunk_size - overlap)
    return chunks

def build_chunks(docs, chunk_size=1200, overlap=200):
    out = []
    for d in docs:
        chunks = chunk_text(d["text"], chunk_size=chunk_size, overlap=overlap)
        for idx, ch in enumerate(chunks):
            out.append({
                "text": ch.strip(),
                "source": d["source"],
                "domain": d["domain"],
                "chunk_id": idx
            })
    return out
