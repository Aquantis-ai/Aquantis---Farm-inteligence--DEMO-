import os
import json
import numpy as np
from dotenv import load_dotenv
from openai import OpenAI

from kb_loader import load_txt_files, build_chunks

load_dotenv()
client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

INDEX_DIR = "rag_index"
EMBED_MODEL = "text-embedding-3-small"

KB_DIR = "knowledge_base"
CP_DIR = os.path.join("knowledge_base", "CP_DIR")  # <-- tvoje složka s Core Principles

def embed_texts(texts: list[str]) -> np.ndarray:
    vectors = []
    batch_size = 64
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i+batch_size]
        resp = client.embeddings.create(model=EMBED_MODEL, input=batch)
        vectors.extend([d.embedding for d in resp.data])
    return np.array(vectors, dtype=np.float32)

def build_and_save_index(source_dir: str, npz_name: str, meta_name: str, doc_tag: str):
    docs = load_txt_files(source_dir)
    chunks = build_chunks(docs, chunk_size=1200, overlap=200)

    # Přidej tag do metadat (aby sis později mohl filtrovat / logovat)
    for c in chunks:
        c["doc_source"] = doc_tag  # "kb" nebo "cp"

    texts = [c["text"] for c in chunks]
    vectors = embed_texts(texts)

    # Ulož vektory
    np.savez_compressed(os.path.join(INDEX_DIR, npz_name), vectors=vectors)

    # Ulož metadata
    with open(os.path.join(INDEX_DIR, meta_name), "w", encoding="utf-8") as f:
        json.dump(chunks, f, ensure_ascii=False, indent=2)

    print(f"Hotovo ({doc_tag}): {len(chunks)} chunků -> {INDEX_DIR}/{npz_name}, {INDEX_DIR}/{meta_name}")

def main():
    os.makedirs(INDEX_DIR, exist_ok=True)

    # 1) KB index (stejně jako doteď)
    build_and_save_index(
        source_dir=KB_DIR,
        npz_name="index.npz",
        meta_name="meta.json",
        doc_tag="kb"
    )

    # 2) CP index (nově)
    if os.path.isdir(CP_DIR):
        build_and_save_index(
            source_dir=CP_DIR,
            npz_name="cp_index.npz",
            meta_name="cp_meta.json",
            doc_tag="cp"
        )
    else:
        print(f"CP složka nenalezena: {CP_DIR} (přeskočeno)")

if __name__ == "__main__":
    main()
