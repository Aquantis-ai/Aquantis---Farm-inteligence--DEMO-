import os
import json
import numpy as np
import streamlit as st
from dotenv import load_dotenv
from openai import OpenAI
from typing import Optional, List

load_dotenv()

try:
    api_key = st.secrets["OPENAI_API_KEY"]
except Exception:
    api_key = os.environ.get("OPENAI_API_KEY")

client = OpenAI(api_key=api_key)

INDEX_DIR = "rag_index"
EMBED_MODEL = "text-embedding-3-small"

# Načti index při importu (rychlé pro Streamlit, ideálně cache v app.py)
_vectors = None
_meta = None


def _load_index():
    global _vectors, _meta
    if _vectors is None:
        data = np.load(os.path.join(INDEX_DIR, "index.npz"))
        _vectors = data["vectors"]
    if _meta is None:
        with open(os.path.join(INDEX_DIR, "meta.json"), "r", encoding="utf-8") as f:
            _meta = json.load(f)


def _embed_query(q: str) -> np.ndarray:
    resp = client.embeddings.create(model=EMBED_MODEL, input=q)
    v = np.array(resp.data[0].embedding, dtype=np.float32)
    return v


def _cosine_sim_matrix(vectors: np.ndarray, q: np.ndarray) -> np.ndarray:
    # cosine sim = dot(normalized)
    v_norm = vectors / (np.linalg.norm(vectors, axis=1, keepdims=True) + 1e-10)
    q_norm = q / (np.linalg.norm(q) + 1e-10)
    return v_norm @ q_norm


def retrieve(
    query: str,
    k: int = 5,
    domain: Optional[str] = None,
    domains: Optional[List[str]] = None,
):
    """
    Multi-domain retrieval.

    - domain: jedna doména (zpětná kompatibilita)
    - domains: seznam domén (nové, doporučené)
      Pokud je domains zadáno, má prioritu před domain.

    Vrací list hitů:
    {text, source, domain, chunk_id, score, rank}
    """
    _load_index()

    # 1) query embedding + cosine similarity
    qv = _embed_query(query)
    sims = _cosine_sim_matrix(_vectors, qv)

    # 2) Filtrace domén
    # domains má prioritu
    selected_domains = None
    if domains is not None:
        # vyčisti a normalizuj (odstraň None, duplicity)
        selected_domains = [d for d in domains if d]
        selected_domains = list(dict.fromkeys(selected_domains))  # zachová pořadí, odstraní duplicity
    elif domain:
        selected_domains = [domain]

    if selected_domains:
        # rychlá maska: domain in selected_domains
        allowed = set(selected_domains)
        mask = np.array([m.get("domain") in allowed for m in _meta], dtype=bool)

        if not mask.any():
            return []

        idxs = np.nonzero(mask)[0]          # globální indexy, které prošly filtrem
        sims_filtered = sims[mask]          # odpovídající similarity
        top_local = np.argsort(-sims_filtered)[:k]

        top_global = idxs[top_local]
        top_scores = sims_filtered[top_local]
    else:
        # bez filtru
        top_global = np.argsort(-sims)[:k]
        top_scores = sims[top_global]

    # 3) Sestav hits
    hits = []
    for rank, (gi, score) in enumerate(zip(top_global, top_scores), start=1):
        m = _meta[int(gi)]
        hits.append(
            {
                "text": m.get("text", ""),
                "source": m.get("source"),
                "domain": m.get("domain"),
                "chunk_id": m.get("chunk_id"),
                "score": float(score),
                "rank": rank,
            }
        )

    return hits


# ==========================================================
# Added helpers (non-breaking): for diagnostics KB-only usage
# ==========================================================

def rag_retrieve_multi(query: str, domains: Optional[List[str]] = None, k: int = 5):
    """
    Wrapper for newer modules expecting:
      hits, domains_used, supports_domains

    domains are passed into retrieve(domains=...).

    Returns:
      hits: list[dict]
      domains_used: list[str] (unique domains found in hits)
      supports_domains: bool (True if domains filter can be applied)
    """
    hits = retrieve(query=query, k=k, domains=domains)
    domains_used = []
    if hits:
        domains_used = list(dict.fromkeys([h.get("domain") for h in hits if h.get("domain")]))
    return hits, domains_used, True


def format_rag_context(hits: List[dict]) -> str:
    """
    Format hits into compact context for the LLM.
    Keeps it short and structured.
    """
    if not hits:
        return ""
    lines = []
    for h in hits:
        src = h.get("source") or "unknown"
        dom = h.get("domain") or "unknown"
        score = h.get("score")
        text = (h.get("text") or "").strip()
        if not text:
            continue
        lines.append(f"[{dom}] ({src}) score={score:.4f}\n{text}")
    return "\n\n---\n\n".join(lines).strip()


def best_score(hits: List[dict]) -> float:
    """
    Best similarity score among hits (0.0 if none).
    """
    if not hits:
        return 0.0
    try:
        return float(max(h.get("score", 0.0) for h in hits))
    except Exception:
        return 0.0