import numpy as np
import lmdb
from logic.create_csr import INDPTR_FILE, INDICES_FILE, DATA_FILE, LMDB_PATH

CONVERGENCE_THRESHOLD = 1e-6
DAMPING_FACTOR = 0.85  # probability of following links (1-a in the formula)
MAX_ITERATIONS = 100
TELEPORT_PROB = 1.0 - DAMPING_FACTOR  # 'a' in P^ = (1-a)P + a/n * J

def load_csr():
    """Load CSR arrays produced by create_csr."""
    indptr = np.memmap(INDPTR_FILE, dtype=np.int64, mode="r")
    indices = np.memmap(INDICES_FILE, dtype=np.int32, mode="r")
    data = np.memmap(DATA_FILE, dtype=np.float32, mode="r")
    n = len(indptr) - 1
    if n <= 0:
        raise ValueError("Indptr file is empty; run create_csr first.")
    return indptr, indices, data, n

def load_id_to_url_mapping(num_nodes):
    """Load a list that maps node IDs to their URLs if available."""
    if num_nodes <= 0:
        return []

    mapping = [None] * num_nodes
    try:
        env = lmdb.open(LMDB_PATH, readonly=True, lock=False, create=False, max_dbs=1)
    except lmdb.Error as exc:
        print(f"Warning: unable to open {LMDB_PATH} ({exc}). URL metadata will be missing.")
        return mapping

    with env.begin() as txn:
        cursor = txn.cursor()
        for url_bytes, id_bytes in cursor:
            try:
                node_id = int(id_bytes.decode("ascii"))
            except (ValueError, AttributeError):
                continue
            if 0 <= node_id < num_nodes:
                mapping[node_id] = url_bytes.decode("utf-8", errors="replace")
    env.close()
    return mapping

def print_top_urls(pagerank, id_to_url, top_k):
    """Print the top-k URLs ranked by PageRank score."""
    if pagerank.size == 0:
        print("PageRank vector is empty.")
        return

    sorted_indices = np.argsort(pagerank)[::-1]
    printed = 0
    for node_id in sorted_indices:
        url = id_to_url[node_id] if node_id < len(id_to_url) else None
        if not url:
            continue
        printed += 1
        score = pagerank[node_id]
        print(f"{printed:2d}. score={score:.6e} url={url}")
        if printed >= top_k:
            break

    if printed == 0:
        print("No URL metadata available. Did you run the crawler first?")
    elif printed < top_k:
        print(f"Only {printed} URLs had metadata.")

def power_iteration(indptr, indices, data, n):
    """Iteratively compute PageRank until convergence."""
    rank = np.full(n, 1.0 / n, dtype=np.float64)
    teleport_term = TELEPORT_PROB / n
    link_term = DAMPING_FACTOR
    dangling_mask = (indptr[1:] - indptr[:-1]) == 0
    has_dangling = bool(np.any(dangling_mask))

    for iteration in range(1, MAX_ITERATIONS + 1):
        next_rank = np.zeros_like(rank)

        # Distribute rank mass along outgoing edges
        for src in range(n):
            start, end = indptr[src], indptr[src + 1]
            if start == end:
                continue
            dests = indices[start:end]
            weights = data[start:end]
            next_rank[dests] += rank[src] * weights

        # Add dangling mass uniformly before damping
        if has_dangling:
            dangling_mass = rank[dangling_mask].sum()
            next_rank += dangling_mass / n

        next_rank = link_term * next_rank + teleport_term

        delta = np.abs(next_rank - rank).sum()
        print(f"Iteration {iteration}: delta={delta:.6e}")
        if delta < CONVERGENCE_THRESHOLD:
            print("Converged.")
            return next_rank
        rank = next_rank

    print("Reached maximum iterations without full convergence.")
    return rank
