# build_csr_from_bin.py
import struct, os
import numpy as np
import lmdb

ADJ_BIN = "data/adjacency.bin"
INDPTR_FILE = "data/indptr.dat"
INDICES_FILE = "data/indices.dat"
DATA_FILE = "data/data.dat"
LMDB_PATH = "crawler.lmdb"   # adjust if needed
        
def first_pass_count_edges(N):
    """Returns E (total edges) and outdeg array (int64)."""
    outdeg = np.zeros(N, dtype=np.int64)
    E = 0
    with open(ADJ_BIN, "rb") as f:
        while True:
            hdr = f.read(8)
            if not hdr:
                break
            src_id, num = struct.unpack("<II", hdr)
            if src_id >= N:
                raise ValueError(f"src_id {src_id} >= N {N}")
            outdeg[src_id] = num
            E += num
            # skip dests (seek)
            f.seek(4 * num, os.SEEK_CUR)
    return E, outdeg

def build_memmaps(N, E, outdeg):
    # allocate memmaps
    indptr = np.memmap(INDPTR_FILE, dtype=np.int64, mode='w+', shape=(N+1,))
    indices = np.memmap(INDICES_FILE, dtype=np.int32, mode='w+', shape=(E,))
    data = np.memmap(DATA_FILE, dtype=np.float32, mode='w+', shape=(E,))
    # fill indptr via prefix sums
    indptr[0] = 0
    for i in range(N):
        indptr[i+1] = indptr[i] + outdeg[i]
    # a write cursor we will advance while filling
    fill_pos = indptr.copy()
    # pass 2: fill indices and data
    with open(ADJ_BIN, "rb") as f:
        while True:
            hdr = f.read(8)
            if not hdr:
                break
            src_id, num = struct.unpack("<II", hdr)
            if num == 0:
                continue
            dest_bytes = f.read(4 * num)
            dests = struct.unpack("<" + "I"*num, dest_bytes)
            start = int(fill_pos[src_id])
            end = start + num
            indices[start:end] = np.fromiter(dests, dtype=np.int32, count=num)
            # weight = 1/outdeg[src] for each outbound edge
            data[start:end] = 1.0 / float(outdeg[src_id])
            fill_pos[src_id] = end
    # flush
    indptr.flush(); indices.flush(); data.flush()
    return indptr, indices, data

def create_csr(MAX_WEBSITES):
    E, outdeg = first_pass_count_edges(MAX_WEBSITES)
    print("Total edges E =", E)
    indptr, indices, data = build_memmaps(MAX_WEBSITES, E, outdeg)
    print("Built memmaps:", INDPTR_FILE, INDICES_FILE, DATA_FILE)
