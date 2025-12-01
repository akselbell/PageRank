from logic.crawl import crawl
from logic.create_csr import create_csr, LMDB_PATH
import numpy as np
from logic.iteration import power_iteration, load_id_to_url_mapping, print_top_urls, load_csr

MAX_WEBSITES = 500000
TOP_K_URLS = 20

if __name__ == "__main__":
    print("Starting crawl... this may take a while...")
    
    # Websites from the top 1000000 websites by alexa
    seed_urls = [
        "https://www.google.com",
        "https://wikipedia.org",
        "https://youtube.com",
        "https://amazon.com",
        "https://github.com",
        "https://stackoverflow.com",
        "https://pinterest.com",
        "https://tumblr.com",
        "https://yahoo.com",
        "https://bing.com",
        "https://ask.com",
        "https://aol.com",
        "https://live.com",
        "https://msn.com",
        "https://outlook.com",
        "https://hotmail.com",
    ]
    crawl(seed_urls, MAX_WEBSITES)

    print("Building CSR...")
    create_csr(MAX_WEBSITES)

    indptr, indices, data, num_nodes = load_csr()
    pagerank = power_iteration(indptr, indices, data, num_nodes)

    print("\nFinal PageRank vector:")
    print(pagerank)

    print("\nTop 20 URLs by PageRank:")
    id_to_url = load_id_to_url_mapping(num_nodes)
    print_top_urls(pagerank, id_to_url, TOP_K_URLS)

