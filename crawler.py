import random
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from queue import Queue

MAX_PAGES = 100
MAX_WORKERS = 10

def crawl_page(url, visited, visited_lock, to_visit, graph, graph_lock):
    """Crawl a single page and update shared data structures."""
    with visited_lock:
        if url in visited:
            return
        visited.add(url)

    try:
        response = requests.get(url, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        links = [joined_url for a in soup.find_all('a', href=True) 
                 if (joined_url := urljoin(url, a['href'])).startswith('http')]
        
        with graph_lock:
            graph[url] = links
        
        with visited_lock:
            new_links = [l for l in links if l not in visited]
            for link in new_links:
                to_visit.put(link)
    except Exception as e:
        print(f"Error crawling {url}: {e}")

# We can have seed_urls be a list of urls given by the user or we can precompute it
def crawl(seed_urls, max_workers=MAX_WORKERS):
    visited = set()
    visited_lock = Lock()
    to_visit = Queue()
    graph = {}
    graph_lock = Lock()
    
    # Initialize queue with seed URLs
    for url in seed_urls:
        to_visit.put(url)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = set()
        
        while len(visited) < MAX_PAGES:
            # Submit new tasks while we have URLs and haven't reached max pages
            while len(visited) < MAX_PAGES and len(futures) < max_workers * 2:
                try:
                    url = to_visit.get_nowait()
                    future = executor.submit(crawl_page, url, visited, visited_lock, to_visit, graph, graph_lock)
                    futures.add(future)
                except:
                    break
            
            # If no futures and no URLs, we're done
            if not futures:
                if to_visit.empty():
                    break
                continue
            
            # Process completed futures (non-blocking check first)
            completed = [f for f in futures if f.done()]
            for future in completed:
                futures.discard(future)
            
            # If we have pending futures, wait for at least one to complete
            if futures and not completed:
                try:
                    future = next(as_completed(futures, timeout=1))
                    futures.discard(future)
                except:
                    pass
    
    return graph

if __name__ == '__main__':
    seed_urls = ['https://www.google.com', 'https://wikipedia.org', 'https://youtube.com', 'https://amazon.com', 'https://github.com', 'https://stackoverflow.com', 'https://reddit.com', 'https://pinterest.com', 'https://tumblr.com', 'https://yahoo.com', 'https://bing.com', 'https://ask.com', 'https://aol.com', 'https://live.com', 'https://msn.com', 'https://outlook.com', 'https://hotmail.com', 'https://live.com', 'https://msn.com', 'https://outlook.com', 'https://hotmail.com']
    graph = crawl(seed_urls)
    print(graph)