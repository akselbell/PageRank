from threading import Lock
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeoutError
import lmdb
import requests, certifi
from bs4 import BeautifulSoup
from logic.bin_write_queue import BinWriteQueue
import os
import shutil
from urllib.parse import urljoin, urlparse

MAX_WORKERS = 10
_next_id = 0
_next_id_lock = Lock()

def reserve_id(MAX_WEBSITES):
    global _next_id
    with _next_id_lock:
        if _next_id >= MAX_WEBSITES:
            return None
        id_ = _next_id
        _next_id += 1
        return id_

def crawl(seed_urls, MAX_WEBSITES):
    # clear the database if it exists
    if os.path.exists("crawler.lmdb"):
        shutil.rmtree("crawler.lmdb")
    
    to_visit_lock = Lock()
    to_visit = Queue()
    write_queue = BinWriteQueue()
    
    # Start the writer thread
    write_queue.start()

    for url in seed_urls:
        to_visit.put(url)

    env = lmdb.open("crawler.lmdb", map_size=1024 * 1024 * 1024, max_dbs=1)

    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        futures = set()

        while True:
            with _next_id_lock:
                if _next_id >= MAX_WEBSITES:
                    break
            
            while len(futures) < MAX_WORKERS * 2:
                with _next_id_lock:
                    if _next_id >= MAX_WEBSITES:
                        break
                try:   
                    url = to_visit.get_nowait()
                    future = executor.submit(crawl_page, url, to_visit, to_visit_lock, env, write_queue, MAX_WEBSITES)
                    futures.add(future)
                except Empty:
                    break

            if not futures:
                if to_visit.empty():
                    break
                continue

            completed = [f for f in futures if f.done()]
            for future in completed:
                futures.discard(future)
            
            if futures and not completed:
                try:
                    future = next(as_completed(futures, timeout=1))
                    futures.discard(future)
                except (StopIteration, FutureTimeoutError):
                    pass
        
        for future in futures:
            future.result()
    
    # Wait for all write operations to complete and shutdown
    write_queue.shutdown()
    print("Total IDs reserved: ", _next_id, ". Total pages crawled: ", env.stat()['entries'])
    env.close()

def normalize_url(url):
    """Normalize URL by removing fragments and trailing slashes."""
    parsed = urlparse(url)
    normalized = parsed._replace(fragment='').geturl()
    if normalized.endswith('/') and len(normalized) > 1:
        normalized = normalized.rstrip('/')
    return normalized

def crawl_page(url, to_visit, to_visit_lock, env, write_queue, MAX_WEBSITES):
    try:
        url = normalize_url(url)
        url_b = url.encode('utf-8')
        
        id_ = reserve_id(MAX_WEBSITES)
        if id_ is None:
            return

        response = requests.get(url, timeout=10, verify=certifi.where())
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        links = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            if href.startswith('http://') or href.startswith('https://'):
                absolute_url = href
            elif href.startswith('//'):
                absolute_url = 'https:' + href
            elif href.startswith('/'):
                absolute_url = urljoin(url, href)
            else:
                absolute_url = urljoin(url, href)
            
            normalized_link = normalize_url(absolute_url)
            if normalized_link.startswith('http://') or normalized_link.startswith('https://'):
                links.append(normalized_link)

        id_b = str(id_).encode('ascii')

        with env.begin(write=True) as txn:
            if txn.get(url_b) is None:
                txn.put(url_b, id_b)

        dest_ids = []
        for link in links:
            link_b = link.encode('utf-8')
            if len(link_b) > 500:
                continue

            with env.begin() as rtxn:
                exists = rtxn.get(link_b)
            
            if exists is None:
                link_id = reserve_id(MAX_WEBSITES)
                if link_id is None:
                    break
                link_id_b = str(link_id).encode('ascii')
                with env.begin(write=True) as wtxn:
                    # double-check it wasn't inserted by another thread since we checked exists above
                    if wtxn.get(link_b) is None:
                        wtxn.put(link_b, link_id_b)
                        with to_visit_lock:
                            to_visit.put(link)
                        dest_ids.append(link_id)
                    else:
                        try:
                            dest_ids.append(int(wtxn.get(link_b).decode('ascii')))
                        except (ValueError, AttributeError):
                            pass
            else:
                try:
                    existing_id = int(exists.decode('ascii'))
                    dest_ids.append(existing_id)
                except (ValueError, AttributeError):
                    pass
        write_queue.send(id_, dest_ids)
    except requests.exceptions.RequestException as e:
        print(f"Request error crawling {url}: {e}")
        return
    except Exception as e:
        print(f"Error crawling {url}: {e}")
        return