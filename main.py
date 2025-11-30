from logic.crawl import crawl
from logic.create_csr import create_csr

if __name__ == "__main__":
    print("Starting crawl... this may take a while...")
    seed_urls = ['https://www.bethesdascholars.com', 'https://www.google.com', 'https://wikipedia.org', 'https://youtube.com', 'https://amazon.com', 'https://github.com', 'https://stackoverflow.com', 'https://pinterest.com', 'https://tumblr.com', 'https://yahoo.com', 'https://bing.com', 'https://ask.com', 'https://aol.com', 'https://live.com', 'https://msn.com', 'https://outlook.com', 'https://hotmail.com', 'https://live.com', 'https://msn.com', 'https://outlook.com', 'https://hotmail.com']
    crawl(seed_urls)
