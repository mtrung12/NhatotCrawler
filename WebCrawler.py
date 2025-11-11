# WebCrawler.py (Cập nhật để dùng Selenium)
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup

class WebCrawler:
    def __init__(self, base_url, user_agent):
        self.base_url = base_url
        self.user_agent = user_agent

    def fetch_page(self, url):
        try:
            options = Options()
            options.headless = True  # Chạy ngầm, không mở cửa sổ trình duyệt
            options.add_argument(f"user-agent={self.user_agent}")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")

            # Nếu chromedriver không trong PATH, chỉ định path (ví dụ: '/path/to/chromedriver')
            # service = Service('/path/to/chromedriver')
            # driver = webdriver.Chrome(service=service, options=options)
            driver = webdriver.Chrome(options=options)  # Nếu đã add PATH

            driver.get(url)
            html = driver.page_source
            driver.quit()
            return html
        except Exception as e:
            print(f"Error fetching {url} with Selenium: {e}")
            return None

    def parse_listing_page(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        ad_links = soup.find_all('a', href=True)
        ids = []
        for link in ad_links:
            href = link['href']
            if href.startswith('/mua-ban-') and href.endswith('.htm'):
                # Extract ID from /mua-ban-...-123456789.htm
                parts = href.split('/')[-1].split('.htm')[0].split('-')[-1]
                if parts.isdigit():
                    ids.append(int(parts))
        return ids

    def crawl_listings(self, max_pages=1, start_page=1):
        all_ids = []
        for page in range(start_page, max_pages + 1):
            url = f"{self.base_url}?page={page}"
            html = self.fetch_page(url)
            if html:
                ids = self.parse_listing_page(html)
                all_ids.extend(ids)
                print(f"Page {page}: Found {len(ids)} IDs")
            else:
                break
        return list(set(all_ids))  # Remove duplicates if any