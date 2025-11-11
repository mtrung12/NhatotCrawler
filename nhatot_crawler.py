import duckdb
from WebCrawler import WebCrawler
import requests
import json
import time
import yaml

class NhatotCrawler:
    def __init__(self, config):
        self.config = config
        self.crawler = WebCrawler(config['base_url'], config['user_agent'])
        self.db_file = config['db_file']
        self.gateway_base = config['gateway_base']
        self.column_mapping = config.get('column_mapping', {})  # Read mapping
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': config['user_agent']})
        self._init_db()

    def _init_db(self):
        con = duckdb.connect(self.db_file)
        columns_def = []
        for key_path, vn_name in self.column_mapping.items():
            if vn_name == "ID bài đăng":  
                continue
            col_type = self._get_column_type(key_path)
            columns_def.append(f'"{vn_name}" {col_type}')
        
        create_sql = f"""
            CREATE TABLE IF NOT EXISTS ads (
                "ID bài đăng" INTEGER PRIMARY KEY,
                {', '.join(columns_def) if columns_def else ''}
            )
        """
        con.execute(create_sql)
        con.close()

    def _get_column_type(self, key_path):
        if key_path == 'ad.list_id':
            return 'INTEGER'
        if any(x in key_path for x in ['rooms', 'toilets']):
            return 'INTEGER'
        if 'is_main_street' in key_path:
            return 'BOOLEAN'
        return 'VARCHAR'

    def _extract_value(self, data, key_path):
        if key_path.startswith('special:'):
            special_key = key_path.split(':', 1)[1]
            if special_key == 'latitude_longitude':
                lat = self._extract_value(data, 'ad.latitude')
                lon = self._extract_value(data, 'ad.longitude')
                return f"{lat},{lon}" if lat and lon else None
            return None
        
        keys = key_path.split('.')
        value = data
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
        return value

    def fetch_ad_json(self, ad_id):
        url = f"{self.gateway_base}{ad_id}"
        try:
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error fetching ad {ad_id}: {e}")
            return None

    def save_to_db(self, ad_id, data):
        extracted = {}
        # ID bài đăng luôn từ list_id hoặc ad_id
        extracted["ID bài đăng"] = data.get('ad', {}).get('list_id', ad_id)
        
        # Extract từ mapping (bao gồm ID để consistent, nhưng DB sẽ handle)
        for key_path, vn_name in self.column_mapping.items():
            value = self._extract_value(data, key_path)
            extracted[vn_name] = value
        
        # Lưu vào DB
        con = duckdb.connect(self.db_file)
        columns = ', '.join(f'"{k}"' for k in extracted.keys())
        placeholders = ', '.join('?' for _ in extracted)
        values = tuple(extracted.values())
        update_set = ', '.join(f'"{k}" = EXCLUDED."{k}"' for k in extracted.keys() if k != "ID bài đăng")
        con.execute(f"INSERT OR REPLACE INTO ads ({columns}) VALUES ({placeholders})", values)
        con.close()

    def scrape_and_save(self):
        start_page = self.config.get('start_page', 1)
        ad_ids = self.crawler.crawl_listings(self.config['max_pages'], start_page)
        print(f"Found {len(ad_ids)} unique IDs to scrape")
        for i, ad_id in enumerate(ad_ids, 1):
            data = self.fetch_ad_json(ad_id)
            if data:
                self.save_to_db(ad_id, data)
                print(f"Saved ad {ad_id} ({i}/{len(ad_ids)})")
                time.sleep(2)
            else:
                print(f"Skipped ad {ad_id} (no data)")