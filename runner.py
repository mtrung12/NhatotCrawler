import yaml
from nhatot_crawler import NhatotCrawler
from export_to_csv import export_to_csv
import os
from logger import log

def main():
    with open('config.yaml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    
    crawler = NhatotCrawler(config)
    crawler.scrape_and_save()
    
    export_to_csv(config['db_file'], 'ads', 'nhatot_export.csv')
    log.info("Completed! Checking 'nhatot_export.csv'")

if __name__ == "__main__":
    main()