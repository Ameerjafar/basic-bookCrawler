import requests
import scrapy
from collections import defaultdict
import os
from dotenv import load_dotenv
import time

load_dotenv()

class KeywordTagSpider(scrapy.Spider):
    name = 'keyword_tags'
    
    # ✅ Custom settings for fast crawling
    custom_settings = {
        'DOWNLOAD_TIMEOUT': 10,              # Skip after 10 seconds
        'RETRY_TIMES': 1,                    # Retry only once
        'CONCURRENT_REQUESTS': 16,           # Parallel requests
        'CONCURRENT_REQUESTS_PER_DOMAIN': 2, # Per domain limit
        'ROBOTSTXT_OBEY': True,              # Respect robots.txt
        'DOWNLOAD_DELAY': 0.5,               # Small delay between requests
        'LOG_LEVEL': 'INFO',
    }
    
    SERPAPI_KEY = os.getenv('SERPAPI_KEY')
    KEYWORD = "donald trump"

    def __init__(self):
        super().__init__()
        self.site_positions = {}
        self.site_tags = defaultdict(set)
        self.all_sites = {}

    def start_requests(self):
        """Get up to 200 trusted sites from SerpApi by paginating 20 pages"""
        all_results = []
        try:
            for start in range(0, 200, 10):  # 20 pages: 0,10,20,...,190
                params = {
                    "engine": "google",
                    "q": self.KEYWORD,
                    "api_key": self.SERPAPI_KEY,
                    "num": 10,
                    "start": start
                }
                response = requests.get('https://serpapi.com/search', params=params)
                data = response.json()
                organic_results = data.get('organic_results', [])
                all_results.extend(organic_results)
                self.logger.info(f"Page {start // 10 + 1}: {len(organic_results)} results")
                time.sleep(0.5)  # SerpApi rate limiting

            self.logger.info(f"🎉 Total {len(all_results)} websites collected!")

            # Crawl only first 100 unique URLs
            for position, result in enumerate(all_results[:100], 1):
                url = result.get('link')
                if url and url not in self.site_positions:
                    self.site_positions[url] = position
                    self.all_sites[url] = {'position': position}
                    self.logger.info(f"Site {position}: {url}")
                    yield scrapy.Request(
                        url=url,
                        callback=self.parse_tags,
                        meta={
                            'source_url': url,
                            'handle_httpstatus_list': [408, 500, 502, 503, 504, 429],  # Accept timeouts/errors
                        },
                        headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'},
                        errback=self.handle_failed_request  # Handle timeouts
                    )
        except Exception as e:
            self.logger.error(f"SerpApi error: {e}")

    def handle_failed_request(self, failure):
        """Handle timeout/failed requests gracefully"""
        self.logger.warning(f"⏰ TIMEOUT/SKIPPED: {failure.request.url}")
        # Still count as visited site
        source_url = failure.request.meta.get('source_url', failure.request.url)
        if source_url not in self.all_sites:
            self.all_sites[source_url] = {'position': self.site_positions.get(source_url, 0)}

    def parse_tags(self, response):
        source_url = response.meta.get('source_url', response.url)
        position = self.site_positions.get(source_url, 0)

        # Skip timeout responses
        if response.status == 408:
            self.logger.warning(f"⏰ TIMEOUT: {source_url}")
            return

        keywords = self.KEYWORD.lower().split()

        for word in keywords:
            xpath_query = f'//*[contains(translate(text(), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "{word}")]'
            for elem in response.xpath(xpath_query):
                try:
                    tag_name = elem.root.tag if hasattr(elem.root, 'tag') else 'unknown'
                    self.site_tags[source_url].add(tag_name)
                except Exception:
                    continue

        self.logger.info(f"Site {position} ({source_url}): Found {len(self.site_tags[source_url])} tags")

    def closed(self, reason):
        """Output all sites with collected tags"""
        results = []
        for url in self.all_sites.keys():
            results.append({
                "src_url": url,
                "position": self.site_positions.get(url, 0),
                "keyword": self.KEYWORD,
                "all_tags": list(self.site_tags.get(url, set())),
                "status_code": getattr(self.site_tags[url], 'status', 200) or 200
            })

        import json
        with open('tags.json', 'w') as f:
            json.dump(results, f, indent=2)

        self.logger.info(f"✅ Final output saved: {len(results)} sites")
