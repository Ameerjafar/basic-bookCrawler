import scrapy
from collections import defaultdict
import os
from dotenv import load_dotenv
import urllib.parse
import re


load_dotenv()


class KeywordTagSpider(scrapy.Spider):
    name = 'keyword_tags_cse'
    custom_settings = {
        'DOWNLOAD_TIMEOUT': 15,
        'RETRY_TIMES': 2,
        'CONCURRENT_REQUESTS': 8,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 2,
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 0.5,
        'LOG_LEVEL': 'INFO',
        'FEED_EXPORT_ENCODING': 'utf-8',
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.INDUSTRY_MODULE = 'solar_energy'
        self.KEYWORD = 'solar panel market analysis'
        self.GOOGLE_CSE_ID = os.getenv('GOOGLE_CSE_ID')
        self.GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')

        print(f"🔍 CSE_ID: {'✅' if self.GOOGLE_CSE_ID else '❌ MISSING'}")
        print(f"🔍 API_KEY: {'✅' if self.GOOGLE_API_KEY else '❌ MISSING'}")

        self.site_css_paths = defaultdict(set)
        self.site_contents = defaultdict(list)
        self.site_positions = {}
        self.all_sites = {}
        self.total_sites = 0

    def get_css_path(self, elem):
        """Generate COMPLETE CSS path from body to EXACT target element"""
        try:
            path = []
            current = elem.root  
            while current is not None:
                tag = current.tag or 'div'
                classes = current.get('class', '')
                ids = current.get('id', '')
                class_parts = []
                for c in classes.split():
                    if c.strip():
                        class_parts.append(c.strip())
                class_str = f".{'.'.join(class_parts)}" if class_parts else ""
                id_str = f"#{ids}" if ids else ""

                selector = f"{tag}{id_str}{class_str}"

                path.append(selector)
                current = current.getparent()

                
                if current is None or current.tag in ['html', 'body'] or len(path) > 50:
                    break
            full_path = " > ".join(reversed(path))
            if not full_path.startswith(('body', 'html')):
                full_path = f"body > {full_path}"

            return full_path[:400]

        except Exception as e:
            self.logger.error(f"CSS path error: {e}")
            return 'body > div'

    def start_requests(self):
        print("FORCING EXECUTION - NO DB CHECK!")
        print("fetching from the google api")

        yield scrapy.Request(
            url=self.build_cse_url(0),
            callback=self.parse_cse_page,
        )   
    def build_cse_url(self, page):
        params = {
            'key': self.GOOGLE_API_KEY,
            'cx': self.GOOGLE_CSE_ID,
            'q': self.KEYWORD,
            'num': 10,
            'start': page * 10 + 1,
            'gl': 'us',
            'lr': 'lang_en'
        }
        return f"https://www.googleapis.com/customsearch/v1?{urllib.parse.urlencode(params)}"

    def parse_cse_page(self, response):
        print("📡 Google CSE response received!")
        data = response.json()
        items = data.get('items', [])
        print(f"🔍 Found {len(items)} URLs from Google")

        for _, result in enumerate(items):
            if self.total_sites >= 10:
                break
            url = result.get('link')
            if url:
                position = self.total_sites + 1
                self.site_positions[url] = position
                self.all_sites[url] = {'position': position, 'status': None}
                self.total_sites += 1
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_tags,
                    meta={'source_url': url, 'position': position}
                )
            

    def parse_tags(self, response):
        source_url = response.meta['source_url']
        position = response.meta['position']
        self.all_sites[source_url]['status'] = response.status

        print(f"Site #{position} ({source_url}): Status {response.status}")

        if response.status >= 400:
            self.logger.warning(f"⚠️ Site #{position} failed: {response.status}")
            return

        keywords = self.KEYWORD.lower().split()

        for word in keywords:
            xpath_query = (
                f'//*[contains(translate(text(), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", '
                f'"abcdefghijklmnopqrstuvwxyz"), "{word}")]'
            )
            for elem in response.xpath(xpath_query):
                css_path = self.get_css_path(elem)
                yield {
                    'url': source_url,
                    'position': position, 
                    'css_path': css_path,
                }

                self.site_css_paths[source_url].add(css_path)

    def closed(self, reason):
        print(f"🎉 FINISHED: {self.total_sites} sites processed!")
        print("💾 Check your output JSON for url, position, css_path!")
