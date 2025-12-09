import scrapy
from collections import defaultdict
import os
from dotenv import load_dotenv
import json
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
        'FEED_EXPORT_ENCODING': 'utf-8',  # ✅ Auto JSON export
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
        """Generate CSS path for element"""
        try:
            path = []
            current = elem.root
            while current is not None and len(path) < 8:
                tag = getattr(current, 'tagname', getattr(current, 'tag', 'div'))
                classes = current.get('class', '')
                ids = current.get('id', '')
                class_str = f".{'.'.join([c.strip() for c in classes.split() if c.strip()])}" if classes else ""
                id_str = f"#{ids}" if ids else ""
                selector = f"{tag}{id_str}{class_str}"
                path.append(selector if selector != tag else tag)
                current = current.getparent()
            return " > ".join(reversed(path))[:120] or "body"
        except:
            return 'body > div'

    def clean_text(self, text):
        """Clean HTML text"""
        if not text: return ''
        text = re.sub(r'&[a-zA-Z0-9#]+;', ' ', text)
        text = re.sub(r'[\x00-\x1F\x7F-\x9F\u00A0]', ' ', text)
        return re.sub(r'\s+', ' ', text.strip())

    def is_good_content(self, text):
        """Quality check for content"""
        text = text.strip()
        if len(text) < 15: return False
        bad_patterns = [r'\{@"?context"@?\}', r'function\s+\w', r'document\.getElementById', 
                       r'cookie|privacy|term', r'^\s*©|\xa9', r'^\s*\$[\d,]+']
        if any(re.search(p, text, re.I) for p in bad_patterns): return False
        return len(re.findall(r'\b[a-zA-Z]{3,}\b', text)) >= 2

    def get_content_block(self, elem):
        """Get best content container"""
        candidates = []
        try:
            selectors = [
                'ancestor::article[1]', 'ancestor::main[1]',
                'ancestor::div[contains(@class,"content") or contains(@class,"article") or contains(@class,"post") or contains(@class,"body")][1]',
                'ancestor::section[1]', 'ancestor-or-self::p[1]',
                'ancestor-or-self::h1|ancestor-or-self::h2|ancestor-or-self::h3[1]'
            ]
            for selector in selectors:
                block = elem.xpath(selector)
                if block:
                    text = block[0].xpath('string()').get(default='')
                    cleaned = self.clean_text(text)
                    if self.is_good_content(cleaned):
                        candidates.append(cleaned)
            candidates = [c for c in candidates if len(c) > 20]
            return max(candidates, key=len) if candidates else ''
        except:
            return ''

    def start_requests(self):
        print("🚀 FORCING EXECUTION - NO DB CHECK!")
        
        if not self.GOOGLE_CSE_ID or not self.GOOGLE_API_KEY:
            print("⚠️ No Google API keys - using test URLs")
            test_urls = [
                'https://en.wikipedia.org/wiki/Solar_panel',
                'https://www.statista.com/topics/4830/solar-energy/'
            ]
            for i, url in enumerate(test_urls, 1):
                yield scrapy.Request(url=url, callback=self.parse_tags,
                                   meta={'source_url': url, 'position': i})
            return
        
        print("✅ Google API ready - fetching SERP!")
        yield scrapy.Request(
            url=self.build_cse_url(0),
            callback=self.parse_cse_page,
            meta={'cse_page': 0}
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
        
        for i, result in enumerate(items):
            if self.total_sites >= 10: break
            url = result.get('link')
            if url:
                position = self.total_sites + 1
                self.site_positions[url] = position
                self.all_sites[url] = {'position': position, 'status': None}
                self.total_sites += 1
                print(f"➡️  Queuing site #{position}: {url}")
                yield scrapy.Request(url=url, callback=self.parse_tags,
                                   meta={'source_url': url, 'position': position})

    def parse_tags(self, response):
        source_url = response.meta['source_url']
        position = response.meta['position']
        self.all_sites[source_url]['status'] = response.status
        
        print(f"📄 Site #{position} ({source_url}): Status {response.status}")
        
        if response.status >= 400:
            self.logger.warning(f"⚠️ Site #{position} failed: {response.status}")
            return
        
        keywords = self.KEYWORD.lower().split()
        content_count = 0
        
        for word in keywords:
            xpath_query = f'//*[contains(translate(text(), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "{word}")]'
            for elem in response.xpath(xpath_query)[:20]:
                css_path = self.get_css_path(elem)
                content = self.get_content_block(elem)
                
                if content:
                    content_count += 1
                    # ✅ YIELD ITEM → SAVED TO discovery.json AUTOMATICALLY
                    yield {
                        'url': source_url,
                        'position': position,
                        'content_id': content_count,
                        'css_path': css_path,
                        'content': content[:2000],
                        'content_length': len(content),
                        'keywords': self.KEYWORD
                    }
                    self.site_contents[source_url].append(content)
                    self.site_css_paths[source_url].add(css_path)
        
        print(f"✅ Site #{position}: {content_count} content blocks")

    def closed(self, reason):
        print(f"🎉 FINISHED: {self.total_sites} sites processed!")
        print("💾 Check discovery.json for all exported items!")
