import scrapy
from collections import defaultdict
import os
from dotenv import load_dotenv
import json
import urllib.parse
import re

load_dotenv()

class KeywordTagSpider(scrapy.Spider):
    name = 'keyword_tags'
    custom_settings = {
        'DOWNLOAD_TIMEOUT': 15,
        'RETRY_TIMES': 2,
        'CONCURRENT_REQUESTS': 8,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 2,
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 0.5,
        'LOG_LEVEL': 'INFO',
    }
    
    SERPAPI_KEY = os.getenv('SERPAPI_KEY')
    KEYWORD = "market analysis solar panel"
    
    def __init__(self):
        super().__init__()
        self.site_positions = {}
        self.site_css_paths = defaultdict(set)
        self.site_contents = defaultdict(list)
        self.all_sites = {}
        self.serp_page = 0
        self.total_sites = 0

    def get_css_path(self, elem):
        """🔥 FIXED: Proper CSS path extraction"""
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
                if selector != tag:  # Only add if has class/id
                    path.append(selector)
                else:
                    path.append(tag)
                    
                current = current.getparent()
            return " > ".join(reversed(path))[:120] or "body"
        except:
            return 'body > div'

    def clean_text(self, text):
        """Clean HTML artifacts"""
        if not text:
            return ''
        text = re.sub(r'&[a-zA-Z0-9#]+;', ' ', text)
        text = re.sub(r'[\x00-\x1F\x7F-\x9F\u00A0]', ' ', text)
        text = re.sub(r'\s+', ' ', text.strip())
        return text

    def is_good_content(self, text):
        """Relaxed filter - MORE content accepted"""
        text = text.strip()
        if len(text) < 15:  # Reduced from 20
            return False
        
        bad_patterns = [
            r'\{@"?context"@?\}', r'function\s+\w', r'document\.getElementById',
            r'cookie|privacy|term', r'^\s*©|\xa9', r'^\s*\$[\d,]+'
        ]
        
        for pattern in bad_patterns:
            if re.search(pattern, text, re.I):
                return False
        
        words = re.findall(r'\b[a-zA-Z]{3,}\b', text)
        return len(words) >= 2

    def get_content_block(self, elem):
        """🔥 ENHANCED: More content + better fallbacks"""
        candidates = []
        
        try:
            # 1. Main content containers
            main_selectors = [
                'ancestor::article[1]',
                'ancestor::main[1]',
                'ancestor::div[contains(@class,"content") or contains(@class,"article") or contains(@class,"post") or contains(@class,"body")][1]',
                'ancestor::section[1]',
                'ancestor::div[not(ancestor::nav) and not(ancestor::footer)][1]'
            ]
            
            for selector in main_selectors:
                block = elem.xpath(selector)
                if block:
                    text = block[0].xpath('string()').get(default='')
                    cleaned = self.clean_text(text)
                    if self.is_good_content(cleaned):
                        candidates.append(cleaned)
            
            # 2. Paragraph-level fallback
            para_selectors = [
                'ancestor-or-self::p[1]',
                'ancestor-or-self::h1|ancestor-or-self::h2|ancestor-or-self::h3[1]',
                'ancestor-or-self::div[not(ancestor::nav)][1]',
                'ancestor-or-self::span[1]'
            ]
            
            for selector in para_selectors:
                block = elem.xpath(selector)
                if block:
                    text = block[0].xpath('string()').get(default='')
                    cleaned = self.clean_text(text)
                    if self.is_good_content(cleaned):
                        candidates.append(cleaned)
            
            # Return best candidate (longest meaningful)
            candidates = [c for c in candidates if len(c) > 20]
            return max(candidates, key=len) if candidates else ''
            
        except:
            return ''

    def start_requests(self):
        if not self.SERPAPI_KEY:
            self.logger.error("❌ SERPAPI_KEY missing in .env!")
            return
            
        yield scrapy.Request(
            url=self.build_serp_url(0),
            callback=self.parse_serp_page,
            meta={'serp_page': 0},
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        )

    def build_serp_url(self, page):
        params = {
            'engine': 'google',
            'q': self.KEYWORD,
            'api_key': self.SERPAPI_KEY,
            'num': 20,  # MORE results
            'start': page * 20
        }
        return f"https://serpapi.com/search?{urllib.parse.urlencode(params)}"

    def parse_serp_page(self, response):
        data = response.json()
        organic_results = data.get('organic_results', [])
        self.logger.info(f"SERP Page {self.serp_page + 1}: {len(organic_results)} results")
        
        page_start_pos = self.serp_page * 20 + 1
        for i, result in enumerate(organic_results):
            if self.total_sites >= 25:  # MORE sites
                break
                
            url = result.get('link')
            if url and url not in self.site_positions:
                position = page_start_pos + i
                self.site_positions[url] = position
                self.all_sites[url] = {'position': position, 'status': None}
                self.total_sites += 1
                
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_tags,
                    meta={'source_url': url, 'position': position},
                    headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'},
                    errback=self.handle_failed_request
                )
        
        if self.serp_page < 2 and self.total_sites < 25:  # MORE pages
            self.serp_page += 1
            yield scrapy.Request(
                url=self.build_serp_url(self.serp_page),
                callback=self.parse_serp_page,
                meta={'serp_page': self.serp_page}
            )

    def handle_failed_request(self, failure):
        source_url = failure.request.meta.get('source_url')
        self.all_sites[source_url] = {'position': failure.request.meta.get('position'), 'status': 0}

    def parse_tags(self, response):
        source_url = response.meta['source_url']
        position = response.meta['position']
        self.all_sites[source_url]['status'] = response.status
        
        if response.status >= 400:
            return

        self.logger.info(f"🔍 Site {position}: {source_url[:60]}")
        keywords = self.KEYWORD.lower().split()
        new_contents = 0
        
        for word in keywords:
            xpath_query = f'//*[contains(translate(text(), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "{word}")]'
            for elem in response.xpath(xpath_query)[:20]:  # MORE elements per keyword
                css_path = self.get_css_path(elem)
                content = self.get_content_block(elem)
                
                if content and content not in self.site_contents[source_url]:
                    self.site_contents[source_url].append(content)
                    self.site_css_paths[source_url].add(css_path)
                    new_contents += 1
                    
                    self.logger.info(f"✅ [{new_contents}] {css_path[:60]} | {len(content)} chars")
        
        self.logger.info(f"✅ Site {position}: {new_contents} contents")

    def closed(self, reason):
        # tags.json - ALL CSS paths
        tags_results = []
        for url, data in self.all_sites.items():
            tags_results.append({
                "src_url": url,
                "position": data['position'],
                "keyword": self.KEYWORD,
                "css_paths": sorted(list(self.site_css_paths[url])),
                "status_code": data['status']
            })
        with open('tags.json', 'w', encoding='utf-8') as f:
            json.dump(tags_results, f, indent=2, ensure_ascii=False)
        
        # content.json - ALL content with PROPER CSS paths
        content_results = []
        for url, data in self.all_sites.items():
            css_paths = list(self.site_css_paths[url])
            for i, content in enumerate(self.site_contents[url], 1):
                css_path = css_paths[min(i-1, len(css_paths)-1)] if css_paths else "body"
                content_results.append({
                    "src_url": url,
                    "position": data['position'],
                    "content_id": i,
                    "css_path": css_path,  # 🔥 FIXED: Real CSS paths!
                    "content": content[:500]  # Truncate for readability
                })
        
        with open('content.json', 'w', encoding='utf-8') as f:
            json.dump(content_results, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"✅ tags.json: {len(tags_results)} sites")
        self.logger.info(f"✅ content.json: {len(content_results)} contents")  # 🔥 MUCH MORE!
