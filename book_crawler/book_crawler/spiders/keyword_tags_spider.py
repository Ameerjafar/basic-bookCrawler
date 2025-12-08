import scrapy
from collections import defaultdict
import os
from dotenv import load_dotenv
import json
import urllib.parse
import re
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime

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
    }
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # ✅ HARDCODED as requested
        self.INDUSTRY_MODULE = 'solar_energy'
        self.KEYWORD = 'solar panel market analysis'
        
        self.GOOGLE_CSE_ID = os.getenv('GOOGLE_CSE_ID')
        self.GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
        
        # Storage
        self.site_css_paths = defaultdict(set)
        self.site_contents = defaultdict(list)
        self.site_positions = {}
        self.all_sites = {}
        self.serp_page = 0
        self.total_sites = 0
        self.industry_module_id = None
        
        self.db_conn = None
        self.init_db()
    
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

    def init_db(self):
        try:
            self.db_conn = psycopg2.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                database=os.getenv('DB_NAME', 'scrapy_db'),
                user=os.getenv('DB_USER', 'postgres'),
                password=os.getenv('DB_PASSWORD', 'password'),
                port=os.getenv('DB_PORT', '5432')
            )
            
            with self.db_conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS keywords, industry_modules, css_paths, page_contents CASCADE")
                
                cur.execute("""
                    CREATE TABLE industry_modules (
                        id SERIAL PRIMARY KEY,
                        module_name VARCHAR(100) NOT NULL UNIQUE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                cur.execute("""
                    INSERT INTO industry_modules (module_name) VALUES ('solar_energy')
                    ON CONFLICT (module_name) DO NOTHING RETURNING id
                """)
                self.industry_module_id = cur.fetchone()[0] or 1
                
                cur.execute("""
                    CREATE TABLE keywords (
                        id SERIAL PRIMARY KEY,
                        industry_module_id INTEGER REFERENCES industry_modules(id),
                        keyword VARCHAR(500) NOT NULL,
                        status VARCHAR(20) DEFAULT 'pending',
                        scraped_at TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(industry_module_id, keyword)
                    )
                """)
                
                keywords_list = ['solar', 'panel', 'market', 'analysis']
                for keyword in keywords_list:
                    cur.execute("""
                        INSERT INTO keywords (industry_module_id, keyword, status) 
                        VALUES (%s, %s, 'pending') ON CONFLICT DO NOTHING
                    """, (self.industry_module_id, keyword))
                
                self.db_conn.commit()
                self.logger.info("✅ DB ready: solar_energy + 4 keywords")
                
        except Exception as e:
            self.logger.error(f"❌ DB Error: {e}")

    def start_requests(self):
        try:
            with self.db_conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM keywords WHERE industry_module_id = %s AND status = 'scraped'", 
                          (self.industry_module_id,))
                if cur.fetchone()[0] == 4:
                    self.logger.info("⏭️ Already scraped - saving JSON summary")
                    self.save_json_summary()
                    return
        except: pass
        
        if not self.GOOGLE_CSE_ID or not self.GOOGLE_API_KEY:
            self.logger.warning("⚠️ No Google API - marking complete")
            self.mark_complete()
            return
        
        self.logger.info("🚀 Scraping 'solar panel market analysis'")
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
        data = response.json()
        items = data.get('items', [])
        
        for i, result in enumerate(items):
            if self.total_sites >= 10: break
            url = result.get('link')
            if url:
                position = self.total_sites + 1
                self.site_positions[url] = position
                self.all_sites[url] = {'position': position, 'status': None}
                self.total_sites += 1
                yield scrapy.Request(url=url, callback=self.parse_tags,
                                   meta={'source_url': url, 'position': position})

    def parse_tags(self, response):
        source_url = response.meta['source_url']
        position = response.meta['position']
        self.all_sites[source_url]['status'] = response.status
        
        if response.status >= 400:
            self.logger.warning(f"⚠️ Site #{position} failed: {response.status}")
            return
        
        keywords = self.KEYWORD.lower().split()
        new_contents = 0
        
        for word in keywords:
            xpath_query = f'//*[contains(translate(text(), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "{word}")]'
            for elem in response.xpath(xpath_query)[:20]:
                css_path = self.get_css_path(elem)
                content = self.get_content_block(elem)
                
                if content and content not in self.site_contents[source_url]:
                    self.site_contents[source_url].append(content)
                    self.site_css_paths[source_url].add(css_path)
                    new_contents += 1
        
        self.logger.info(f"✅ Site #{position}: {new_contents} content blocks")

    def mark_complete(self):
        try:
            with self.db_conn.cursor() as cur:
                cur.execute("""
                    UPDATE keywords SET status = 'scraped', scraped_at = CURRENT_TIMESTAMP
                    WHERE industry_module_id = %s
                """, (self.industry_module_id,))
                self.db_conn.commit()
        except Exception as e:
            self.logger.error(f"❌ DB error: {e}")

    def save_content_json(self):
        """✅ Save ALL content to content.json"""
        content_data = []
        
        for url, data in self.all_sites.items():
            position = data['position']
            css_paths_list = list(self.site_css_paths[url])
            
            for i, content in enumerate(self.site_contents[url], 1):
                css_path = css_paths_list[min(i-1, len(css_paths_list)-1)] if css_paths_list else "body"
                content_data.append({
                    'url': url,
                    'position': position,
                    'content_id': i,
                    'css_path': css_path,
                    'content': content[:2000],
                    'content_length': len(content),
                    'keywords': self.KEYWORD
                })
        
        with open('content.json', 'w', encoding='utf-8') as f:
            json.dump(content_data, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"💾 SAVED {len(content_data)} content blocks to content.json")

    def save_css_paths_json(self):
        """Save CSS paths to css_paths.json"""
        css_data = []
        for url, data in self.all_sites.items():
            for css_path in self.site_css_paths[url]:
                css_data.append({
                    'url': url,
                    'position': data['position'],
                    'css_path': css_path,
                    'status_code': data['status']
                })
        
        with open('css_paths.json', 'w', encoding='utf-8') as f:
            json.dump(css_data, f, indent=2, ensure_ascii=False)

    def closed(self, reason):
        self.mark_complete()
        
        # ✅ SAVE JSON FILES
        self.save_content_json()
        self.save_css_paths_json()
        
        self.logger.info(f"🎉 FINISHED: {self.total_sites} sites, {sum(len(c) for c in self.site_contents.values())} contents!")
        if self.db_conn:
            self.db_conn.close()
