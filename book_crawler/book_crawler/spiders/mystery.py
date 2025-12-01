import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule

class MysterySpider(CrawlSpider):
    name = 'mystery'
    allowed_domains = ['books.toscrape.com']
    start_urls = ['http://books.toscrape.com/catalogue/category/books/mystery_3/index.html']
    
    rules = (
        Rule(LinkExtractor(restrict_css='li.next a'), callback=None, follow=True),
        Rule(LinkExtractor(restrict_css='article.product_pod h3 a'), callback='parse_book', follow=False)
    )
    def parse_book(self, response):
        yield {
            'book_url': response.url,
            'title': response.css('h1::text').get()
        }
