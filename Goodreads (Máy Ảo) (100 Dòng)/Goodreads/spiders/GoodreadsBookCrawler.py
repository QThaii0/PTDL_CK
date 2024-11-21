import scrapy
from Goodreads.items import GoodreadsCrawlerItem

class GoodreadsSpider(scrapy.Spider):
    name = "goodreads_spider"
    allowed_domains = ["goodreads.com"]
    start_urls = ['https://www.goodreads.com/list/show/6.Best_Books_of_the_20th_Century']
    page_count = 0  # Biến đếm số trang đã cào

    def parse(self, response):
        # Lấy danh sách các liên kết sách
        book_links = response.xpath('//*[@id="all_votes"]//td[2]//a/@href').getall()
        for link in book_links:
            yield scrapy.Request(url=response.urljoin(link), callback=self.parse_book_details)
        
    def parse_book_details(self, response):
        item = GoodreadsCrawlerItem()
        
        # Lấy thông tin chi tiết về sách
        item['title'] = response.xpath('normalize-space(string(//*[@id="__next"]/div[2]/main/div[1]/div[2]/div[2]/div[1]/div[1]))').get()
        item['author'] = response.xpath('normalize-space(string(//*[@id="__next"]/div[2]/main/div[1]/div[2]/div[2]/div[2]/div[1]))').get()
        item['rating'] = response.xpath('normalize-space(string(//*[@id="__next"]/div[2]/main/div[1]/div[2]/div[2]/div[2]/div[2]/a/div[1]))').get()
        item['rating_count'] = response.xpath('normalize-space(string(//*[@id="__next"]/div[2]/main/div[1]/div[2]/div[2]/div[2]/div[2]/a/div[2]/div/span[1]))').get()
        item['review_count'] = response.xpath('normalize-space(string(//*[@id="__next"]/div[2]/main/div[1]/div[2]/div[2]/div[2]/div[2]/a/div[2]/div/span[2]))').get()
        item['num_pages'] = response.xpath('normalize-space(string(//*[@id="__next"]/div[2]/main/div[1]/div[2]/div[2]/div[2]/div[6]/div/span[1]/span/div/p[1]))').get()
        item['published_date'] = response.xpath('normalize-space(string(//*[@id="__next"]/div[2]/main/div[1]/div[2]/div[2]/div[2]/div[6]/div/span[1]/span/div/p[2]))').get()
        item['quotes'] = response.xpath('normalize-space(//*[@id="__next"]/div[2]/main/div[3]/div/div/a[1]/div[2]/div[1]/text())').get()
        item['discussions'] = response.xpath('normalize-space(//*[@id="__next"]/div[2]/main/div[3]/div/div/a[2]/div[2]/div[1]/text())').get()
        item['questions'] = response.xpath('normalize-space(//*[@id="__next"]/div[2]/main/div[3]/div/div/a[3]/div[2]/div[1]/text())').get()
        item['description'] = response.xpath('normalize-space(string(//*[@id="__next"]/div[2]/main/div[1]/div[2]/div[2]/div[2]/div[4]/div/div[1]))').get()
        item['about_the_author'] = response.xpath('normalize-space(string(//*[@id="__next"]/div[2]/main/div[1]/div[2]/div[2]/div[2]/div[8]/div[3]/div[1]))').get()
        
        yield item
