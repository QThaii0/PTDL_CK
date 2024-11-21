# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class GoodreadsCrawlerItem(scrapy.Item):
    title = scrapy.Field()
    author = scrapy.Field()
    rating = scrapy.Field()
    rating_count = scrapy.Field()
    review_count = scrapy.Field()
    num_pages = scrapy.Field()
    published_date = scrapy.Field()
    quotes = scrapy.Field()
    discussions = scrapy.Field()
    questions = scrapy.Field()
    description = scrapy.Field()
    about_the_author = scrapy.Field()
