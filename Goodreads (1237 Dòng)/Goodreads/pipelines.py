# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import scrapy
import pymongo
import json
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
import csv
import os

class MongoDBGoodreadsPipeline:
    def __init__(self):
        # Connection String
        econnect = str(os.environ.get('Mongo_HOST', 'localhost'))
        self.client = pymongo.MongoClient(f'mongodb://{econnect}:27017')
        self.db = self.client['goodreads_db']  # Create Database

    def process_item(self, item, spider):
        collection = self.db['books']  # Create Collection or Table
        try:
            collection.insert_one(dict(item))
            return item
        except Exception as e:
            raise DropItem(f"Error inserting item: {e}")

class JsonDBGoodreadsPipeline:
    def process_item(self, item, spider):
        with open('jsondata_goodreads.json', 'a', encoding='utf-8') as file:
            line = json.dumps(dict(item), ensure_ascii=False) + '\n'
            file.write(line)
        return item

class CSVDBGoodreadsPipeline:
    '''
    Mỗi thông tin cách nhau với dấu $
    Ví dụ: title$author$rating$rating_count$review_count$description$num_pages$published_date$genres$isbn
    Sau đó, cài đặt cấu hình để ưu tiên Pipeline này đầu tiên
    '''
    def process_item(self, item, spider):
        with open('csvdata_goodreads.csv', 'a', encoding='utf-8', newline='') as file:
            writer = csv.writer(file, delimiter='$')
            writer.writerow([
                item.get('title', ''),
                item.get('author', ''),
                item.get('rating', ''),
                item.get('rating_count', ''),
                item.get('review_count', ''),
                item.get('num_pages', ''),
                item.get('published_date', ''),
                item.get('quotes', ''),
                item.get('discussions', ''),
                item.get('questions', ''),
                item.get('description', ''),
                item.get('about_the_author', ''),
            ])
        return item
