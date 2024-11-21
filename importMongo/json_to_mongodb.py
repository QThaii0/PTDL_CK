import pandas as pd
from pymongo import MongoClient

# Đọc file CSV
file_path = 'jsondata_goodreads.json'
df = pd.read_json(file_path,lines=True)

# Kết nối tới MongoDB
client = MongoClient('mongodb://mymongodb:27017/') #kết nối tới MongoDB
db = client['GoodreadsBook_Crawler_Full'] #tạo database
collection = db['GoodBook'] #tạo collection

#Chuyển đổi DataFrame thành danh sách các từ điển
data = df.to_dict(orient='records')

#Chèn dữ liệu vào MongoDB
collection.insert_many(data) 
print(f"Đã chèn {len(data)} bản ghi vào MongoDB.")