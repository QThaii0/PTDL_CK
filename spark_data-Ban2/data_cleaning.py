from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, explode, split, trim, to_date, collect_list
from pyspark.sql.types import IntegerType, DoubleType

# Khởi tạo Spark session
spark = SparkSession.builder \
    .appName("Goodreadbook") \
    .config("spark.mongodb.input.uri", "mongodb://mymongodb:27017/GoodreadsBook_Crawler_Full.GoodBook") \
    .config("spark.mongodb.output.uri", "mongodb://mymongodb:27017/GoodreadsBook_Crawler_Full.GoodBook") \
    .getOrCreate()

# Tải dữ liệu từ MongoDB
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

# Làm sạch và biến đổi dữ liệu
df_cleaned = df \
    .withColumn("published_date", regexp_replace(col("published_date"), "First published ", "")) \
    .withColumn("published_date", regexp_replace(col("published_date"), "Published ", "")) \
    .withColumn("published_date", to_date(col("published_date"), "yyyy-MM-dd")) \
    .withColumn("rating_count", regexp_replace(col("rating_count"), "[^0-9]", "").cast(IntegerType())) \
    .withColumn("review_count", regexp_replace(col("review_count"), "[^0-9]", "").cast(IntegerType())) \
    .withColumn("num_pages", regexp_replace(col("num_pages"), "[^0-9]", "").cast(IntegerType())) \
    .withColumn("quotes", regexp_replace(col("quotes"), "[^0-9]", "").cast(IntegerType())) \
    .withColumn("discussions", regexp_replace(col("discussions"), "[^0-9]", "").cast(IntegerType())) \
    .withColumn("questions", regexp_replace(col("questions"), "[^0-9]", "").cast(IntegerType()))

# Loại bỏ sách trùng lặp theo title
df_cleaned_unique_books = df_cleaned.dropDuplicates(["title"])

# Tách các tác giả ra thành các bản ghi riêng lẻ
df_split_authors = df_cleaned_unique_books.withColumn("author", explode(split(col("author"), ","))) \
    .select(trim(col("author")).alias("author")).distinct()

# Chi tiết kết nối PostgreSQL
postgres_url = "jdbc:postgresql://mypostgres:5432/goodreads2_db"
postgres_properties = {
    "user": "postgres",
    "password": "12345",
    "driver": "org.postgresql.Driver"
}

# Ghi các tác giả vào bảng 'TAC_GIA' trong PostgreSQL
df_split_authors.write.jdbc(
    url=postgres_url,
    table="TAC_GIA",
    mode="append",
    properties=postgres_properties
)

# Đọc lại bảng 'TAC_GIA' để lấy ID cho các tác giả
authors_with_id_df = spark.read.jdbc(postgres_url, "TAC_GIA", properties=postgres_properties)

# Tạo một DataFrame với tiêu đề và ID của tác giả
df_with_author_ids = df_cleaned_unique_books.withColumn("author_array", split(col("author"), ",")) \
    .select("title", explode(col("author_array")).alias("cleaned_author_name"))

# Kết hợp để lấy ID của tác giả
df_author_mapping = df_with_author_ids.join(
    authors_with_id_df.select("id_tac_gia", "author"),
    trim(col("cleaned_author_name")) == trim(authors_with_id_df.author),
    "left"
).groupBy("title").agg(collect_list("id_tac_gia").alias("author_ids"))

# Kiểm tra xem author_ids có được tạo thành công không
df_author_mapping.show(truncate=False)  # Xem dữ liệu

# Chuẩn bị dữ liệu cho bảng 'SACH'
df_books = df_cleaned_unique_books.select(
    "title", "num_pages", "published_date", "description", "discussions", "questions"
)

# Ghi dữ liệu 'SACH' đã lọc vào PostgreSQL
df_books.write.jdbc(
    url=postgres_url,
    table="SACH",
    mode="append",
    properties=postgres_properties
)

# Đọc lại bảng 'SACH' để lấy ID của sách
books_with_id_df = spark.read.jdbc(postgres_url, "SACH", properties=postgres_properties)

# Kiểm tra cấu trúc của dataframe để xác nhận cột 'rating' có tồn tại không
df_cleaned_unique_books.printSchema()

# Nếu cột rating không tồn tại, thay vì 'rating', bạn có thể sử dụng cột khác như 'rating_count'
df_rating = df_cleaned_unique_books.select(
    regexp_replace(col("rating"), "[^0-9.]", "").cast(DoubleType()).alias("rating"),  # Sử dụng 'rating' nếu có
    regexp_replace(col("rating_count"), "[^0-9]", "").cast(IntegerType()).alias("rating_count"),
    regexp_replace(col("review_count"), "[^0-9]", "").cast(IntegerType()).alias("review_count"),
    col("title")  # Lấy title để ghép với id_sach
)

# Kết hợp để lấy id_sach tương ứng từ bảng 'SACH'
df_rating_with_id = df_rating.join(
    books_with_id_df.select("id_sach", "title"),
    "title",
    "left"
).select("id_sach", "rating", "rating_count", "review_count")  # Chỉ lấy các cột cần thiết

# Ghi dữ liệu vào bảng 'DANH_GIA_XEP_HANG'
df_rating_with_id.write.jdbc(
    url=postgres_url,
    table="DANH_GIA_XEP_HANG",
    mode="append",
    properties=postgres_properties
)

# Chuẩn bị dữ liệu cho bảng 'TRICH_DAN'
df_quotes = df_cleaned_unique_books.select(
    regexp_replace(col("quotes"), "[^0-9]", "").cast(IntegerType()).alias("quotes"),
    col("title")  # Lấy title để ghép với id_sach
)

# Kết hợp để lấy id_sach tương ứng từ bảng 'SACH'
df_quotes_with_id = df_quotes.join(
    books_with_id_df.select("id_sach", "title"),
    "title",
    "left"
).select("id_sach", "quotes")  # Chỉ lấy các cột cần thiết

# Ghi dữ liệu vào bảng 'TRICH_DAN'
df_quotes_with_id.write.jdbc(
    url=postgres_url,
    table="TRICH_DAN",
    mode="append",
    properties=postgres_properties
)

print("Data has been successfully saved to PostgreSQL.")
