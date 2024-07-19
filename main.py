from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("IntrumentGenres").getOrCreate()

#* Пусть:
#* Продукты - музыкальные интсрументы
#* Категории - жанры, в которых эти инструменты встречаются

instruments_path = "./data/instrument.csv"
genres_path = "./data/genres.csv"
instrument_genre_link_path = "./data/instrument_genres_link.csv"

instruments = spark.read.option("header", "true").csv(instruments_path)
genres = spark.read.option("header", "true").csv(genres_path)
instrument_genre_link = spark.read.option("header", "true").csv(instrument_genre_link_path)

instruments = instruments.withColumn("InstrumentID", col("InstrumentID").cast("int"))
genres = genres.withColumn("GenreID", col("GenreID").cast("int"))
instrument_genre_link = instrument_genre_link \
                            .withColumn("InstrumentID", col("InstrumentID").cast("int")) \
                            .withColumn("GenreID", col("GenreID").cast("int"))

# Внутреннее соединение для получения всех пар "Название интсрумента - Название жанра"
instruments_with_genres = instruments \
                            .join(instrument_genre_link, on="InstrumentID", how="inner") \
                            .join(genres, on="GenreID", how="inner") \
                            .select("InstrumentName", "GenreName")

# Левое соединение для получения всех инструментов (в том числе тех, которые не связаны ни с одним жанром)
all_instruments_genres = instruments \
                            .join(instrument_genre_link, on="InstrumentID", how="left") \
                            .join(genres, on="GenreID", how="left") \
                            .select("InstrumentName", "GenreName")

# Отфильтровываем интсрументы без жанров
instruments_without_genres = all_instruments_genres \
                                .filter(col("GenreName").isNull()) \
                                .select("InstrumentName")

instruments_with_genres.show()
instruments_without_genres.show()

spark.stop()
