from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('IntrumentGenres').getOrCreate()

#* Пусть:
#* Продукты - музыкальные интсрументы
#* Категории - жанры, в которых эти инструменты встречаются

instruments_path = './data/instrument.csv'
genres_path = './data/genres.csv'
instrument_genre_link_path = './data/instrument_genres_link.csv'

instruments = spark.read.option('header', 'true').csv(instruments_path)
genres = spark.read.option('header', 'true').csv(genres_path)
instrument_genre_link = spark.read.option('header', 'true').csv(instrument_genre_link_path)

# instruments = instruments.withColumn('InstrumentID', col('InstrumentID').cast('int'))
# genres = genres.withColumn('GenreID', col('GenreID').cast('int'))
# instrument_genre_link = instrument_genre_link \
#                             .withColumn('InstrumentID', col('InstrumentID').cast('int')) \
#                             .withColumn('GenreID', col('GenreID').cast('int'))


# все пары инструмент - жанр 
instruments_genres_pairs = instruments \
                            .join(instrument_genre_link, on='InstrumentID', how='left') \
                            .join(genres, on='GenreID', how='left') \
                            .select('InstrumentName', 'GenreName')

# отфильтровываем с жанром
instruments_with_genres = instruments_genres_pairs \
                            .filter(col('GenreName').isNotNull()) \
                            .select('InstrumentName', 'GenreName')

# отфильтровываем без жанра
instruments_without_genres = instruments_genres_pairs \
                                .filter(col('GenreName').isNull()) \
                                .select('InstrumentName')


print('Instruments with genres:')
instruments_with_genres.show()

print('Instruments without genres:')
instruments_without_genres.show()

spark.stop()
