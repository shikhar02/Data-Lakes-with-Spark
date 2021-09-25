"""
   Discription: Importing important modules and functions.
"""
import configparser
from datetime import datetime
from pyspark.sql import SparkSession,functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, to_timestamp


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():

    '''
            Discription: Function to start a spark session which will be use to process data and read or write data to S3.
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):

    '''
            Discription: - This function will process song data by reading files from S3 and dividing data into two tables: 1) \
                           Songs 2) Artists. 
                         - Also, both tables will be loaded back to S3 and stored in parquet format under their respective \
                           directory name.
    '''

    # File path of song data in S3 bucket.
    song_data = input_data+'song_data/A/*/*/*.json'
    
    # read song data file
    df = spark.read.format('json').load(song_data)
    
    df = df.withColumn('duration',col('duration').cast('float')) \
    .withColumn('num_songs',col('num_songs').cast('int')) \
    .withColumn('year',col('year').cast('bigint'))
    
    print(df.show(10))
    
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').drop_duplicates()
    
    # Load songs table back to S3 in parquet file format which will be partitioned by year and artist
    songs_table.write.parquet(output_data+'songs/songs.parquet',partitionBy=['year','artist_id'],mode='overwrite')


    artist = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude').drop_duplicates()
    
    #  Load artist table back to S3 in parquet file format
    artist.write.parquet(output_data+'artists/artists.parquet',mode='overwrite')

def process_log_data(spark, input_data, output_data):

    '''
            Discription: - This function will load log data from S3, divide data into three tables 1)Users 2) Time 3)Songplays, \
                           remove some quality issues. 
                         - Data will be loaded back to S3 in separate directories in S3 bucket.
                         - Extract some more data from 'ts' column like year, month etc..
                         - Load song data from S3 for songplays table.
                         - Join song data on log data.
                         - Write files in parquet format to S3 and partitioned by 'year' & 'month'.
    '''

    # File path of Log data in S3 bucket.
    log_data = input_data+'log_data/*/*/*.json'

    # Read log data file from S3.
    df = spark.read.json(log_data)

    df = df.withColumn('length',col('length').cast('float')) \
    .withColumn('sessionId',col('sessionId').cast('int')) \
    .withColumn('userId',col('userId').cast('int'))
    
    # Remove all the rows from log data where column 'page' is not equal to 'NextSong'.
    df = df.select('*').filter(col('page')=='NextSong').drop_duplicates()

    # Extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level')
    print(users_table.show(10))

    # Write users table to parquet files
    users_table.write.parquet(output_data+'users/users.parquet',mode='overwrite')

    # Convert and create a timestamp column with timestamp as datatype from 'ts' column.
    
    df = df.withColumn('timestamp',to_timestamp(col('ts')/1000.0))
    
    # Extract some more datetime columns from 'timestamp' column
    #get_datetime = udf()
    df = df.withColumn('start_time', col('timestamp')) \
    .withColumn("hour", hour(col("timestamp"))) \
    .withColumn("day", dayofmonth(col("timestamp"))) \
    .withColumn("week", weekofyear(col("timestamp"))) \
    .withColumn("month", month(col("timestamp"))) \
    .withColumn("year",year(col("timestamp"))) \
    .withColumn("weekday",date_format(col("timestamp"), "EEEE"))
    
    # Extract columns to create time table
    time_table = df.select('start_time','hour','day','week','month','year','weekday')
    
    print(time_table.show(10))
    
    # Load artist table to back to S3 in parquet file format partitioned by 'year' & 'month'.
    time_table.write.parquet(output_data+'time/time.parquet',partitionBy=['year','month'],mode='overwrite')

    # Load song data from S3 to be used in songplays table.
    song_path = input_data + 'song_data/A/*/*/*.json'
    song_df = spark.read.json(song_path)
    
    # Creating a temporary data set which be use by spark.sql API to query the results.
    df.createOrReplaceTempView('events')
    song_df.createOrReplaceTempView('songs')

    # Creating songplays table by joining song data with log data.
    songplays_table = spark.sql('''
                                SELECT row_number() over (order by events.timestamp) AS songplay_id, 
                                events.timestamp AS start_time,
                                events.userId AS user_id,
                                events.level AS level,
                                songs.song_id,
                                songs.artist_id,
                                events.sessionId AS session_id,
                                events.location,
                                events.userAgent AS user_agent,
                                year(events.timestamp) AS year,
                                month(events.timestamp) AS month
                                FROM events
                                JOIN songs ON (events.artist = songs.artist_name
                                AND events.song = songs.title)
                                ''')

    print(songplays_table.show(10))

    # Load songplays_table to S3 in parquet file format partitioned by 'year' and 'month'.
    songplays_table.write.parquet(output_data+'songplays/songplays.parquet',partitionBy=['year','month'],mode='overwrite')



def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://datalake2/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
