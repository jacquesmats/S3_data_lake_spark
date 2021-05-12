import configparser
from datetime import datetime
import os
import calendar
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp, to_date, from_unixtime
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Process the Song Data JSON file and save artists and songs tables in parquets"""
    # get filepath to song data file
    song_data = os.path.join(input_data,"song_data/*/*/*/*.json")
       
    print("# Reading JSON: {}".format(song_data))
    # read song data file
    #df = spark.read.json(song_data)
    
    # test a small sample
    df = spark.read.json('data/song_data/A/A/*')

    print("# Extracting columns")
    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates(['song_id'])
    
    print("# Writing song table to parquet")
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode('overwrite').parquet(os.path.join(output_data, 'songs.parquet'))                               
    print("# Extracting columns")
    # extract columns to create artists table 
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude').dropDuplicates(['artist_id'])
    
    print("# Writing artists table to parquet")
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(os.path.join(output_data, 'artists.parquet'))      


def process_log_data(spark, input_data, output_data):
    """Process the Log Data JSON file and save users, time and songplays tables in parquets"""
    
    # get filepath to log data file
    log_data =os.path.join(input_data,"log_data/*/*/*.json")
    
    print("# Reading JSON: {}".format(log_data))
    # read log data file
    #df = spark.read.json(log_data)
    
    # test in a small sample                                                                             
    df = spark.read.json('data/log-data/*')
    
    print("# Processing songplays table")
    # filter by actions for song plays
    songplays_table = df.select('ts', 'userId', 'level','sessionId', 'location', 'userAgent')

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').dropDuplicates(['userId', 'level'])
    
    print("# Writing users table to parquet")
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(os.path.join(output_data, 'users.parquet'))
    
    print("# Processing timestamp column")
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # helper functions
    get_datetime = udf(lambda x: datetime.fromtimestamp(int(int(x)/1000)))
    get_weekday = udf(lambda x: x.isocalendar()[1])


    df = df.withColumn('start_time', get_datetime(df.ts))
    df = df.withColumn('hour', hour(df.start_time))
    df = df.withColumn('day', dayofmonth(df.start_time))
    df = df.withColumn('week', weekofyear(df.start_time))
    df = df.withColumn('month', month(df.start_time))
    df = df.withColumn('year', year(df.start_time))
    df = df.withColumn('weekday', get_weekday(df.start_time))
    
    # extract columns to create time table
    time_table = df['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday'].dropDuplicates(['start_time'])
    
    print("# Writing time table to parquet")
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").mode('overwrite').parquet(os.path.join(output_data, 'time.parquet'))
    
    # read in song data to use for songplays table
    song_df = spark.read.parquet("songs.parquet")

    print("# Extracting columns")
    # extract columns from joined song and log datasets to create songplays table
    df = df.join(song_df, song_df.title == df.song)
    songplays_table = df['start_time', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent']
    songplays_table.select(monotonically_increasing_id().alias('songplay_id')).collect()
        
    print("# Writing songplays table to parquet")
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(os.path.join(output_data, 'songplays.parquet'))
    

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    print("\n...\n\n### Spark Session Created")
    
    print("\n### Processing Song Data")
    process_song_data(spark, input_data, output_data)

    print("\n### Processing Log Data")
    process_log_data(spark, input_data, output_data)
    
    print("\n### ETL Complete")

if __name__ == "__main__":
    main()
