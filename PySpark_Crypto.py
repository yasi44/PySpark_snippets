# import argparse
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.types import *
# import pyspark.pandas as ps
from pyspark.sql import functions as F
from pyspark.sql.functions import substring_index, substring, concat, col, lit, to_date, to_timestamp, ceil, col, round, when, mean, stddev
from pyspark.sql.window import Window
import sys
# import matplotlib.pyplot as plt
from math import floor
from datetime import datetime
import gzip
import shutil
import boto3
import csv
from io import BytesIO, StringIO
from pyspark.sql.types import *


# Auxiliar functions
def equivalent_type(f):
    if f == 'datetime64[ns]': return TimestampType()
    elif f == 'int64': return LongType()
    elif f == 'int32': return IntegerType()
    elif f == 'float64': return DoubleType()
    elif f == 'float32': return FloatType()
    else: return StringType()


def define_structure(string, format_type):
    try: typo = equivalent_type(format_type)
    except: typo = StringType()
    return StructField(string, typo)


# Given pandas dataframe, it will return a spark's dataframe.
def pandas_to_spark(pandas_df, sc):
    columns = list(pandas_df.columns)
    types = list(pandas_df.dtypes)
    struct_list = []
    for column, typo in zip(columns, types):
      struct_list.append(define_structure(column, typo))
    p_schema = StructType(struct_list)
    return sc.createDataFrame(pandas_df, p_schema)


def write_to_S3_as_scv(df_in, bucket_name, outputfile):
    # to store to S3
    # bucket_name = 'temp-bucket'  # already created on S3
    # result_pdf = df_in.select("*").toPandas()

    csv_buffer = StringIO()
    df_in.to_csv(csv_buffer)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket_name, outputfile).put(Body=csv_buffer.getvalue())
    # outputfile = 's3a://trade-temp-bucket/'+outputfile
    # df_in.write.format('csv').option('header', True).mode('overwrite').option('sep', ',').save('s3a://temp-bucket/'+outputfile)


#exchange,symbol,timestamp,local_timestamp,id,side,price,amount
mySchema = StructType([ StructField("exchange", StringType(), True)
                          ,StructField("symbol", StringType(), True)
                          ,StructField("timestamp", StringType(), True)
                          ,StructField("local_timestamp", StringType(), True)
                          ,StructField("id", StringType(), True)
                          ,StructField("side", StringType(), True)
                          ,StructField("price", DoubleType(), True)
                          ,StructField("amount", StringType(), True)
                          ,StructField("time", StringType(), True)])


def calculate_Trade_CVD(data_source, output_uri):
    """
    :param data_source: The URI of data CSV, such as 's3://DOC-EXAMPLE-BUCKET/temp-data.csv'.
    :param output_uri: The URI where output is written, such as 's3://DOC-EXAMPLE-BUCKET/temp_results'.
    """
    # store the .gz file into temp bucket as .csv
#     data_source = 's3a://v2-manual-backfill/ftx/TRADE/ftx_trades_2022-01-02_FUTURES.csv.gz'

    with SparkSession.builder.appName("Calculate Trade_CVD").config("spark.some.config.option", "some-value").getOrCreate() as spark:
        # sparkDF_inputdata = pandas_to_spark(df_inputdata, spark)
        # temporary disabled   #todo: later on fix it, to use this method
        # sparkDF_inputdata = spark.createDataFrame(df_inputdata, schema=mySchema)

        # Load CSV data from S3
        # if data_source is not None:
#             trades_df = spark.read.option("header", "true").csv('/home/<your path>PycharmProjects/pythonProject/FTX/trades_history/0.csv')
#             trades_df = spark.read.option("header", "true").csv(data_source)
#             trades_df.to_csv(output_uri, storage_options={'key': '...', 'secret': '...'})
        sparkDF_inputdata = spark.read.format('csv').options(header='true', inferSchema='true').load(data_source)
        # df.head(2)

        # with gzip.open(data_source, 'rb') as f_in:
        #     with open(output_uri, 'wb') as f_out: # bucket temppath
        #         shutil.copyfileobj(f_in, f_out)
    
        # trades_df = sparkDF_inputdata.withColumn("price", col("price").cast("double")).withColumn("size", col("size").cast("double")).withColumn("liquidation", col("liquidation").cast("boolean"))
        trades_df = sparkDF_inputdata.withColumn("price", col("price").cast("double")).\
            withColumn("size", col("amount").cast("double"))#\
            #.          withColumn("time", col("timestamp").cast("unixtime"))

        # F.from_utc_timestamp
        # trades_df.withColumn("date", col("price").cast("double"))
        # df3 = trades_df.select(F.from_unixtime(col("timestamp"), "MM-dd-yyyy HH:mm:ss").alias("time"))
        # F.when((trades_df.date.isNull() | (trades_df.date == '')), '0').otherwise(F.unix_timestamp(trades_df.timestamp, 'yyyy-MM-dd HH:mm:ss.SSS'))

        # add  time_ms_right_4 cols --> contains the rounded decimal",
        trades_df= trades_df.withColumn("time_ms_right_4", substring_index(round(substring(trades_df.time, 18, 7),3),'.',-1))

        # concat ms to the rest of datetime
        #df=df.withColumn('joined_column', F.concat(df.datetime_ms,F.lit('.'),F.col('time_ms_right_4')))
        trades_df=trades_df.withColumn('joined_column', F.concat(substring(trades_df.time, 1, 19),F.lit('.'),F.col('time_ms_right_4')))

        # convert to date time data type
        trades_df=trades_df.withColumn("time", to_timestamp(trades_df.joined_column, "yyyy-MM-dd HH:mm:ss.SSS"))
        trades_df = trades_df.drop("joined_column", "time_ms_right_4")

        # ----- if dataformat is timestamp
        # trades_df=trades_df.withColumn("time", to_date("timestamp"))
        # trades_df=trades_df.select(F.from_unixtime(col("timestamp"), "yyyy-MM-dd HH:mm:ss.SSS").alias("time")

        trades_df = trades_df.withColumn("size_usd", (trades_df.price * trades_df.size))

        # trades['side'] = np.where(trades['side'] == 'buy', 1, -1)
        trades_df = trades_df.withColumn('side', when(trades_df.side=='buy',1).otherwise(0))

        # group_trades = trades.copy().groupby('time')
        group_trades_df = trades_df.alias('group_trades_df')

        # processed_trades['average_price'] = group_trades.mean()['price']
        temp2 = group_trades_df.groupBy('time').sum('size').withColumnRenamed("sum(size)", "sum_size").withColumnRenamed("time", "time2")

        # processed_trades['average_price'] = group_trades.mean()['price']
        temp3 = trades_df.groupBy('time').mean('price').withColumnRenamed("avg(price)", "mean_price").withColumnRenamed("time", "time3")

        # processed_trades['size_usd'] = group_trades.sum()['size_usd']
        temp4 = trades_df.groupBy('time').sum('size_usd').withColumnRenamed("sum(size_usd)", "sum_size_usd").withColumnRenamed("time", "time4")

        # processed_trades['side'] = group_trades.sum()['side']
        temp5 = group_trades_df.groupBy('time').sum('side').withColumnRenamed("sum(side)", "sum_side").withColumnRenamed("time", "time5")

        processed_trades = temp2.join(temp3,  temp2.time2 == temp3.time3).join(temp4,  temp2.time2 == temp4.time4).join(temp5,  temp2.time2 == temp5.time5)
        processed_trades = processed_trades.drop('time3','time4','time5')

        vol_tier_1 = processed_trades.filter(processed_trades["sum_size"]<1)
        # vol_tier_1.count() # to test\n",
        vol_tier_2 = processed_trades.filter((processed_trades["sum_size"]>=1) & (processed_trades["sum_size"]<5))
        vol_tier_3 = processed_trades.filter((processed_trades["sum_size"]>=5) & (processed_trades["sum_size"]<10))
        vol_tier_4 = processed_trades.filter(processed_trades["sum_size"]>10)

        # write_to_S3_as_scv(vol_tier_1, 'trade-temp-bucket', 'vol_1.csv')
        # write_to_S3_as_scv(vol_tier_2, 'trade-temp-bucket', 'vol_2.csv')
        # write_to_S3_as_scv(vol_tier_3, 'trade-temp-bucket', 'vol_3.csv')
        # write_to_S3_as_scv(vol_tier_4, 'trade-temp-bucket', 'vol_4.csv')

        windowval = (Window.partitionBy('tempCol').orderBy('time2').rangeBetween(Window.unboundedPreceding, 0))

        vol_tier_1 = vol_tier_1.withColumn("tempCol", lit(0))
        vol_tier_1 = vol_tier_1.withColumn('cum_sum', F.sum('sum_size').over(windowval))
        # vol_tier_1.show(2, False)

        vol_tier_2 = vol_tier_2.withColumn("tempCol", lit(0))
        vol_tier_2 = vol_tier_2.withColumn('cum_sum', F.sum('sum_size').over(windowval))
        # vol_tier_2.show(2, False)

        vol_tier_3 = vol_tier_3.withColumn("tempCol", lit(0))
        # windowval = (Window.partitionBy('tempCol').orderBy('time').rangeBetween(Window.unboundedPreceding, 0))
        vol_tier_3 = vol_tier_3.withColumn('cum_sum', F.sum('sum_size').over(windowval))
        # vol_tier_3.show(2, False)
        # vol_tier_3.cum_sum  is actually cv_3

        vol_tier_4 = vol_tier_4.withColumn("tempCol", lit(0))
        # windowval = (Window.partitionBy('tempCol').orderBy('time').rangeBetween(Window.unboundedPreceding, 0))
        vol_tier_4 = vol_tier_4.withColumn('cum_sum', F.sum('sum_size').over(windowval))

        #------------
        vol_tier_1 = vol_tier_1.withColumn('side_new', when(vol_tier_1.sum_side>1,1).otherwise(vol_tier_1.sum_side))
        vol_tier_1 = vol_tier_1.withColumn('side_new', when(vol_tier_1.sum_side<-1,-1).otherwise(vol_tier_1.sum_side))
        vol_tier_1 = vol_tier_1.withColumn('size_new', vol_tier_1.side_new * vol_tier_1.sum_size)

        vol_tier_2 = vol_tier_2.withColumn('side_new', when(vol_tier_2.sum_side>1,1).otherwise(vol_tier_2.sum_side))
        vol_tier_2 = vol_tier_2.withColumn('side_new', when(vol_tier_2.sum_side<-1,-1).otherwise(vol_tier_2.sum_side))
        vol_tier_2 = vol_tier_2.withColumn('size_new', vol_tier_2.side_new * vol_tier_2.sum_size)

        vol_tier_3 = vol_tier_3.withColumn('side_new', when(vol_tier_3.sum_side>1,1).otherwise(vol_tier_3.sum_side))
        vol_tier_3 = vol_tier_3.withColumn('side_new', when(vol_tier_3.sum_side<-1,-1).otherwise(vol_tier_3.sum_side))
        vol_tier_3 = vol_tier_3.withColumn('size_new', vol_tier_3.side_new * vol_tier_3.sum_size)

        vol_tier_4 = vol_tier_4.withColumn('side_new', when(vol_tier_4.sum_side>1,1).otherwise(vol_tier_4.sum_side))
        vol_tier_4 = vol_tier_4.withColumn('side_new', when(vol_tier_4.sum_side<-1,-1).otherwise(vol_tier_4.sum_side))
        vol_tier_4 = vol_tier_4.withColumn('size_new', vol_tier_4.side_new * vol_tier_4.sum_size)

        #--------
        vol_tier_1 = vol_tier_1.withColumn('cum_sum_cvd', F.sum('size_new').over(windowval))
        vol_tier_2 = vol_tier_2.withColumn('cum_sum_cvd', F.sum('size_new').over(windowval))
        vol_tier_3 = vol_tier_3.withColumn('cum_sum_cvd', F.sum('size_new').over(windowval))
        vol_tier_4 = vol_tier_4.withColumn('cum_sum_cvd', F.sum('size_new').over(windowval))

        #------------
        tempMean = vol_tier_1.select(mean('cum_sum_cvd')).first()['avg(cum_sum_cvd)']
        tempSTD = vol_tier_1.select(stddev('cum_sum_cvd')).first()['stddev_samp(cum_sum_cvd)']
        vol_tier_1 = vol_tier_1.withColumn('cum_sum_cvd_normalized',(vol_tier_1['cum_sum_cvd'] - tempMean)/tempSTD)
#             print(tempMean,tempSTD,vol_tier_1)

        tempMean = vol_tier_2.select(mean('cum_sum_cvd')).first()['avg(cum_sum_cvd)']
        tempSTD = vol_tier_2.select(stddev('cum_sum_cvd')).first()['stddev_samp(cum_sum_cvd)']
        vol_tier_2 = vol_tier_2.withColumn('cum_sum_cvd_normalized',(vol_tier_2['cum_sum_cvd'] - tempMean)/tempSTD)
#             print(tempMean,tempSTD,vol_tier_2)

        tempMean = vol_tier_3.select(mean('cum_sum_cvd')).first()['avg(cum_sum_cvd)']
        tempSTD = vol_tier_3.select(stddev('cum_sum_cvd')).first()['stddev_samp(cum_sum_cvd)']
        vol_tier_3 = vol_tier_3.withColumn('cum_sum_cvd_normalized',(vol_tier_3['cum_sum_cvd'] - tempMean)/tempSTD)
#             print(tempMean,tempSTD,vol_tier_3)

        tempMean = vol_tier_4.select(mean('cum_sum_cvd')).first()['avg(cum_sum_cvd)']
        tempSTD = vol_tier_4.select(stddev('cum_sum_cvd')).first()['stddev_samp(cum_sum_cvd)']
        vol_tier_4 = vol_tier_4.withColumn('cum_sum_cvd_normalized',(vol_tier_4['cum_sum_cvd'] - tempMean)/tempSTD)
#             print(tempMean,tempSTD,vol_tier_4)

        # Write the results to the specified output URI
        # vol_tier_1.write.option("header", "true").mode("overwrite").csv(output_uri)
        # vol_tier_2.write.option("header", "true").mode("overwrite").csv(output_uri)
        # vol_tier_3.write.option("header", "true").mode("overwrite").csv(output_uri)
        # vol_tier_4.write.option("header", "true").mode("overwrite").csv(output_uri)

        # vol_tier_1_pandas = vol_tier_1.select("*").toPandas()
        # vol_tier_2_pandas = vol_tier_2.select("*").toPandas()
        # vol_tier_3_pandas = vol_tier_3.select("*").toPandas()
        # vol_tier_4_pandas = vol_tier_4.select("*").toPandas()

        write_to_S3_as_scv(vol_tier_1.select("*").toPandas(), 'trade-temp-bucket', 'vol_tier_1.csv')
        write_to_S3_as_scv(vol_tier_2.select("*").toPandas(), 'trade-temp-bucket', 'vol_tier_2.csv')
        write_to_S3_as_scv(vol_tier_3.select("*").toPandas(), 'trade-temp-bucket', 'vol_tier_3.csv')
        write_to_S3_as_scv(vol_tier_4.select("*").toPandas(), 'trade-temp-bucket', 'vol_tier_4.csv')

#             vol_tier_1.toPandas().to_csv('pandas_vol_tier_1.csv')
#             vol_tier_2.toPandas().to_csv('pandas_vol_tier_2.csv')
#             vol_tier_3.toPandas().to_csv('pandas_vol_tier_3.csv')
#             vol_tier_4.toPandas().to_csv('pandas_vol_tier_4.csv')


if __name__ == "__main__":
    # parser = argparse.ArgumentParser()
    # parser.add_argument('--data_source', help="The URI of .csv file in S3 bucket location.")
    # parser.add_argument('--output_uri', help="The URI where output is saved, like an S3 bucket location.")
    # args = parser.parse_args()
    # list_file = ['ftx/TRADE/ftx_trades_2022-01-02_FUTURES.csv.gz']#,
    list_file = ['ftx/TRADE/ftx_trades_2022-01-02_PERPETUALS.csv.gz']#,
                 # 'ftx/TRADE/ftx_trades_2022-01-01_PERPETUALS.csv.gz',
                 # 'ftx/TRADE/ftx_trades_2022-01-02_PERPETUALS.csv.gz',
                 # 'ftx/TRADE/ftx_trades_2022-01-03_PERPETUALS.csv.gz']

    for file in list_file:
        try:
            # s3 = boto3.resource("s3")
            # obj = s3.Object("v2-manual-backfill", file)
            # n = obj.get()["Body"].read()
            # gzipfile = BytesIO(n)
            # gzipfile = gzip.GzipFile(fileobj = gzipfile)
            # content = gzipfile.read()
            # print(content)

            # works correctly
            s3 = boto3.resource("s3")
            obj = s3.Object("v2-manual-backfill", file)
            df_in = pd.read_csv(obj.get()["Body"], compression='gzip', sep=',')

            # df_in = pd.read_csv('binance_incremental_book_L2_2022-05-05_BTCUSDT-sample.csv')
            # df_in = pd.read_csv('ftx_trades_2022-01-02_FUTURES.csv')
            df_in['time']=pd.to_datetime(df_in['timestamp']//1000, unit='ms')
            write_to_S3_as_scv(df_in, "temp-bucket", "tempFile.csv")

            # df_in.to_csv('tempFile.csv', header=True, sep=',')
            calculate_Trade_CVD()#df_in)  # args.data_source, args.output_uri)

        except Exception as e:
            print(e)
            raise e
            
