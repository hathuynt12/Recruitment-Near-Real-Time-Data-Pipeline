#import findspark
#findspark.init()
from pyspark.sql import SparkSession
import os
import time
import datetime
import pyspark.sql.functions as sf
from uuid import *
from pyspark.sql import SQLContext
from pyspark.sql.functions import when, col, lit
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row
from pyspark.sql.functions import udf, monotonically_increasing_id
from pyspark.sql.window import Window as W

spark = SparkSession.builder \
    .config("spark.jars", "/opt/bitnami/spark/jars/spark-cassandra-connector-assembly_2.12-3.4.0.jar") \
    .getOrCreate()

def calculating_clicks(df):
    clicks_data = df.filter(df.custom_track == 'click')
    clicks_data = clicks_data.na.fill({'bid':0, 'job_id':0, 'publisher_id':0, 'group_id':0, 'campaign_id':0})
    clicks_data.registerTempTable('clicks')
    clicks_output = spark.sql("""
        SELECT job_id, date(ts) AS date, hour(ts) AS hour, publisher_id, campaign_id, group_id,
               AVG(bid) AS bid_set, COUNT(*) AS clicks, SUM(bid) AS spend_hour
        FROM clicks
        GROUP BY job_id, date(ts), hour(ts), publisher_id, campaign_id, group_id
    """)
    return clicks_output

def calculating_conversion(df):
    conversion_data = df.filter(df.custom_track == 'conversion')
    conversion_data = conversion_data.na.fill({'job_id':0, 'publisher_id':0, 'group_id':0, 'campaign_id':0})
    conversion_data.registerTempTable('conversion')
    conversion_output = spark.sql("""
        SELECT job_id, date(ts) AS date, hour(ts) AS hour, publisher_id, campaign_id, group_id,
               COUNT(*) AS conversions
        FROM conversion
        GROUP BY job_id, date(ts), hour(ts), publisher_id, campaign_id, group_id
    """)
    return conversion_output

def calculating_qualified(df):
    qualified_data = df.filter(df.custom_track == 'qualified')
    qualified_data = qualified_data.na.fill({'job_id':0, 'publisher_id':0, 'group_id':0, 'campaign_id':0})
    qualified_data.registerTempTable('qualified')
    qualified_output = spark.sql("""
        SELECT job_id, date(ts) AS date, hour(ts) AS hour, publisher_id, campaign_id, group_id,
               COUNT(*) AS qualified
        FROM qualified
        GROUP BY job_id, date(ts), hour(ts), publisher_id, campaign_id, group_id
    """)
    return qualified_output

def calculating_unqualified(df):
    unqualified_data = df.filter(df.custom_track == 'unqualified')
    unqualified_data = unqualified_data.na.fill({'job_id':0, 'publisher_id':0, 'group_id':0, 'campaign_id':0})
    unqualified_data.registerTempTable('unqualified')
    unqualified_output = spark.sql("""
        SELECT job_id, date(ts) AS date, hour(ts) AS hour, publisher_id, campaign_id, group_id,
               COUNT(*) AS unqualified
        FROM unqualified
        GROUP BY job_id, date(ts), hour(ts), publisher_id, campaign_id, group_id
    """)
    return unqualified_output

def process_final_data(clicks_output, conversion_output, qualified_output, unqualified_output):
    final_data = clicks_output.join(conversion_output,['job_id','date','hour','publisher_id','campaign_id','group_id'],'full') \
                               .join(qualified_output,['job_id','date','hour','publisher_id','campaign_id','group_id'],'full') \
                               .join(unqualified_output,['job_id','date','hour','publisher_id','campaign_id','group_id'],'full')
    return final_data

# ✅ Sửa: trả về thêm max(ts) để dùng làm updated_at
def process_cassandra_data(df):
    clicks_output = calculating_clicks(df)
    conversion_output = calculating_conversion(df)
    qualified_output = calculating_qualified(df)
    unqualified_output = calculating_unqualified(df)
    final_data = process_final_data(clicks_output, conversion_output, qualified_output, unqualified_output)
    max_ts = df.agg(sf.max("ts")).collect()[0][0]  # ✅ lấy max ts từ dữ liệu batch hiện tại
    return final_data, max_ts

def retrieve_company_data(url, driver, user, password):
    sql = """(SELECT id AS job_id, company_id, group_id, campaign_id FROM jobs) test"""
    company = spark.read.format('jdbc').options(url=url, driver=driver, dbtable=sql, user=user, password=password).load()
    return company

def import_to_mysql(final_output):
    final_output = final_output.select(
        'job_id','date','hour','publisher_id','company_id','campaign_id','group_id',
        'unqualified','qualified','conversions','clicks','bid_set','spend_hour','updated_at'
    )

    final_output = final_output.withColumnRenamed('date','dates') \
        .withColumnRenamed('hour','hours') \
        .withColumnRenamed('qualified','qualified_application') \
        .withColumnRenamed('unqualified','disqualified_application') \
        .withColumnRenamed('conversions','conversion') \
        .withColumn('sources', lit('Cassandra'))

    final_output.printSchema()

    final_output.write.format("jdbc") \
        .option("driver","com.mysql.cj.jdbc.Driver") \
        .option("url", "jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}") \
        .option("dbtable", "events") \
        .mode("append") \
        .option("user", MYSQL_USER) \
        .option("password", MYSQL_PASSWORD) \
        .save()

    print('Data imported successfully')

def main_task(mysql_time):
    host = MYSQL_HOST
    port = MYSQL_PORT
    db_name = MYSQL_DB
    user = MYSQL_USER
    password = MYSQL_PASSWORD
    url = 'jdbc:mysql://' + host + ':' + port + '/' + db_name
    driver = "com.mysql.cj.jdbc.Driver"

    print('The host is', host)
    print('The port using is', port)
    print('The db using is', db_name)
    print('-----------------------------')
    print('Retrieving data from Cassandra')
    print('-----------------------------')
    df = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table="tracking", keyspace="logs") \
    .option("spark.cassandra.connection.host", "cassandra") \
    .load().where(col('ts') >= mysql_time)

    print('-----------------------------')
    print('Selecting data from Cassandra')
    print('-----------------------------')
    df = df.select('ts','job_id','custom_track','bid','campaign_id','group_id','publisher_id')
    df = df.filter(df.job_id.isNotNull())
    df.printSchema()
    print('-----------------------------')
    print('Processing Cassandra Output')
    print('-----------------------------')

    # ✅ nhận thêm max_ts từ batch
    cassandra_output, max_ts = process_cassandra_data(df)

    print('-----------------------------')
    print('Merge Company Data')
    print('-----------------------------')
    company = retrieve_company_data(url, driver, user, password)
    print('-----------------------------')
    print('Finalizing Output')
    print('-----------------------------')
    final_output = cassandra_output.join(company, 'job_id', 'left') \
                                    .drop(company.group_id) \
                                    .drop(company.campaign_id)

    # ✅ thêm cột updated_at trước khi import
    final_output = final_output.withColumn("updated_at", sf.lit(max_ts))

    print('-----------------------------')
    print('Import Output to MySQL')
    print('-----------------------------')
    import_to_mysql(final_output)
    print('Task Finished')

def get_latest_time_cassandra():
    data = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table='tracking', keyspace='logs') \
    .option("spark.cassandra.connection.host", "cassandra") \
    .load()


    cassandra_latest_time = data.agg({'ts': 'max'}).take(1)[0][0]
    return cassandra_latest_time

def get_mysql_latest_time(url, driver, user, password):
    sql = """(SELECT MAX(updated_at) FROM events) data"""
    mysql_time = spark.read.format('jdbc').options(
        url=url, driver=driver, dbtable=sql, user=user, password=password
    ).load().take(1)[0][0]

    if mysql_time is None:
        # ✅ Trả về kiểu datetime đúng thay vì string
        mysql_latest = datetime.datetime.strptime("1998-01-01 23:59:59", "%Y-%m-%d %H:%M:%S")
    else:
        mysql_latest = mysql_time
    return mysql_latest

host = MYSQL_HOST
port = MYSQL_PORT
db_name = MYSQL_DB
user = MYSQL_USER
password = MYSQL_PASSWORD
url = 'jdbc:mysql://' + host + ':' + port + '/' + db_name
driver = "com.mysql.cj.jdbc.Driver"

while True:
    start_time = datetime.datetime.now()
    cassandra_time = get_latest_time_cassandra()
    print(f'Cassandra latest time is {cassandra_time}')
    mysql_time = get_mysql_latest_time(url, driver, user, password)
    print(f'MySQL latest time is {mysql_time}')

    if cassandra_time > mysql_time:
        main_task(mysql_time)
    else:
        print("No new data found")

    end_time = datetime.datetime.now()
    execution_time = (end_time - start_time).total_seconds()
    print(f'Job takes {execution_time} seconds to execute')
    time.sleep(10)
