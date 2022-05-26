import time
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("PySpark PostgreSQL") \
    .config("spark.jars", "C:/tools/spark-3.0.2-bin-hadoop2.7/jars/postgresql-42.2.19.jar") \
    .config("spark.local.dir", "C:/tmp") \
    .config("spark.executor.instances", "1") \
    .getOrCreate()

user = "postgres"
password = "toor"



sql_q1_a=" \
    SELECT COUNT(*), CAST(time_stamp AS DATE) AS timestamp_date \
    FROM pullreq_event WHERE ev='opened' \
    GROUP BY timestamp_date \
    ORDER BY timestamp_date \
"
sql_q1_b=" \
    SELECT COUNT(DISTINCT(pull_requestid)), CAST(time_stamp AS DATE) AS timestamp_date \
    FROM pullreq_event WHERE ev='discussed' \
    GROUP BY timestamp_date \
    ORDER BY timestamp_date \
"
sql_q2=" \
    WITH \
    tmp as \
    ( \
        SELECT EXTRACT(MONTH FROM time_stamp) as date_month, \
        author, count(*) as numPRs \
        FROM pullreq_event \
        WHERE ev = 'discussed' \
        GROUP BY date_month, author \
    ) \
    SELECT * FROM tmp \
    WHERE (date_month, numPRs) IN \
    ( \
        SELECT date_month, MAX(numPRs) \
        FROM tmp \
        GROUP BY date_month \
    ) \
"
sql_q3="\
    WITH \
    tmp AS \
    ( \
        SELECT DATE( DATE(time_stamp) - interval '1 day'*EXTRACT(DOW FROM time_stamp)) AS sunday_of_week, \
        COUNT(*) AS numPRs, author \
        FROM pullreq_event \
        WHERE ev = 'discussed' \
        GROUP BY sunday_of_week, author \
    ) \
    SELECT * FROM tmp \
    WHERE (sunday_of_week, numPRs) IN \
    ( \
        SELECT sunday_of_week, MAX(numPRs) \
        FROM tmp \
        GROUP BY sunday_of_week \
    ) \
"
sql_q4="\
    SELECT DATE( DATE(time_stamp) - interval '1 day'*EXTRACT(DOW FROM time_stamp)) AS sunday_of_week, \
    COUNT(*) AS numPRs \
    FROM pullreq_event \
    WHERE ev = 'opened' \
    GROUP BY sunday_of_week \
"
sql_q5="\
    WITH \
    months AS \
    ( \
        SELECT DISTINCT EXTRACT(MONTH FROM time_stamp) AS date_month \
        FROM pullreq_event \
    ), \
    answers AS \
    ( \
        SELECT COUNT(*) AS numPRs, EXTRACT(MONTH FROM time_stamp) AS date_month \
        FROM pullreq_event WHERE EXTRACT(YEAR FROM time_stamp)='2010' \
        AND ev='merged' \
        GROUP BY date_month \
    ) \
    SELECT months.date_month, COALESCE(answers.numPRs,0) \
    FROM months \
    FULL OUTER JOIN answers \
    ON months.date_month=answers.date_month \
"
sql_q6="\
    Select COUNT(ev), CAST(time_stamp AS DATE) AS timestamp_date \
    FROM pullreq_event \
    GROUP BY timestamp_date \
    ORDER BY timestamp_date \
"
sql_q7="\
    SELECT COUNT(*) AS num_PRs, author FROM pullreq_event \
    WHERE ev='opened' AND EXTRACT(YEAR FROM time_stamp)='2011' \
    GROUP BY author \
    ORDER BY num_PRs DESC LIMIT 1 \
"

queries = [sql_q1_a, sql_q1_b, sql_q2, sql_q3, sql_q4, sql_q5, sql_q6, sql_q7]
names = ["1.a","1.b","2.","3.","4.","5.","6.","7."]

SHOW_RUNTIMES_BACK = True
if SHOW_RUNTIMES_BACK:
    runtimes = []
    for i in range(8):
        runtime = 0
        for j in range(3):
            start = time.time()
            
            df = spark.read \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://localhost:5432/bda") \
                .option("dbtable", "(" + queries[i] + ") AS results" ) \
                .option("user", user) \
                .option("password", password) \
                .option("driver", "org.postgresql.Driver") \
                .load()
            df.show()

            end = time.time()
            runtime += (end-start)
        runtimes.append(runtime/3)
    print(runtimes)





df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/bda") \
    .option("dbtable", "pullreq_event") \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "org.postgresql.Driver") \
    .load()
df.registerTempTable('pullreq_event')
df.printSchema()

SHOW_RUNTIMES_FRONT = False
if SHOW_RUNTIMES_FRONT:
    runtimes = []
    for i in range(8):
        runtime = 0
        for j in range(3):
            start = time.time()

            spark.sql(queries[i]).show()

            end = time.time()
            runtime += (end-start)
        runtimes.append(runtime/3)
    print(runtimes)

