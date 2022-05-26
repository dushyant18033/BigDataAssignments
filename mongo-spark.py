# import datetime
from pyspark.sql import *
import time


spark = SparkSession\
    .builder\
    .appName("PySpark MongoDB") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/bda.pullreq_event") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/bda.pullreq_event") \
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12-3.0.1') \
    .config("spark.local.dir", "C:/tmp") \
    .config("spark.executor.instances", "1") \
    .getOrCreate()




q1_a = [
        { '$match':
            {'ev':'opened'} 
        },
        { '$group':
            {
                '_id': { '$dateToString': { 'format': '%Y-%m-%d', 'date': '$time_stamp', 'timezone': '+05:30'} },
                'count': { '$sum':1 }
            }
        },
        { '$sort': 
            { '_id':1} 
        }
    ]

q1_b = [
        { '$match':
            {'ev':'discussed'}
        },
        { '$group':
            { 
                '_id': { '$dateToString': { 'format': '%Y-%m-%d', 'date': '$time_stamp', 'timezone': '+05:30'} },
                'pullreqs': { '$addToSet': '$pull_requestid'} 
            }
        },
        { '$unwind': 
            "$pullreqs"
        },
        { '$group':
            {
                '_id': '$_id',
                'count': { '$sum':1} 
            }
        },
        { '$sort':
            { '_id':1}
        }
    ]

q2 = [
        { '$match':
            {'ev': 'discussed'}
        },
        { '$group':
            {
                '_id': { 
                    'Month': {
                    '$dateToString': { 'format': '%m', 'date':'$time_stamp', 'timezone': '+0530'}
                    },
                    'author': '$author'
                },
                'numPRs': {'$sum': 1 },
            }
        },
        { '$sort': 
            { 'numPRs': -1} 
        },
        { '$group':
            {
                '_id': '$_id.Month',
                'MaxNumPRs': { '$first': '$numPRs'}, 
                'author': { '$first': '$_id.author' }
            }
        },
        { '$sort': 
            { '_id':1} 
        }
    ]

q3 = [
        { '$match':
            {'ev': 'discussed'}
        },
        { '$project':
            {
                'DayOfWeek': {'$dayOfWeek':
                    {
                    'date': '$time_stamp',
                    'timezone': '+0530'
                    }
                },
                'time_stamp' : {'$add': ['$time_stamp', 19800000]},
                'ev': '$ev',
                'pull_requestid': '$pull_requestid',
                'author': '$author'
            }
        },
        { '$project':
            {
                'sundayOfWeek': {'$subtract': ['$time_stamp', { '$multiply': [ {'$subtract': ['$DayOfWeek',1]}, 86400000 ] }] },
                'ev': '$ev',
                'pull_requestid': '$pull_requestid',
                'author': '$author'
            }
        },
        { '$group':
            {
                '_id': { 
                    'sundayOfWeek': { '$dateToString': { 'format': '%Y-%m-%d', 'date':'$sundayOfWeek'} },
                    'author': '$author'
                },
                'numPRs': {'$sum': 1 },
            }
        },
        { '$sort':
            { 'numPRs': -1}
        },
        { '$group':
            {
                '_id': '$_id.sundayOfWeek',
                'MaxNumPRs': { '$first': '$numPRs'}, 
                'author': { '$first': '$_id.author' }
            }
        },
        { '$sort':
            { '_id': 1}
        }
    ]

q4 = [
        { '$project':
            {
                'DayOfWeek': {'$dayOfWeek':
                    {
                    'date': '$time_stamp',
                    'timezone': '+0530'
                    }
                },
                'time_stamp' : {'$add': ['$time_stamp', 19800000]},
                'ev': '$ev',
                'pull_requestid': '$pull_requestid',
                'author': '$author'
            }
        },
        { '$project':
            {
                'sundayOfWeek': {'$subtract': ['$time_stamp', { '$multiply': [ {'$subtract': ['$DayOfWeek',1]}, 86400000 ] }] },
                'ev': '$ev',
                'pull_requestid': '$pull_requestid',
                'author': '$author'
            }
        },
        { '$match':
            {
                'ev':'opened'
            }
        },
        { '$group':
            {
                '_id': { '$dateToString': { 'format': '%Y-%m-%d', 'date':'$sundayOfWeek'} },
                'count': {'$sum': 1 }
            }
        },
        { '$sort':
            {
               '_id': 1
            }
        }
   ]

q5 = [
        { '$project':
            {
                'year': { '$year': '$time_stamp' },
                'time_stamp': '$time_stamp',
                'ev': '$ev',
                'pull_requestid': '$pull_requestid',
                'author': '$author'
            }
        },
        { '$match':
            { 'ev':'merged', 'year':2010 }
        },
        { '$group':
            {
                '_id': {
                '$dateToString': { 'format': '%m', 'date':'$time_stamp', 'timezone': '+0530'}
                },
                'numPRs': {'$sum': 1 },
            }
        },
        { '$sort':
            { '_id': 1 }
        }
    ]

q6 = [
        { '$project':
            {
                'time_stamp': '$time_stamp',
                'ev': '$ev',
                'pull_requestid': '$pull_requestid',
                'author': '$author'
            }
        },
        { '$group':
            {
                '_id': { '$dateToString': { 'format': '%Y-%m-%d', 'date': '$time_stamp', 'timezone': '+05:30'} },
                'count': { '$sum':1 }
            }
        },
        { '$sort':
            {
                '_id': 1
            }
        },
    ]

q7 = [
        { '$project':
            {
                'year': { '$year': '$time_stamp' },
                'time_stamp': '$time_stamp',
                'ev': '$ev',
                'pull_requestid': '$pull_requestid',
                'author': '$author'
            }
        },
        { '$match':
            {'ev':'opened', 'year':2011}
        },
        { '$group':
            {
                '_id': '$author',
                'count': { '$sum':1 }
            }
        },
        { '$sort':
            {
                'count': -1
            }
        },
        { '$limit':1 }
    ]

queries = [q1_a, q1_b, q2, q3, q4, q5, q6, q7]
names = ["1.a","1.b","2.","3.","4.","5.","6.","7."]

SHOW_RUNTIMES_BACK = True
if SHOW_RUNTIMES_BACK:
    runtimes = []
    for i in range(8):
        runtime = 0
        for j in range(3):
            start = time.time()

            spark.read.format("mongo").option("pipeline", queries[i]).load().show()

            end = time.time()
            runtime += (end-start)
        runtimes.append(runtime/3)
    print(runtimes)




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

df = spark.read.format('com.mongodb.spark.sql.DefaultSource').load()
df.registerTempTable('pullreq_event')

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

