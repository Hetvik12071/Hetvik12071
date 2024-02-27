Big Data Analytics [CN7031] CRWK 2023-24
Group ID: [41]

    Student 1: Hetal Goswami | u2496947
    Student 2: Ravi Chachpara | u2438051
    Student 3: Raxit Kevadiya | u2448292
    Student 4: Reema Mohammed | u2497808

Initiate and Configure Spark

!pip3 install pyspark

Collecting pyspark
  Downloading pyspark-3.5.0.tar.gz (316.9 MB)
     ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 316.9/316.9 MB 4.1 MB/s eta 0:00:00
  Preparing metadata (setup.py) ... done
Requirement already satisfied: py4j==0.10.9.7 in /usr/local/lib/python3.10/dist-packages (from pyspark) (0.10.9.7)
Building wheels for collected packages: pyspark
  Building wheel for pyspark (setup.py) ... done
  Created wheel for pyspark: filename=pyspark-3.5.0-py2.py3-none-any.whl size=317425345 sha256=c2658a96ce21aded916ebbbf277af06942e9e2e81f57d308dde3798aa6aa5e72
  Stored in directory: /root/.cache/pip/wheels/41/4e/10/c2cf2467f71c678cfc8a6b9ac9241e5e44a01940da8fbb17fc
Successfully built pyspark
Installing collected packages: pyspark
Successfully installed pyspark-3.5.0

# linking with Spark
import pyspark;
import pyspark.sql.functions as F;
from pyspark.sql import SparkSession;
from pyspark.sql.functions import col,when,count,sum,to_date;
from pyspark.sql.types import StringType,IntegerType,BooleanType;

import numpy as np;
import pandas as pd;
import matplotlib.pyplot as plt;

print(pyspark.__version__);

3.5.0

spark = SparkSession.builder.appName("SPK").getOrCreate();

Load Unstructured Data and Convert it to Spark DF [10 marks]

# mounting the googleDrive to colab in order to access data.
from google.colab import drive
drive.mount('/content/drive')

Mounted at /content/drive

# reading the web.log file located at Google Drive.
with open('/content/drive/MyDrive/BigData/web.log','r') as file:
    r = file.read();

# printing the content of the data.
r.split("\n")[:5]

['88.211.105.115 - - [04/Mar/2022:14:17:48] "POST /history/missions/ HTTP/2.0" 414 12456 Caution: System may require attention. Check logs for details.',
 '144.6.49.142 - - [02/Sep/2022:15:16:00] "POST /security/firewall/ HTTPS/1.0" 203 97126 Warning: Unusual behavior detected. Investigate further.',
 '231.70.64.145 - - [19/Jul/2022:01:31:31] "PUT /web-development/countdown/ HTTP/1.0" 201 33093 Informational message. No action required.',
 '219.42.234.172 - - [08/Feb/2022:11:34:57] "POST /networking/technology/ HTTP/1.0" 415 68827 Debug: Detailed system state information.',
 '183.173.185.94 - - [29/Aug/2023:03:07:11] "GET /security/firewall/ HTTP/2.0" 205 30374 Warning: Unusual behavior detected. Investigate further.']

# converting un-structured data to structured one using regular expresion.
import re;
pattern = r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) - - \[(\d{2}/[A-Za-z]{3}/\d{4}):(\d{2}:\d{2}:\d{2})\] "([A-Z]{0,8}) (\S+?) ([A-Z]+)/(\d.\d)" (\d{3}) (\d+) (.*)\.';
x = re.findall(pattern,r);

# printing the number of records present in the file
print(len(x));

3000000

# create appropriate heading for the data
heading = ["IP","DATE","TIME","METHOD","URL",
           "PROTOCOL","PROTOCOL_VERSION","STATUS","CODE","MESSAGE"];

# Convert the unstructrued data to Spark DF
import os;
import csv;

if not os.path.exists('/content/drive/MyDrive/BigData/Output.csv'):

    csv_file_path = '/content/drive/MyDrive/BigData/Output.csv';

    with open(csv_file_path,mode='w',newline='') as file:
        csvWriter = csv.writer(file);
        csvWriter.writerow(heading);
        csvWriter.writerows(x);

    print("CSV file is created at : " +csv_file_path);


df = spark.read.csv('/content/drive/MyDrive/BigData/Output.csv',
                    header = True,
                    inferSchema=True);

df.show();

+---------------+-----------+-------------------+------+--------------------+--------+----------------+------+-----+--------------------+
|             IP|       DATE|               TIME|METHOD|                 URL|PROTOCOL|PROTOCOL_VERSION|STATUS| CODE|             MESSAGE|
+---------------+-----------+-------------------+------+--------------------+--------+----------------+------+-----+--------------------+
| 88.211.105.115|04/Mar/2022|2023-12-17 14:17:48|  POST|  /history/missions/|    HTTP|             2.0|   414|12456|Caution: System m...|
|   144.6.49.142|02/Sep/2022|2023-12-17 15:16:00|  POST| /security/firewall/|   HTTPS|             1.0|   203|97126|Warning: Unusual ...|
|  231.70.64.145|19/Jul/2022|2023-12-17 01:31:31|   PUT|/web-development/...|    HTTP|             1.0|   201|33093|Informational mes...|
| 219.42.234.172|08/Feb/2022|2023-12-17 11:34:57|  POST|/networking/techn...|    HTTP|             1.0|   415|68827|Debug: Detailed s...|
| 183.173.185.94|29/Aug/2023|2023-12-17 03:07:11|   GET| /security/firewall/|    HTTP|             2.0|   205|30374|Warning: Unusual ...|
|   164.12.8.113|22/May/2023|2023-12-17 09:48:25|   GET|/web-development/...|    HTTP|             1.0|   200|14633|Informational mes...|
|   110.98.7.240|22/Jan/2023|2023-12-17 09:55:54|   PUT|    /history/apollo/|    HTTP|             2.0|   204|63819|Debug: Detailed s...|
| 27.182.196.243|28/Mar/2022|2023-12-17 05:37:59|   GET| /history/apollo-11/|   HTTPS|             1.0|   414|93885|Caution: System m...|
|  123.31.25.147|25/Feb/2023|2023-12-17 12:03:32|   GET|/data-analysis/mi...|    HTTP|             1.0|   204|75897|Developer Note: D...|
|  220.182.78.75|22/Dec/2022|2023-12-17 12:55:00|   GET|    /history/launch/|    HTTP|             2.0|   200|84446|Debugging informa...|
| 206.186.128.82|13/Oct/2023|2023-12-17 07:21:13|   GET|/security/apollo-11/|    HTTP|             1.0|   404|24520|Debugging informa...|
| 143.238.50.180|26/Oct/2023|2023-12-17 17:17:20|  POST|/data-analysis/mi...|   HTTPS|             1.0|   307|31361|Debugging informa...|
|   12.33.251.59|15/Aug/2022|2023-12-17 15:59:37|   GET|/web-development/...|    HTTP|             2.0|   203|37029|Warning: Unusual ...|
| 39.107.109.242|08/Oct/2023|2023-12-17 07:07:24|   PUT|      /shuttle/data/|   HTTPS|             1.0|   201|68708|Informational mes...|
| 250.231.144.68|22/Jun/2022|2023-12-17 19:17:24|  POST|/security/deep-le...|   HTTPS|             1.0|   500|69887|Caution: System m...|
| 103.105.160.60|24/Oct/2023|2023-12-17 01:54:56|   GET|/shuttle/cybersec...|    HTTP|             1.0|   416| 3924|Warning: Unusual ...|
|246.167.148.159|14/Oct/2022|2023-12-17 11:49:35|   GET|/security/deep-le...|    HTTP|             2.0|   500| 1685|Debug: Detailed s...|
| 185.221.50.185|25/Jan/2023|2023-12-17 02:47:50|  POST|/images/frontend-...|    HTTP|             2.0|   308|22650|FYI: System opera...|
| 144.143.171.58|30/Aug/2022|2023-12-17 08:04:05|   PUT|/software/apollo-11/|    HTTP|             2.0|   304|49029|Debugging informa...|
|  131.119.14.93|26/Jun/2022|2023-12-17 16:40:44|   GET| /networking/launch/|    HTTP|             1.0|   205| 8159|Potential issue d...|
+---------------+-----------+-------------------+------+--------------------+--------+----------------+------+-----+--------------------+
only showing top 20 rows

Task 1: Spark SQL [30 marks]

# Student 1> Hetal Goswami | u2496947
# Query 1> Calculate the percentage of successful requests (STATUS = 200) for each method

success_percentage = df.groupBy("METHOD") \
                       .agg((sum(when(df["STATUS"] == 200, 1).otherwise(0)) / count("*") * 100) \
                       .alias("Success Percentage"))
success_percentage.show();
success_percentage_db = success_percentage.toPandas();
success_percentage_db.plot(
    kind = 'bar',
    logy=True,
    x = 'METHOD',
    y = 'Success Percentage'
);
plt.title("Calculate the percentage of successful requests (STATUS = 200) for each method");
plt.xlabel("METHODs");
plt.ylabel("Percentage");
plt.show();

+------+------------------+
|METHOD|Success Percentage|
+------+------------------+
|  POST| 7.132098290363366|
|   PUT| 7.162687840777523|
|   GET| 7.184306768040933|
+------+------------------+

# Student 1> Hetal Goswami | u2496947
# Query 2> [Number of POST request percentage made using HTTP/HTTPS protocol]:
# which shows potential threat of data interception and data integraty by eavesdroppers.

result = df.groupBy("PROTOCOL") \
           .agg((sum(when(df["METHOD"] == "POST",1).otherwise(0))).alias("Result"));

result = result.withColumn('Result',col('Result') /
                           result.agg(sum(col('Result')).alias('Total')).select('Total').rdd.flatMap(lambda x:x).collect()[-1] * 100);
result.show();

result_db = result.toPandas();
result_db.plot(
    kind = 'pie',
    y = 'Result',
    labels = result_db["PROTOCOL"]
);

plt.title("HTTP/HTTPS -> POST access [percentage]");
plt.legend();
plt.show();

# Analysis :- Which shows majority of POST methods request uses HTTP protocol.
# Therefore, in order to enhance security, server should enforce HTTPS usage.

+--------+------------------+
|PROTOCOL|            Result|
+--------+------------------+
|   HTTPS|33.362152113182844|
|    HTTP| 66.63784788681716|
+--------+------------------+

# Student 2> Ravi Chachpara | u2438051
# Query 1> [Different URLs and its supported protocols]:
@F.udf(StringType())
def combineStrings(data):
    lst = [];
    for _ in data.split(","):
        if _ not in lst:
            lst.append(_);
    return "|".join(sorted(lst));

urls = df.groupBy("URL").agg(
    combineStrings(F.concat_ws(",",F.collect_list("PROTOCOL")))
    .alias("Supported Protocols")
);
urls.show();

# Analysis:- By seeing top-20 rows, it looks like urls supportes both Protols(HTTP/HTTPS).

+--------------------+-------------------+
|                 URL|Supported Protocols|
+--------------------+-------------------+
|/web-development/...|         HTTP|HTTPS|
|   /security/apollo/|         HTTP|HTTPS|
|      /history/data/|         HTTP|HTTPS|
|/history/technology/|         HTTP|HTTPS|
|/data-analysis/aw...|         HTTP|HTTPS|
|/cloud-computing/...|         HTTP|HTTPS|
|/shuttle/frontend...|         HTTP|HTTPS|
|    /history/launch/|         HTTP|HTTPS|
|/software/apollo-11/|         HTTP|HTTPS|
|/data-analysis/fr...|         HTTP|HTTPS|
|/networking/front...|         HTTP|HTTPS|
|   /security/launch/|         HTTP|HTTPS|
|/machine-learning...|         HTTP|HTTPS|
|/security/countdown/|         HTTP|HTTPS|
|     /security/data/|         HTTP|HTTPS|
|/networking/count...|         HTTP|HTTPS|
|/web-development/...|         HTTP|HTTPS|
|/networking/aws-c...|         HTTP|HTTPS|
|  /shuttle/missions/|         HTTP|HTTPS|
|      /shuttle/data/|         HTTP|HTTPS|
+--------------------+-------------------+
only showing top 20 rows

# Student 2> Ravi Chachpara | u2438051
# Query 2> [Particular hours where server is accessed by client]:
# In simple words calculating web traffic at server
@F.udf(IntegerType())
def convertTimeToInt(time):
    return int(str(time).split(" ")[1].split(':')[0]);

traffic = df.withColumn("TIME",convertTimeToInt(df['TIME'])) \
            .groupBy('TIME') \
            .count() \
            .orderBy("TIME");

traffic_pd = traffic.toPandas();
traffic_pd.plot(
    kind='line',
    x='TIME',
    y='count',
    marker='x',
    linestyle='-',
    color='b'
);

# Analysis :- it seems like 12 to 16 is peak hour for the server, during that
# time maximum requests have been made by the clients.

# Student 3> Raxit Kevadiya | u2448292
# Query 1 server access pattern during most busy day for the server:

# finding the most busy day for the server.
most_busy_day = df.groupBy('DATE') \
                  .count() \
                  .orderBy('count',ascending=False) \
                  .limit(1) \
                  .select('DATE').rdd.flatMap(lambda x:x).collect()[-1];

print(f'Most busy day for the server : {most_busy_day}');
# result :- '09/Sep/2022'

# finding access pattern at '09/Sep/2022'.
@F.udf(IntegerType())
def extractHourFromTime(time):
    return int(str(time).split(' ')[-1].split(':')[0]);

result = df.filter(df["DATE"] == '09/Sep/2022') \
           .withColumn("TIME",extractHourFromTime(df["TIME"])) \
           .groupBy("TIME") \
           .count() \
           .orderBy("TIME") ;
result.show();
result_db = result.toPandas();
result_db.plot(
    kind='line',
    x='TIME',
    y='count',
    marker='o',
    color='r'
);

# Analysis :- after seeing the access pattern it seems like
# server remained most busy from 14:00 (2 PM) to 17:00 (5 PM).

Most busy day for the server : 09/Sep/2022
+----+-----+
|TIME|count|
+----+-----+
|   0|  186|
|   1|  176|
|   2|  188|
|   3|  193|
|   4|  197|
|   5|  172|
|   6|  194|
|   7|  176|
|   8|  184|
|   9|  177|
|  10|  193|
|  11|  211|
|  12|  195|
|  13|  187|
|  14|  188|
|  15|  212|
|  16|  196|
|  17|  206|
|  18|  201|
|  19|  175|
+----+-----+
only showing top 20 rows

# Student 3> Raxit Kevadiya | u2448292
# Query 2 [URLs and its supported protocols]:
# Find url which has 200 status with https protocol between 1/1/2022 to 1/7/2022

@F.udf(StringType())
def convertToDate(date):
    month_index = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'];
    day,month,year = date.split("/");
    month = str(month_index.index(month.lower()) + 1);

    day = day.zfill(2);
    month = month.zfill(2);

    return "-".join([year,month,day]);

result = df.filter(df["STATUS"] == 200) \
           .withColumn("DATE",convertToDate(df["DATE"])) \
           .withColumn("DATE",to_date(col("DATE"))) \
           .filter(
               (col("DATE") <= '2022-07-01') &
               (col("DATE") >= '2022-01-01')
            ) \
           .filter(df["PROTOCOL"] == "HTTPS") \
           .select("URL");

result.show();

+--------------------+
|                 URL|
+--------------------+
| /security/firewall/|
|/history/deep-lea...|
|    /shuttle/apollo/|
|/security/aws-cer...|
|     /software/data/|
| /software/firewall/|
|/cloud-computing/...|
|/data-analysis/aw...|
|/web-development/...|
|  /history/firewall/|
|/data-analysis/mi...|
|/cloud-computing/...|
| /software/firewall/|
|/security/fronten...|
|/images/aws-certi...|
|   /networking/data/|
|/software/technol...|
|/data-analysis/aw...|
|/networking/deep-...|
|/networking/front...|
+--------------------+
only showing top 20 rows

# Student 4> Reema Mohammed | u2497808
# Query 1 [Server Access during the 2022]:
@F.udf(StringType())
def extractYear(date):
    return str(date).split('/')[2];

@F.udf(IntegerType())
def extractMonth(date):
    li = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'];
    return li.index(str(date).lower().split('/')[1]);

access = df.withColumn('YEAR',extractYear(df['DATE']));
access = access.filter(col('YEAR') == '2022');
access = access.withColumn('MONTH',extractMonth(df['DATE']));
access = access.groupBy(col('MONTH')).count().orderBy(col('MONTH'));

access.show();

access_pd = access.toPandas();
access_pd.plot(
    kind = 'line',
    x = 'MONTH',
    y = 'count',
    marker = 'x',
    linestyle = '-',
    color = 'b'
);

+-----+------+
|MONTH| count|
+-----+------+
|    0|136602|
|    1|123133|
|    2|135878|
|    3|132298|
|    4|136718|
|    5|131962|
|    6|136486|
|    7|135991|
|    8|132470|
|    9|136734|
|   10|132001|
|   11|136407|
+-----+------+

# Student 4> Reema Mohammed | u2497808
# Query 2 [Counting status code occurance]:

status_code = df.groupBy('STATUS').count().orderBy('STATUS');
status_code.show();

+------+------+
|STATUS| count|
+------+------+
|   200|214791|
|   201|214586|
|   202|214738|
|   203|214133|
|   204|213491|
|   205|214455|
|   304|213366|
|   307|213800|
|   308|214515|
|   404|214584|
|   414|214705|
|   415|214135|
|   416|214174|
|   500|214527|
+------+------+

Task 2 - Spark RDD [50 marks]

csv_rdd = spark.sparkContext.textFile("/content/drive/MyDrive/BigData/Output.csv");
parsed_rdd = csv_rdd.map(lambda line:line.split(','));
rdd = parsed_rdd.zipWithIndex().filter(
    lambda row:row[1] > 0
).map(
    lambda row:row[0]
);

print(rdd.take(5));

[['88.211.105.115', '04/Mar/2022', '14:17:48', 'POST', '/history/missions/', 'HTTP', '2.0', '414', '12456', 'Caution: System may require attention. Check logs for details'], ['144.6.49.142', '02/Sep/2022', '15:16:00', 'POST', '/security/firewall/', 'HTTPS', '1.0', '203', '97126', 'Warning: Unusual behavior detected. Investigate further'], ['231.70.64.145', '19/Jul/2022', '01:31:31', 'PUT', '/web-development/countdown/', 'HTTP', '1.0', '201', '33093', 'Informational message. No action required'], ['219.42.234.172', '08/Feb/2022', '11:34:57', 'POST', '/networking/technology/', 'HTTP', '1.0', '415', '68827', 'Debug: Detailed system state information'], ['183.173.185.94', '29/Aug/2023', '03:07:11', 'GET', '/security/firewall/', 'HTTP', '2.0', '205', '30374', 'Warning: Unusual behavior detected. Investigate further']]

heading_index = {var:index for index,var in enumerate(parsed_rdd.first())};
print(heading_index);

{'IP': 0, 'DATE': 1, 'TIME': 2, 'METHOD': 3, 'URL': 4, 'PROTOCOL': 5, 'PROTOCOL_VERSION': 6, 'STATUS': 7, 'CODE': 8, 'MESSAGE': 9}

# Student 1> Hetal Goswami | u2496947
# -----------------------------------------
# Analysis :-1 This querie is for visualize the distribution of access counts between the years 2022 and 2023.

# -----------------------------------------

result = rdd.map(
    lambda row : (row[heading_index['DATE']].split('/')[-1],1)
).reduceByKey(
    lambda x,y : x+y
).sortBy(
    lambda row: row[0]
).collect();

# The 'map' transformation is extracting the year part from the 'DATE' column for each row.
# 'reduceByKey' is aggregating the count of access for each year.
# 'sortBy' is sorting the result by year.
# 'collect' is retrieving the result.

for _ in result:
    print(_);

# Separate the year and count for plotting
cat = [_[0] for _ in result];
val = [_[1] for _ in result];

# Plotting a pie chart
plt.pie(val,labels=cat);
plt.legend();
plt.show();

('2022', 1606680)
('2023', 1393320)

# Student 1> Hetal Goswami | u2496947
# counting the access to url ['/history/missions/'] using differnt IPs.
# -----------------------------------------
# Analysis :- 2  If we have data about IPs belong to which particular geographic
# region then in order to reduce the response time we can allocate new server
# to that particular region.
# -----------------------------------------

# Filter rows where the URL is '/history/missions/'
url_hit = rdd.filter(
    lambda row: row[heading_index['URL']] == '/history/missions/'
).map(
    lambda row : (row[heading_index['IP']],1)

# Reduce by key (IP) to count the number of accesses from each IP
).reduceByKey(
    lambda x,y : x+y
).sortBy(
    lambda row : row[1],ascending=False
);

# Display the top 50 results
for _ in url_hit.take(50):
    print(_);

('244.79.131.72', 1)
('48.130.56.42', 1)
('143.214.179.74', 1)
('171.2.203.8', 1)
('207.219.99.192', 1)
('141.79.140.106', 1)
('84.185.13.166', 1)
('46.167.121.228', 1)
('199.158.242.136', 1)
('35.105.133.142', 1)
('126.12.124.1', 1)
('72.49.174.144', 1)
('210.157.209.187', 1)
('144.138.99.5', 1)
('191.102.143.228', 1)
('1.242.177.104', 1)
('85.187.104.140', 1)
('170.117.241.52', 1)
('107.143.39.153', 1)
('46.234.48.170', 1)
('236.48.162.219', 1)
('212.131.70.160', 1)
('143.5.24.228', 1)
('25.45.230.93', 1)
('158.196.132.132', 1)
('101.32.75.168', 1)
('22.164.64.117', 1)
('5.48.38.191', 1)
('143.24.177.23', 1)
('136.152.107.87', 1)
('71.96.166.112', 1)
('241.199.216.81', 1)
('21.1.142.248', 1)
('221.37.72.169', 1)
('124.26.153.107', 1)
('119.83.188.136', 1)
('131.247.193.34', 1)
('159.114.130.102', 1)
('25.23.92.138', 1)
('39.227.38.230', 1)
('183.253.209.32', 1)
('223.26.32.64', 1)
('110.16.74.254', 1)
('163.230.145.245', 1)
('84.144.197.98', 1)
('63.72.44.86', 1)
('207.177.95.208', 1)
('63.156.252.147', 1)
('44.190.205.248', 1)
('203.233.209.243', 1)

# Student 1> Hetal Goswami | u2496947
# Analysis :- 3
# Access difference between days and nights:

def extractTime(time):
    hour = int(time.split(':')[0]);
    return 'NIGHT' if (6 <= hour and hour <= 18) else 'DAY'  ;

# This function takes a time string as input, extracts the hour from it,
# and check whether it's day or night based on the hour.
# If the hour is between 6 and 18 , it returns 'NIGHT'; otherwise, it returns 'DAY'.

access_diff = rdd.map(
    lambda row:( extractTime((row[heading_index['TIME']])) ,1 )
).reduceByKey(
    lambda x,y: x+y
).collect();

print(access_diff);

plt.pie([ _[1] for _ in access_diff ],labels=[_[0] for _ in access_diff],shadow=True);
plt.legend();
plt.show();

[('DAY', 1374841), ('NIGHT', 1625159)]

# Student 2> Ravi Chachpara | u2438051
# analysis 1 and result using RDD operators:
# Successful and Unsucessful attempt to server

def convertCodeToString(code):
    return "success" if str(code).startswith("2") else "unsuccess";

def isSuccessfulOrUnsuccessful(code):
    code = str(code);
    return code.startswith('2') or code.startswith('4');

attempt = rdd.filter(
    lambda row: isSuccessfulOrUnsuccessful(row[heading_index['CODE']])
).map(
    lambda row : (convertCodeToString(row[heading_index["CODE"]]),1)
).reduceByKey(
    lambda x,y : x+y
).collect();

print(attempt)

plt.pie([_[1] for _ in attempt],labels=[_[0] for _ in attempt],shadow=True);
plt.title("successful and unsuccessful attempt to server");
plt.legend();
plt.show();

[('success', 332843), ('unsuccess', 333353)]

# Student 2> Ravi Chachpara | u2438051
# analysis 2 and result using RDD operators:
# TOP-5 urls having most warning messages.

def isWarningMessage(message):
    return "warning" in message.lower();

most_urgent_need_to_fix_url = rdd.filter(
    lambda row : isWarningMessage(row[heading_index['MESSAGE']])
).map(
    lambda row : (row[heading_index['URL']],1)
).reduceByKey(
    lambda x,y : x+y
).sortBy(
    lambda row : row[1], ascending=False
).take(5);

for _ in most_urgent_need_to_fix_url:
    print(_);

('/software/data/', 2996)
('/networking/missions/', 2897)
('/history/aws-certification/', 2890)
('/cloud-computing/launch/', 2880)
('/data-analysis/apollo/', 2879)

# Student 2> Ravi Chachpara | u2438051
# analysis 3 and result using RDD operators:
# most busy days for the server

busy_days = rdd.map(
    lambda row : (row[heading_index['DATE']],1)
).reduceByKey(
    lambda x,y: x+y
).sortBy(
    lambda row: row[1], ascending=False
).take(5);

print(busy_days)

cat = [_[0] for _ in busy_days];
val = [_[1] for _ in busy_days];

fig,ax = plt.subplots();
bar = ax.bar(cat,val);
ax.set_ylabel('hit');
ax.set_title('most busy Day for the server');
plt.show();

[('09/Sep/2022', 4604), ('11/Oct/2022', 4601), ('09/Jul/2022', 4592), ('06/May/2023', 4592), ('23/Jan/2022', 4581)]

# Student 3> Raxit Kevadiya | u2448292
# analysis 1 and result using RDD operators:
# different root urls and how many times they accessed.


root_urls_accessed = rdd.map(
    lambda row : (row[heading_index['URL']].split('/')[1],1)
).reduceByKey(
    lambda x,y : x+y
).sortBy(
    lambda row : row[1], ascending=False
).collect();

print(root_urls_accessed)

cat = [_[0] for _ in root_urls_accessed];
val = [_[1] for _ in root_urls_accessed];

plt.barh(cat,val);
plt.xlabel('access count');
plt.ylabel('urls');
plt.title('root urls --> access count');

plt.show();

[('software', 300815), ('history', 300732), ('data-analysis', 300555), ('networking', 300534), ('security', 299997), ('web-development', 299691), ('cloud-computing', 299690), ('shuttle', 299611), ('images', 299249), ('machine-learning', 299126)]

# Student 3> Raxit Kevadiya | u2448292
# analysis 2 and result using RDD operators:
# Which root-urls have most Potential issue;

root_urls_having_potential_issues = rdd.filter(
    lambda row :  "potential issue" in row[heading_index['MESSAGE']].lower()
).map(
    lambda row : (row[heading_index['URL']].split("/")[1],1)
).reduceByKey(
    lambda x,y : x+y
).sortBy(
    lambda row : row[1],ascending=False
).collect();

cat = [_[0] for _ in root_urls_having_potential_issues];
val = [_[1] for _ in root_urls_having_potential_issues];

plt.barh(cat,val);
plt.xlabel('Having number of potential issues');
plt.ylabel('urls');
plt.title('root urls --> access count');

plt.show();

# Student 3> Raxit Kevadiya | u2448292
# analysis 3 and result using RDD operators:
# most accessed url on particular day;

most_accessed_url = rdd.map(
    lambda row : ((row[heading_index['DATE']],row[heading_index['URL']]),1)
).reduceByKey(
    lambda x,y : x+y
).sortBy(
    lambda row : row[1],ascending=False
).map(
    lambda row : (row[0][0],row[0][1],row[1])
).take(5);

print(most_accessed_url);

cat = [_[0] + " - " + _[1] for _ in most_accessed_url];
val = [_[2] for _ in most_accessed_url];



plt.barh(cat,val);
plt.xlabel('Access count');
plt.ylabel('urls');
plt.title('root urls --> access count');

plt.show();

[('25/Apr/2023', '/networking/countdown/', 67), ('09/Nov/2022', '/cloud-computing/cybersecurity/', 66), ('05/Oct/2022', '/cloud-computing/cybersecurity/', 65), ('16/May/2023', '/software/missions/', 64), ('20/Aug/2023', '/web-development/cybersecurity/', 64)]

# Student 4> Reema Mohammed | u2497808
# analysis 1 and result using RDD operators:
# printing all the urls that sever can't able to find; throwing 404 Error

url_not_found = rdd.filter(
    lambda row : row[heading_index['STATUS']] == '404'
).map(
    lambda row : row[heading_index['URL']]
).take(50);

for _ in url_not_found:
    print(_);

/security/apollo-11/
/images/deep-learning/
/security/missions/
/images/missions/
/networking/aws-certification/
/shuttle/apollo-11/
/cloud-computing/launch/
/images/countdown/
/shuttle/frontend-frameworks-comparison/
/web-development/cybersecurity/
/machine-learning/missions/
/history/launch/
/shuttle/apollo/
/history/cybersecurity/
/machine-learning/data/
/machine-learning/data/
/cloud-computing/technology/
/cloud-computing/firewall/
/machine-learning/launch/
/security/missions/
/images/data/
/machine-learning/deep-learning/
/cloud-computing/countdown/
/software/frontend-frameworks-comparison/
/data-analysis/cybersecurity/
/software/launch/
/machine-learning/cybersecurity/
/data-analysis/apollo-11/
/images/apollo/
/security/apollo-11/
/networking/technology/
/data-analysis/aws-certification/
/images/apollo/
/machine-learning/data/
/data-analysis/countdown/
/images/frontend-frameworks-comparison/
/networking/launch/
/web-development/missions/
/shuttle/apollo-11/
/web-development/aws-certification/
/shuttle/deep-learning/
/software/deep-learning/
/shuttle/missions/
/machine-learning/cybersecurity/
/security/data/
/history/aws-certification/
/software/apollo/
/cloud-computing/countdown/
/networking/technology/
/data-analysis/deep-learning/

# Student 4> Reema Mohammed | u2497808
# analysis 2 and result using RDD operators:
# IPs -> accessed urls

accessed_urls = rdd.map(
    lambda row : (row[heading_index['IP']],row[heading_index['URL']])
).reduceByKey(
    lambda x,y: x+", "+y
).take(20);

for _ in accessed_urls:
    print(_)

('110.98.7.240', '/history/apollo/')
('8.15.149.55', '/software/aws-certification/')
('163.218.146.10', '/web-development/apollo/')
('20.172.131.244', '/images/frontend-frameworks-comparison/')
('134.64.105.162', '/security/missions/')
('36.247.236.96', '/history/technology/')
('161.70.227.133', '/cloud-computing/cybersecurity/')
('216.165.77.205', '/software/missions/')
('53.211.7.217', '/history/firewall/')
('13.142.178.254', '/shuttle/deep-learning/')
('104.170.4.65', '/security/data/')
('6.216.5.221', '/web-development/technology/')
('50.7.208.241', '/images/aws-certification/')
('61.70.70.239', '/images/launch/')
('163.240.84.123', '/cloud-computing/apollo/')
('13.107.203.101', '/images/data/')
('183.37.242.139', '/images/launch/')
('192.251.166.112', '/networking/aws-certification/')
('107.43.227.16', '/machine-learning/frontend-frameworks-comparison/')
('97.4.7.159', '/machine-learning/data/')

# Student 4> Reema Mohammed | u2497808
# analysis 3 and result using RDD operators:
# proportion of protocol & protocol version usage.

result = rdd.map(
    lambda row : (row[heading_index['PROTOCOL']] + " "\
    + row[heading_index['PROTOCOL_VERSION']],1)
).reduceByKey(
    lambda x,y : x+y
).collect();


for _ in result:
    print(_)

plt.pie([_[1] for _ in result],labels=[_[0] for _ in result],shadow=True);
plt.legend();
plt.show();

('HTTPS 1.0', 999348)
('HTTP 2.0', 1000568)
('HTTP 1.0', 1000084)

Task 3 - LSEP (legal, social, ethical, and professional) considerations [5 marks]

For all analyses performed, critically analyze the legal, social, ethical, and professional implications associated with the data and the analysis. Consider factors such as data privacy,data protection, bias, fairness, transparency, and the potential impact of the analysis on individuals or society as a whole.

Each student should take one of these factors as their contribution.

As a team, discuss and share your individual analyses and LSEP considerations with each other. Learn from each other's perspectives and insights.
Student 1: Potential Impact

In this assignment, we have conducted a range of queries which enable us to make data-driven decisions, ultimately benefical for the organization. For example, one of our query find number of http requests vs https requests. And, in that we find out that proportion of http is two times more than what https stood for. Which is concerning, because it makes user transaction vulnerable to different attacks such as, spoofing, and snooping. Another query provide insight into the peak usage of the server; means maximum requests come during that time. Therefore, if we know this fact before then during that time we can run more than one instance of the server in order to reduce the work-load of the one CPU.
Student 2: Data Privacy & Protection

[web.log] is readily available on the kaggle website under the CC0 1.0 Universal public Domain Dedication. In addition to this, all the tools and software that we used during this project, which includes, python packages (numpy, pandas, matplotlib, pyspark), google drive (for data storage), colab (for data-processing), are all either open source or free to use. Thus, we haven't breached any legal consideration.

Talking about data protection, Data is previously available in the unstructured format. However, we converted into the structured format using regular expression and stored it inside .csv file in Google Drive. Access to that .csv file is restricted to author only and others who has assigned privilaged by the author. Lastly, tools that we used in our project are well-checked and consistantly used by working professionals. Therefore, in terms of data safety and consistancy, data is secured.
Student 3: Fairness & transparancy

The dataset contains server logs, and it is well understood that log are created to provide a deeper understanding of the system and its status in order to resolve bugs. In some cases, logs are proven to be safegard for the data-recovery. Consequently, we can affirm that the data is logged impartially, with no bias present.

Additionally, this document will include the methods employed for data collection, pre-processing and analysis, providing evidence that all the techniques and methodologies used in the processing are legitimate and free from bias.
Student 4: Professional codes of conduct

During this project, we adhere to the ethical guidelines and the code of conduct proposed by the relevent professional organizations or industrial standard. In addition to this, throught out this project, we got the knowledge about the one of the demanding and emerging technology. During the whole process, we ensure that each individual worked in this project involved in the analysis and possess the necessay skills and competence for data analysis and data handling.
Convert ipynb to HTML for Turnitin submission [5 marks]

# install nbconvert
!pip3 install nbconvert


# convert ipynb to html
# file name: "Group_ID_41_CN7031.ipynb
!jupyter nbconvert --to html '/content/drive/MyDrive/Colab Notebooks/Group41_CN7031.ipynb';

Requirement already satisfied: nbconvert in /usr/local/lib/python3.10/dist-packages (6.5.4)
Requirement already satisfied: lxml in /usr/local/lib/python3.10/dist-packages (from nbconvert) (4.9.3)
Requirement already satisfied: beautifulsoup4 in /usr/local/lib/python3.10/dist-packages (from nbconvert) (4.11.2)
Requirement already satisfied: bleach in /usr/local/lib/python3.10/dist-packages (from nbconvert) (6.1.0)
Requirement already satisfied: defusedxml in /usr/local/lib/python3.10/dist-packages (from nbconvert) (0.7.1)
Requirement already satisfied: entrypoints>=0.2.2 in /usr/local/lib/python3.10/dist-packages (from nbconvert) (0.4)
Requirement already satisfied: jinja2>=3.0 in /usr/local/lib/python3.10/dist-packages (from nbconvert) (3.1.2)
Requirement already satisfied: jupyter-core>=4.7 in /usr/local/lib/python3.10/dist-packages (from nbconvert) (5.5.0)
Requirement already satisfied: jupyterlab-pygments in /usr/local/lib/python3.10/dist-packages (from nbconvert) (0.3.0)
Requirement already satisfied: MarkupSafe>=2.0 in /usr/local/lib/python3.10/dist-packages (from nbconvert) (2.1.3)
Requirement already satisfied: mistune<2,>=0.8.1 in /usr/local/lib/python3.10/dist-packages (from nbconvert) (0.8.4)
Requirement already satisfied: nbclient>=0.5.0 in /usr/local/lib/python3.10/dist-packages (from nbconvert) (0.9.0)
Requirement already satisfied: nbformat>=5.1 in /usr/local/lib/python3.10/dist-packages (from nbconvert) (5.9.2)
Requirement already satisfied: packaging in /usr/local/lib/python3.10/dist-packages (from nbconvert) (23.2)
Requirement already satisfied: pandocfilters>=1.4.1 in /usr/local/lib/python3.10/dist-packages (from nbconvert) (1.5.0)
Requirement already satisfied: pygments>=2.4.1 in /usr/local/lib/python3.10/dist-packages (from nbconvert) (2.16.1)
Requirement already satisfied: tinycss2 in /usr/local/lib/python3.10/dist-packages (from nbconvert) (1.2.1)
Requirement already satisfied: traitlets>=5.0 in /usr/local/lib/python3.10/dist-packages (from nbconvert) (5.7.1)
Requirement already satisfied: platformdirs>=2.5 in /usr/local/lib/python3.10/dist-packages (from jupyter-core>=4.7->nbconvert) (4.1.0)
Requirement already satisfied: jupyter-client>=6.1.12 in /usr/local/lib/python3.10/dist-packages (from nbclient>=0.5.0->nbconvert) (6.1.12)
Requirement already satisfied: fastjsonschema in /usr/local/lib/python3.10/dist-packages (from nbformat>=5.1->nbconvert) (2.19.0)
Requirement already satisfied: jsonschema>=2.6 in /usr/local/lib/python3.10/dist-packages (from nbformat>=5.1->nbconvert) (4.19.2)
Requirement already satisfied: soupsieve>1.2 in /usr/local/lib/python3.10/dist-packages (from beautifulsoup4->nbconvert) (2.5)
Requirement already satisfied: six>=1.9.0 in /usr/local/lib/python3.10/dist-packages (from bleach->nbconvert) (1.16.0)
Requirement already satisfied: webencodings in /usr/local/lib/python3.10/dist-packages (from bleach->nbconvert) (0.5.1)
Requirement already satisfied: attrs>=22.2.0 in /usr/local/lib/python3.10/dist-packages (from jsonschema>=2.6->nbformat>=5.1->nbconvert) (23.1.0)
Requirement already satisfied: jsonschema-specifications>=2023.03.6 in /usr/local/lib/python3.10/dist-packages (from jsonschema>=2.6->nbformat>=5.1->nbconvert) (2023.11.2)
Requirement already satisfied: referencing>=0.28.4 in /usr/local/lib/python3.10/dist-packages (from jsonschema>=2.6->nbformat>=5.1->nbconvert) (0.32.0)
Requirement already satisfied: rpds-py>=0.7.1 in /usr/local/lib/python3.10/dist-packages (from jsonschema>=2.6->nbformat>=5.1->nbconvert) (0.13.2)
Requirement already satisfied: pyzmq>=13 in /usr/local/lib/python3.10/dist-packages (from jupyter-client>=6.1.12->nbclient>=0.5.0->nbconvert) (23.2.1)
Requirement already satisfied: python-dateutil>=2.1 in /usr/local/lib/python3.10/dist-packages (from jupyter-client>=6.1.12->nbclient>=0.5.0->nbconvert) (2.8.2)
Requirement already satisfied: tornado>=4.1 in /usr/local/lib/python3.10/dist-packages (from jupyter-client>=6.1.12->nbclient>=0.5.0->nbconvert) (6.3.2)
[NbConvertApp] Converting notebook /content/drive/MyDrive/Colab Notebooks/Group41_CN7031.ipynb to html
[NbConvertApp] Writing 1118986 bytes to /content/drive/MyDrive/Colab Notebooks/Group41_CN7031.html

