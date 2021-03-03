import pyspark
import datetime
import pyspark.sql.functions as f
from dateutil.parser import parse
from pyspark.sql import SparkSession
from pyspark.sql import Row
sc = pyspark.SparkContext()
spark = spark = SparkSession.builder.getOrCreate()
import re


host_pattern = r'(^\S+\.[\S+\.]+\S+)\s'
ts_pattern = r".*\[\s?(\d+/\D+?/.*?)\]"
method_uri_protocol_pattern = r'\"(\S+)\s(\S+)\s*(\S*)\"'
status_pattern = r'\s(\d{3})\s'
content_size_pattern =  r'\s\d{3}\s(\d+)\s'
url_pattern = r'((https?):((//)|(\\\\))+([\w\d:#@%/;$()~_?\+-=\\\.&](#!)?)*)'
def parse_apache_log_line(line):
    _host = re.search(host_pattern,line)
    _datetime = re.search(ts_pattern,line)
    _method  =  re.search(method_uri_protocol_pattern,line)
    _url  =  re.search(url_pattern,line)
    _status  =  re.search(status_pattern,line)
    _content_size = re.search(content_size_pattern,line)

    return Row(
        host = _host.group(0) if _host else None, 
        datetime = parse(_datetime.group(1)[:11] + " " + _datetime.group(1)[12:]) if _datetime else None, 
        method  = _method.group(1) if _method else None, 
        endpoint  = _method.group(2) if _method else None, 
        protocol  = _method.group(3) if _method else None,
        url  = _url.group(0) if _url else None, 
        status  = _status.group(1) if _status else None, 
        content_size = _content_size.group(1) if _content_size else None 
    )



rdd_logs = sc.textFile("D:\\access.log")
new_rdd =rdd_logs.map(parse_apache_log_line)
new_df = new_rdd.toDF()

numberOfRequests_PerHost = new_df.groupBy('host').count().alias('number of requests')
AvgDuration_PerHost = new_df.groupBy('host').agg(f.avg('datetime').alias('Avg Duration'))
AvgDuration_PerHost.show(5)

new_df.coalesce(1).write.format('csv').option("header", "true").save('D://parsed_log.csv')
numberOfRequests_PerHost.coalesce(1).write.format('csv').option("header", "true").save('D://numberOfRequests_PerHost.csv')
AvgDuration_PerHost.coalesce(1).write.format('csv').option("header", "true").save('D://AvgDuration_PerHost.csv')
