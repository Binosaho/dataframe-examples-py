from sql.pyspark import SparkSession , Row
from distutils.util import strtobool
import os.path
import yaml

if __name__ == "__main__":
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
      )

    spark = SparkSession \
        .builder \
        .appName("RDD examples")\
        .master('local[*]')\
        .getOrCreate()

    spark.sparkcontext.setLogLevel("ERROR")

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../" + "application.yml")
    secret_config_path = os.path.abspath(current_dir + "/../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader = yaml.FullLoader)
    secret = open(secret_config_path)
    secret_conf = yaml.load(secret, Loader = yaml.FullLoader)

    hadoop_conf = spark.sparkcontext._jcs.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", secret_conf["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", secret_conf["s3_conf"]["secret_access_key"])

    demographics_rdd = spark.sparkcontext.textFile("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/demographic.csv" )
    finances_rdd = spark.sparkcontext.textFile("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/finances.csv")

    demographics_pair_rdd = demographics_rdd \
        .map(lambda line: line.split(",")) \
        .map(lambda lst: (int(lst[0]), int(lst[1]), strtobool(lst[2]), lst[3], lst[4], strtobool(lst[5]), strtobool(lst[6]), int(lst[7])))

    finances_pair_rdd = finances_rdd \
        .map(lambda line: line.split(",")) \
        .map(lambda lst: (int(lst[0]), strtobool(lst[1]), strtobool(lst[2]), strtobool(lst[3]), int(lst[4])))

    join_pair_rdd = demographics_pair_rdd.cartesian(finances_pair_rdd)\
        .filter(lambda rec: rec[0][0] == rec[1][0])  \
        .filter(lambda rec: (rec[0][3] == "Switzerland") and (rec[1][1]) and (rec[1][2]))

    join_pair_rdd.foreach(print)

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" Exercises/scholaship_recipient_cartesian_filter.py