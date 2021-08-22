from pyspark.sql import SparkSession
from pyspark.sql.function import explode, col
import os.path
import yaml

if __name__ == "__main__":
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    spark = SparkSession \
        .builder \
        .appName("Reading from json")\
        .master('local[*]')\
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir +"/../../../" + "application.yml")
    secret_conf_path = os.path.abspath(current_dir + "/../../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader = yaml.FullLoader)
    secret = open(secret_conf_path)
    secret_conf = yaml.load(secret, Loader = yaml.FullLoader)

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key","s3a://" + secret_conf["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", "s3a://"+ secret_conf["s3_conf"]["secret_access_key"])

    company_df = spark.read \
        .json("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/company.json")

    company_df.printSchema()
    company_df.show(5, False)

    flattend_df = company_df.select(col("company"), explode(col("Employees")).alias("Employee"))

    flattend_df.show(5, False)

    flattend_df \
        .select(col("company"), col("employee.firstName").alias("emp_name"))\
        .show()

    spark.stop()

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" Exercises/ingestion/files/json_df.py