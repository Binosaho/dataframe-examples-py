from pyspark.sql import SparkSession,Row
import os.path
import yaml

if __name__ == "__main__":

    spark = SparkSession \
        .builder\
        .appName("DataFrames examples")\
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.4')\
        .master('local[*]')\
        .gerOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    txn_fct_rdd = spark.sparkContext.textFile("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/txn_fct.csv") \
        .filter(lambda record: record.find("txn_id")) \
        .map(lambda record: record.split("|")) \
        .map(lambda record: (int(record[0]), record[1], float(record[2]), record[3], record[4], record[5], record[6]))

    for rec in txn_fct_rdd.take(5):
        print(rec)

    print("\nConvert RDD to Dataframe using toDF() - without column names,")

    txnDfNoColumns = txn_fct_rdd.toDF()
    txnDfNoColumns.printSchema()
    txnDfNoColumns.show(5, False)

    print("\nCreating Dataframe out of RDD without column names using createDataFrame(),")
    txnDfNoColumns2 = spark.createDataFrame(txn_fct_rdd)
    txnDfNoColumns2.printSchema()
    txnDfNoColumns2.show(5, False)

    print("\nConvert RDD to Dataframe using toDF(colNames: String*) - with column names,")
    txnDfWithColName = txn_fct_rdd.toDF(["txn_id", "created_time_string", "amount", "cust_id", "status", "merchant_id", "created_time_ist"])
    txnDfWithColName.printSchema()
    txnDfWithColName.show(5, False)

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" dataframe/ingestion/rdd/rdd2df_thru_schema_autoinfer.py
