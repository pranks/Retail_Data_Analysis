#Impoting IMportant Functions

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

#Defining Spark Session 

spark = SparkSession  \
        .builder  \
        .appName("RetailProject")  \
        .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

#Reading the stream from Kafka Topic 

lines = spark  \
        .readStream  \
        .format("kafka")  \
        .option("kafka.bootstrap.servers","18.211.252.152:9092")  \
        .option("subscribe","real-time-project")  \
        .option("startingOffsets","latest")\
        .load()


#Defining Schema to read the topic in defined columns 

retail_schema = StructType() \
                .add("invoice_no",LongType()) \
                .add("country",StringType()) \
                .add("timestamp",TimestampType()) \
                .add("type",StringType()) \
                .add("items",ArrayType(StructType([StructField("SKU", StringType()),
                                                   StructField("title", StringType()),
                                                   StructField("unit_price", FloatType()),
                                                   StructField("quantity", IntegerType())
                                                  ])))

#Reading Json using defining schema 

infer_schema_retail= lines.select(from_json(col("value").cast("string"),retail_schema).alias("retail_data")).select("retail_data.*")

#Defining Function to calculate Derived columns 
#1) Function for Total_cost

def total_cost(type1,items):
    sum=0
    if type1.lower()=='order':
        for i in items:
            sum=sum+(i['unit_price']*i['quantity'])
        return sum
    elif type1.lower()=='return':
        for j in items:
            sum=sum+(j['unit_price']*j['quantity'])
        return sum*(-1)
#2)Function for total_items
def total_items(items):
    sum=0
    for i in items:
        sum=sum+i["quantity"]
    return sum

#3)Function for is_order and is_return 
def is_order(type1):
    if type1.lower()=='order':
        return 1
    elif type1.lower()=='return':
        return 0


def is_return(type1):
    if type1.lower()=='order':
        return 0
    elif type1.lower()=='return':
        return 1


#Converting Functions in UDF 
udf_total_cost=udf(total_cost,FloatType())
udf_total_items=udf(total_items,IntegerType())
udf_is_order=udf(is_order,IntegerType())
udf_is_return=udf(is_return,IntegerType())


#Adding Derived Columns in existing Data Frame infer_schema_retail

new_df_with_derived_columns=infer_schema_retail.withColumn("total_cost",udf_total_cost(infer_schema_retail.type,infer_schema_retail.items))\
        .withColumn("total_items",udf_total_items(infer_schema_retail.items))\
        .withColumn("is_order",udf_is_order(infer_schema_retail.type))\
        .withColumn("is_return",udf_is_return(infer_schema_retail.type))


# Writing stream output to console with mentioned columns name 

Retail_Console_Output = new_df_with_derived_columns \
       .select("invoice_no", "country", "timestamp","total_cost","total_items","is_order","is_return") \
       .writeStream \
       .outputMode("append") \
       .format("console") \
       .option("truncate", "false") \
       .trigger(processingTime="1 minute") \
       .start()

#calculating Time based KPI 

time_based_kpi_df=new_df_with_derived_columns.withWatermark("timestamp","1 minutes") \
        .groupBy(window("timestamp","1 minutes")).agg(sum("total_cost").alias("Total volume of sales"),count("invoice_no").alias("OPM"),avg("total_cost").alias("Average transaction size"),avg("is_return").alias("Rate of return")) \
        .select("window.start","window.end","OPM","Total volume of sales","Average transaction size","Rate of return")

#Writing Stream to Json File 

time_based_kpi_df_output = time_based_kpi_df \
        .writeStream \
        .outputMode("append") \
        .format("json") \
        .option("format","append") \
        .option("truncate", "false") \
        .option("path", "time_based_kpi_1") \
        .option("checkpointLocation", "KPI_T_1") \
        .option("truncate", "False") \
        .trigger(processingTime="1 minute") \
        .start()

#calculating Time and country based KPI
time_and_country_based_kpi_df=new_df_with_derived_columns.withWatermark("timestamp","1 minutes") \
        .groupBy(window("timestamp","1 minutes"),"country").agg(sum("total_cost").alias("Total volume of sales"),count("invoice_no").alias("OPM"),avg("total_cost").alias("Average transaction size"),avg("is_return").alias("Rate of return")) \
        .select("window.start","window.end","country","OPM","Total volume of sales","Average transaction size","Rate of return")



#Writing Stream to Json File

time_and_country_based_kpi_df_output = time_and_country_based_kpi_df \
        .writeStream \
        .outputMode("append") \
        .format("json") \
        .option("format","append") \
        .option("truncate", "false") \
        .option("path", "time_and_country_based_kpi_1") \
        .option("checkpointLocation", "KPI_T_C_1") \
        .option("truncate", "False") \
        .trigger(processingTime="1 minute") \
        .start()


time_and_country_based_kpi_df_output.awaitTermination() 


