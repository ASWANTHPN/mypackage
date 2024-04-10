from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
import logging
def apply_udf_to_dataframe(data, udf_function, input_col_name, output_col_name):
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("UDF Example") \
        .getOrCreate()
    # Create DataFrame
    df = spark.createDataFrame(data, [input_col_name])
    # Register the UDF
    udf_function = udf(udf_function, IntegerType())
    # Apply the UDF to create a new column
    df_with_udf_applied = df.withColumn(output_col_name, udf_function(df[input_col_name]))
    # Show the DataFrame with the new column
    df_with_udf_applied.show()
    # Stop the SparkSession
    spark.stop()
    logging.info(df_with_udf_applied)
    return df_with_udf_applied
# Define the UDF function
def double_age(age):
    return age * 2
def main():
    data = [(25,), (30,), (35,)]
    apply_udf_to_dataframe(data, double_age, "Age", "DoubledAge")
