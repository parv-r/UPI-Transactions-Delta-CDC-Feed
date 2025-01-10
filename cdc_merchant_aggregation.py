# Databricks notebook source
# # Read CDC stream
# cdc_stream = spark.readStream.format("delta") \
#     .option("readChangeFeed", "true") \
#     .table("incremental_load.default.raw_upi_transactions")

# # Display CDC changes
# query = cdc_stream.select(
#     "transaction_id",
#     "upi_id",
#     "merchant_id",
#     "transaction_amount",
#     "transaction_timestamp",
#     "transaction_status",
#     "_change_type",  # CDC change type
#     "_commit_version",
#     "_commit_timestamp"
# ).writeStream.format("console") \
#     .outputMode("update") \
#     .start()

# query.awaitTermination()

# COMMAND ----------

from pyspark.sql.functions import col, sum, when
from delta.tables import DeltaTable

# Target Delta table for aggregated data
aggregated_table_name = "incremental_load.default.aggregated_upi_transactions"
raw_table_name = "incremental_load.default.raw_upi_transactions_v1"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {aggregated_table_name} (
    merchant_id STRING,
    total_sales DOUBLE,
    total_refunds DOUBLE,
    net_sales DOUBLE
)
USING delta
""")

# Define aggregation logic and merge into the target table
def process_aggregation(batch_df, batch_id):
    print(f"Processing batch: {batch_id}")

    # Perform aggregation
    aggregated_df = (
        batch_df
        .filter(col("_change_type").isin("insert", "update_postimage"))  # Handle inserts and updates
        .groupBy("merchant_id")
        .agg(
            sum(when(col("transaction_status") == "completed", col("transaction_amount")).otherwise(0)).alias("total_sales"),
            sum(when(col("transaction_status") == "refunded", col("transaction_amount")).otherwise(0)).alias("total_refunds")
        )
        .withColumn("net_sales", col("total_sales") - col("total_refunds"))
    )

    # Merge aggregated data into the target table
    target_table = DeltaTable.forName(spark, aggregated_table_name)
    target_table.alias("target").merge(
        aggregated_df.alias("source"),
        "target.merchant_id = source.merchant_id"
    ).whenMatchedUpdate(set={
        "total_sales": "target.total_sales + source.total_sales",
        "total_refunds": "target.total_refunds + source.total_refunds",
        "net_sales": "target.net_sales + source.net_sales"
    }).whenNotMatchedInsertAll().execute()

# Read CDC changes and apply aggregation logic
cdc_stream = spark.readStream.format("delta").option("readChangeFeed", "true").table(raw_table_name)
print("Read Stream Started.........")

cdc_stream.writeStream.foreachBatch(process_aggregation).outputMode("update").start().awaitTermination()
print("Write Stream Started.........")
