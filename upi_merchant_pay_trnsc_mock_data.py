# Databricks notebook source
# Create the raw Delta table with CDC enabled
spark.sql("""
CREATE TABLE IF NOT EXISTS incremental_load.default.raw_upi_transactions_v1 (
    transaction_id STRING,
    upi_id STRING,
    merchant_id STRING,
    transaction_amount DOUBLE,
    transaction_timestamp TIMESTAMP,
    transaction_status STRING
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = true)
""")
print("Delta table 'incremental_load.default.raw_upi_transactions_v1' created with CDC enabled.")

# COMMAND ----------

from delta.tables import DeltaTable
import time

# Mock data batches to simulate CDC
mock_batches = [
    # Batch 1: Insert new transactions
    spark.createDataFrame([
        ("T001", "upi1@bank", "M001", 500.0, "2024-12-21 10:00:00", "initiated"),
        ("T002", "upi2@bank", "M002", 1000.0, "2024-12-21 10:05:00", "initiated"),
        ("T003", "upi3@bank", "M003", 1500.0, "2024-12-21 10:10:00", "initiated"),
    ], ["transaction_id", "upi_id", "merchant_id", "transaction_amount", "transaction_timestamp", "transaction_status"]),

    # Batch 2: Update and insert transactions
    spark.createDataFrame([
        ("T001", "upi1@bank", "M001", 500.0, "2024-12-21 10:15:00", "completed"),  # Update transaction
        ("T002", "upi2@bank", "M002", 1000.0, "2024-12-21 10:20:00", "failed"),    # Update transaction
        ("T004", "upi4@bank", "M004", 2000.0, "2024-12-21 10:25:00", "initiated"), # New transaction
    ], ["transaction_id", "upi_id", "merchant_id", "transaction_amount", "transaction_timestamp", "transaction_status"]),

    # Batch 3: Handle refunds and updates
    spark.createDataFrame([
        ("T001", "upi1@bank", "M001", 500.0, "2024-12-21 10:30:00", "refunded"),  # Refund issued
        ("T003", "upi3@bank", "M003", 1500.0, "2024-12-21 10:35:00", "completed"), # Completed transaction
    ], ["transaction_id", "upi_id", "merchant_id", "transaction_amount", "transaction_timestamp", "transaction_status"]),
]


# Merge logic
def merge_to_delta_table(delta_table_name: str, batch_df):
    delta_table = DeltaTable.forName(spark, delta_table_name)

    # Perform merge operation
    delta_table.alias("target").merge(
        batch_df.alias("source"),
        "target.transaction_id = source.transaction_id"
    ).whenMatchedUpdate(
        set={
            "upi_id": "source.upi_id",
            "merchant_id": "source.merchant_id",
            "transaction_amount": "source.transaction_amount",
            "transaction_timestamp": "source.transaction_timestamp",
            "transaction_status": "source.transaction_status"
        }
    ).whenNotMatchedInsertAll().execute()

for i, batch_df in enumerate(mock_batches):
    print(f"Processing batch {i + 1}")
    merge_to_delta_table("incremental_load.default.raw_upi_transactions_v1", batch_df)
    print(f"Batch {i + 1} processed successfully.")
    time.sleep(10)

# merge_to_delta_table("incremental_load.default.raw_upi_transactions_v1", mock_batches[2])
# print(f"Batch processed successfully.")
