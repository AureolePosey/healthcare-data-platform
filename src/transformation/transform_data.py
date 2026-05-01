from src.utils.spark_session import create_spark_session
from src.utils.logger import setup_logger
from src.utils.config import CLEANED_DATA_PATH, CURATED_DATA_PATH

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def transform_data():
    logger = setup_logger()
    spark = create_spark_session()

    logger.info("Starting data transformation...")

    # ==============================
    # 1. LOAD CLEANED DATA
    # ==============================
    patients_df = spark.read.parquet(f"{CLEANED_DATA_PATH}/patients")
    doctors_df = spark.read.parquet(f"{CLEANED_DATA_PATH}/doctors")
    appointments_df = spark.read.parquet(f"{CLEANED_DATA_PATH}/appointments")
    treatments_df = spark.read.parquet(f"{CLEANED_DATA_PATH}/treatments")
    billing_df = spark.read.parquet(f"{CLEANED_DATA_PATH}/billing")

    # ==============================
    # 2. JOINS AVEC ALIAS (PRO)
    # ==============================
    t = treatments_df.alias("t")
    a = appointments_df.alias("a")
    p = patients_df.alias("p")
    d = doctors_df.alias("d")
    b = billing_df.alias("b")

    df = t \
        .join(a, F.col("t.appointment_id") == F.col("a.appointment_id"), "inner") \
        .join(p, F.col("a.patient_id") == F.col("p.patient_id"), "inner") \
        .join(d, F.col("a.doctor_id") == F.col("d.doctor_id"), "inner") \
        .join(b, F.col("t.treatment_id") == F.col("b.treatment_id"), "left")

    # ==============================
    # 3. SÉLECTION PROPRE (ANTI-AMBIGUÏTÉ)
    # ==============================
    df = df.select(
        F.col("t.treatment_id").alias("treatment_id"),
        F.col("a.patient_id").alias("patient_id"),
        F.col("a.doctor_id").alias("doctor_id"),
        F.col("a.appointment_date").alias("appointment_date"),
        F.col("b.amount").alias("amount"),
        F.col("b.payment_status").alias("payment_status")
    )

    # ==============================
    # 4. FEATURE ENGINEERING
    # ==============================

    # Revenue flag
    df = df.withColumn(
        "revenue_flag",
        F.when(F.col("payment_status") == "paid", "PAID").otherwise("UNPAID")
    )

    # Price
    df = df.withColumn("price", F.col("amount"))

    # Window
    window_patient = Window.partitionBy("patient_id")

    # Number of treatments per patient
    df = df.withColumn(
        "treatment_count",
        F.count("treatment_id").over(window_patient)
    )

    # Total spent per patient
    df = df.withColumn(
        "total_spent",
        F.sum("amount").over(window_patient)
    )

    # ==============================
    # 5. DATE FEATURES
    # ==============================
    df = df.withColumn("date", F.to_date("appointment_date"))

    df = df.withColumn("year", F.year("date")) \
           .withColumn("month", F.month("date")) \
           .withColumn("quarter", F.quarter("date"))

    # ==============================
    # 6. FINAL DATASET
    # ==============================
    df_final = df.select(
        "treatment_id",
        "patient_id",
        "doctor_id",
        "date",
        "year",
        "month",
        "quarter",
        "price",
        "revenue_flag",
        "treatment_count",
        "total_spent"
    )

    # ==============================
    # 7. SAVE
    # ==============================
    df_final.write.mode("overwrite").parquet(CURATED_DATA_PATH)

    logger.info("Transformation completed successfully")
    df_final.show(10)

    return df_final


if __name__ == "__main__":
    transform_data()