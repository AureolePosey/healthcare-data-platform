import os
from pyspark.sql import functions as F
from src.utils.spark_session import create_spark_session
from src.utils.logger import setup_logger
from src.utils.config import RAW_DATA_PATH, CLEANED_DATA_PATH


def clean_patients(df):
    df = df.withColumn(
        "gender",
        F.when(F.col("gender") == "M", "F")
         .when(F.col("gender") == "F", "M")
         .otherwise(F.col("gender"))
    )

    # trim seulement colonnes string
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, F.trim(F.col(c)))

    df = df.dropDuplicates(["patient_id"])
    return df


def clean_doctors(df):
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, F.trim(F.col(c)))

    df = df.dropDuplicates(["doctor_id"])
    return df


def clean_appointments(df, patients_df, doctors_df):
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, F.trim(F.col(c)))

     #JOIN au lieu de collect
    df = df.join(patients_df.select("patient_id"), on="patient_id", how="inner")
    df = df.join(doctors_df.select("doctor_id"), on="doctor_id", how="inner")

    df = df.dropDuplicates(["appointment_id"])
    return df


def clean_treatments(df, appointments_df):
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, F.trim(F.col(c)))

    df = df.join(
        appointments_df.select("appointment_id"),
        on="appointment_id",
        how="inner"
    )

    df = df.dropDuplicates(["treatment_id"])
    return df


def clean_billing(df, patients_df, treatments_df):
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, F.trim(F.col(c)))

    df = df.join(patients_df.select("patient_id"), on="patient_id", how="inner")
    df = df.join(treatments_df.select("treatment_id"), on="treatment_id", how="inner")

    df = df.dropDuplicates(["bill_id"])
    return df


def clean_data():
    logger = setup_logger()
    spark = create_spark_session()

    logger.info("Starting data cleaning...")

    dfs = {}

    # ingestion simple
    for file in os.listdir(RAW_DATA_PATH):
        if file.endswith(".csv"):
            path = os.path.join(RAW_DATA_PATH, file)
            name = file.replace(".csv", "")
            df = spark.read.option("header", True).option("inferSchema", True).csv(path)
            dfs[name] = df

    # cleaning
    patients_df = clean_patients(dfs["patients"])
    doctors_df = clean_doctors(dfs["doctors"])
    appointments_df = clean_appointments(dfs["appointments"], patients_df, doctors_df)
    treatments_df = clean_treatments(dfs["treatments"], appointments_df)
    billing_df = clean_billing(dfs["billing"], patients_df, treatments_df)

    # sauvegarde en parquet 
    os.makedirs(CLEANED_DATA_PATH, exist_ok=True)

    patients_df.write.mode("overwrite").parquet(f"{CLEANED_DATA_PATH}/patients")
    doctors_df.write.mode("overwrite").parquet(f"{CLEANED_DATA_PATH}/doctors")
    appointments_df.write.mode("overwrite").parquet(f"{CLEANED_DATA_PATH}/appointments")
    treatments_df.write.mode("overwrite").parquet(f"{CLEANED_DATA_PATH}/treatments")
    billing_df.write.mode("overwrite").parquet(f"{CLEANED_DATA_PATH}/billing")

    logger.info("Data cleaning completed.")

    return {
        "patients": patients_df,
        "doctors": doctors_df,
        "appointments": appointments_df,
        "treatments": treatments_df,
        "billing": billing_df
    }
    


if __name__ == "__main__":
    clean_data()