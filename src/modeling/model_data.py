from src.utils.spark_session import create_spark_session
from src.utils.logger import setup_logger
from src.utils.config import CURATED_DATA_PATH, MODELING_DATA_PATH

from pyspark.sql import functions as F


def create_star_schema():
    logger = setup_logger()
    spark = create_spark_session()

    logger.info("Starting Star Schema modeling...")

    # ==============================
    # 1. LOAD CURATED DATA
    # ==============================
    df = spark.read.parquet(CURATED_DATA_PATH)

    # ==============================
    # 2. DIMENSIONS
    # ==============================

    # 🧑 dim_patient
    dim_patient = (
        df.select("patient_id")
        .distinct()
        .withColumn("patient_sk", F.monotonically_increasing_id())
    )

    # 👨‍⚕️ dim_doctor
    dim_doctor = (
        df.select("doctor_id")
        .distinct()
        .withColumn("doctor_sk", F.monotonically_increasing_id())
    )

    # 📅 dim_date
    dim_date = (
        df.select("date", "year", "month", "quarter")
        .distinct()
        .withColumn("date_sk", F.monotonically_increasing_id())
    )

    # 💊 dim_treatment (optionnel mais propre)
    dim_treatment = (
        df.select("treatment_id")
        .distinct()
        .withColumn("treatment_sk", F.monotonically_increasing_id())
    )

    # ==============================
    # 3. SAVE DIMENSIONS
    # ==============================
    dim_patient.write.mode("overwrite").parquet(f"{MODELING_DATA_PATH}/dim_patient")
    dim_doctor.write.mode("overwrite").parquet(f"{MODELING_DATA_PATH}/dim_doctor")
    dim_date.write.mode("overwrite").parquet(f"{MODELING_DATA_PATH}/dim_date")
    dim_treatment.write.mode("overwrite").parquet(f"{MODELING_DATA_PATH}/dim_treatment")

    logger.info("Dimensions created successfully")

    # ==============================
    # 4. FACT TABLE (fact_billing)
    # ==============================

    fact = df \
        .join(dim_patient, "patient_id", "left") \
        .join(dim_doctor, "doctor_id", "left") \
        .join(dim_date, "date", "left") \
        .join(dim_treatment, "treatment_id", "left")

    fact_billing = fact.select(
        "patient_sk",
        "doctor_sk",
        "date_sk",
        "treatment_sk",
        "price",
        "revenue_flag",
        "treatment_count",
        "total_spent"
    )

    # ==============================
    # 5. SAVE FACT
    # ==============================
    fact_billing.write.mode("overwrite").parquet(f"{MODELING_DATA_PATH}/fact_billing")

    logger.info("Fact table created successfully")
    fact_billing.show(10)

    return fact_billing


if __name__ == "__main__":
    create_star_schema()