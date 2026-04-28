# Importation des modules nécessaires
import os
from src.utils.spark_session import create_spark_session
from src.utils.logger import setup_logger
from src.utils.config import RAW_DATA_PATH


def ingest_data():
    """Fonction principale pour ingérer les données depuis les fichiers CSV"""
    logger = setup_logger()
    spark = create_spark_session()

    logger.info("Starting data ingestion...")

    dfs = {}

    files = os.listdir(RAW_DATA_PATH)

    for file in files:
        if file.endswith(".csv"):
            file_path = os.path.join(RAW_DATA_PATH, file)
            table_name = file.replace(".csv", "")

            logger.info(f"Reading file: {file}")

            df = (spark.read
                  .option("header", "true")
                  .option("inferSchema", "true")
                  .csv(file_path)
            )

            logger.info(f"Schema for {table_name}:")
            df.printSchema()

            logger.info(f"Preview for {table_name}:")
            df.show(5)

            count = df.count()
            logger.info(f"Number of rows in {table_name}: {count}")

            dfs[table_name] = df

    logger.info("Data ingestion completed.")

    return dfs


if __name__ == "__main__":
    dataframes = ingest_data()