from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, when

import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == '__main__':
    
    def app():
        try:
            logger.info("Starting Spark session")
            spark = SparkSession.builder.appName("FormatJson") \
                .config("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "minio")) \
                .config("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")) \
                .config("fs.s3a.endpoint", os.getenv("ENDPOINT", "http://host.docker.internal:9000")) \
                .config("fs.s3a.connection.ssl.enabled", "false") \
                .config("fs.s3a.path.style.access", "true") \
                .config("fs.s3a.attempts.maximum", "1") \
                .config("fs.s3a.connection.establish.timeout", "5000") \
                .config("fs.s3a.connection.timeout", "10000") \
                .getOrCreate()
            
            bucket_name = os.getenv("SPARK_APPLICATION_ARGS")
            category = os.getenv("FOLDER_NAME")
            date = os.getenv("DAG_RUN_DATE")
            input_json_path = f's3a://{bucket_name}/{date}/{category}/{category}.json'
            
            try:
                df = spark.read.json(input_json_path)
                logger.info("JSON file read successfully.")
            except Exception as e:
                logger.error("Failed to read json file.")
            
            if category == 'population_trend':
                formatted_df = df.select(
                    col("PRD_DE").alias("Date"),
                    col("DT").alias("Value"),
                    col("PRD_SE").alias("Gender"),
                    when(col("C1_NM_ENG") == "Whole country", "Total").otherwise(col("C1_NM_ENG")).alias("Country"),
                    col("C2_NM").alias("Category")
                )
                
                formatted_df = formatted_df.withColumn("Date", concat(
                    col("Date").substr(1, 4),
                    lit("-"),
                    col("Date").substr(5, 2),
                    lit("-01")
                ))
                
            
            elif category == 'average_first_marriage_age':
                formatted_df = df.select(
                    col("PRD_DE").alias("Date"),
                    when(col("ITM_NM_ENG") == "Husband", "M") \
                        .when(col("ITM_NM_ENG") == "Wife", "F") \
                        .otherwise(col("ITM_NM_ENG")).alias("Gender"),
                    col("DT").alias("Age"),
                    when(col("C1_NM_ENG") == "Whole country", "Total").otherwise(col("C1_NM_ENG")).alias("Country"),
                )

                
            elif category == 'gender_income':
                formatted_df = df.select(
                    col("PRD_DE").alias("Date"),
                    col("ITM_NM_ENG").alias("Category"),
                    col("UNIT_NM").alias("Unit"),
                    col("C1_NM").alias("Employment_type"),
                    when(col("C2_NM_ENG") == "Male", "M") \
                        .when(col("C2_NM_ENG") == "Female", "F") \
                        .otherwise(col("C2_NM_ENG")).alias("Gender"),
                )

            
            output_csv_path = f's3a://{bucket_name}/{date}/{category}'
            logger.info(f"Saving formatted data to: {output_csv_path}")
            
            formatted_df.write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(output_csv_path)
            logger.info("Formatted data saved successfully.")
        
            
        except Exception as e:
            logger.error("An error occurred:", exc_info=True)

    app()
    os.system('kill %d' % os.getpid())