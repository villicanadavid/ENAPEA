#------------------------------------------------------------------Preparacion de entorno
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StringType, BooleanType
import pandas as pd
from simpledbf import Dbf5
import os
spark = SparkSession.builder.appName("DEFUN_ELT").getOrCreate()

#------------------------------------------------------------------Convertir .dbf a parquet para optimizar procesamiento
dbf_folder = "/content/drive/MyDrive/DEFUN/"
parquet_folder = "/content/drive/MyDrive/DEFUN/parquet_files/"
os.makedirs(parquet_folder, exist_ok=True)
spark = SparkSession.builder.appName("DBFtoParquet").getOrCreate()
dbf_files = [f for f in os.listdir(dbf_folder) if f.endswith(".dbf")]

for dbf_file in dbf_files:
    dbf_path = os.path.join(dbf_folder, dbf_file)
    parquet_path = os.path.join(parquet_folder, dbf_file.replace(".dbf", ".parquet"))
    try:
        dbf = Dbf5(dbf_path, codec="utf-8")
        df_pandas = dbf.to_dataframe()
        df_pandas.to_parquet(parquet_path, engine="pyarrow")
        print(f"✅ Convertido: {dbf_file} → {parquet_path}")
    except Exception as e:
        print(f"❌ Error al procesar {dbf_file}: {e}")
      
#------------------------------------------------------------------Convertir .dbf a parquet para optimizar procesamiento

parquet_files = [os.path.join(parquet_folder, f) for f in os.listdir(parquet_folder) if f.endswith(".parquet")]
DEFUN = spark.read.parquet(*parquet_files)
DEFUN_GRAL = DEFUN.filter(
    (col("Anio_ocur").between(2005, 2030)) & 
    (col("Embarazo").isin(1, 2, 3, 4))
)

DEFUN_ENAPEA = DEFUN_GRAL.filter(col("Edad") <= 19)

DEFUN_GRAL = DEFUN_GRAL.select(
    col("Anio_ocur").cast(IntegerType()),
    col("Ent_resid").alias("Ent_ID").cast(IntegerType()),
    col("Mun_resid").alias("Mun_ID").cast(IntegerType()),
    col("Loc_resid").alias("Loc_ID").cast(IntegerType()),
    col("Edad").cast(IntegerType()),
    col("Causa_def").cast(StringType()),
    col("Embarazo").cast(BooleanType()),
    col("Rel_emba").cast(BooleanType())
)

DEFUN_ENAPEA = DEFUN_ENAPEA.select(
    col("Anio_ocur").cast(IntegerType()),
    col("Ent_resid").alias("Ent_resid_ID").cast(IntegerType()),
    col("Mun_resid").alias("Mun_resid_ID").cast(IntegerType()),
    col("Loc_resid").alias("Loc_resid_ID").cast(IntegerType()),
    col("Edad").cast(IntegerType()),
    col("Causa_def").cast(StringType()),
    col("Edo_civil").cast(StringType()),
    col("Escolarida").cast(StringType()),
    col("Sitio_ocur").cast(StringType()),
    col("Derechohab").cast(StringType()),
    col("Embarazo").cast(BooleanType()),
    col("Rel_emba").cast(BooleanType()),
    col("Presunto").cast(BooleanType())
)

DEFUN_GRAL.write.mode("overwrite").parquet(os.path.join(parquet_folder, "DEFUN_GRAL.parquet"))
DEFUN_ENAPEA.write.mode("overwrite").parquet(os.path.join(parquet_folder, "DEFUN_ENAPEA.parquet"))
print("✅ Procesamiento completado. Archivos guardados en Parquet.")
