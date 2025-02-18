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
dbf_files = [f for f in os.listdir(dbf_folder) if f.endswith(".dbf")]
for dbf_file in dbf_files:
    dbf_path = os.path.join(dbf_folder, dbf_file)
    parquet_path = os.path.join(parquet_folder, dbf_file.replace(".dbf", ".parquet"))
    try:
        dbf = Dbf5(dbf_path, codec="utf-8")
        df_pandas = dbf.to_dataframe()
        
        for col in df_pandas.columns:
            df_pandas[col] = df_pandas[col].astype(str)
        df_pandas.to_parquet(parquet_path, engine="pyarrow")
        print(f"✅ Convertido: {dbf_file} → {parquet_path}")
    except Exception as e:
        print(f"❌ Error al procesar {dbf_file}: {e}")

#------------------------------------------------------------------Obtener DF EDR

parquet_files = [os.path.join(parquet_folder, f) for f in os.listdir(parquet_folder) if f.endswith(".parquet")]
DEFUN = spark.read.option("mergeSchema", "true").parquet(*parquet_files)
EDR = DEFUN.filter((DEFUN["Embarazo"].isin(1, 2, 3, 4)))
EDR = EDR.select(
    "Anio_ocur", "Ent_resid", "Mun_resid", "Loc_resid", "Edad", "Causa_def", "Edo_civil", 
    "Escolarida", "Sitio_ocur", "Derechohab", "Embarazo", "Rel_emba", "Presunto", "Capitulo","Edad_agru")
EDR.write.mode("overwrite").parquet(os.path.join(parquet_folder, "EDR_INEGI.parquet"))
print("✅ Procesamiento completado. Archivos guardados en Parquet.")

#------------------------------------------------------------------Alimentar la DB en MYSQL

        
