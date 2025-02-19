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
    "Escolarida", "Sitio_ocur", "Derechohab", "Embarazo", "Rel_emba", "Presunto", "Par_agre","Capitulo","Edad_agru")
EDR.write.mode("overwrite").parquet(os.path.join(parquet_folder, "EDR_INEGI.parquet"))
print("✅ Procesamiento completado. Archivos guardados en Parquet.")

#------------------------------------------------------------------Limpieza y normalizacion
EDR["Edad"]= EDR["Edad"].str[-2:]
EDR["Ent_id"] = EDR["Ent_resid"].astype(int)
EDR["Mun_id"] = EDR["Ent_resid"].astype(int).astype(str) + EDR["Mun_resid"].astype(int).astype(str)
EDR["Loc_id"] = EDR["Ent_resid"].astype(int).astype(str) + EDR["Mun_resid"].astype(int).astype(str) + EDR["Loc_resid"].astype(int).astype(str)
EDR["Year"] = EDR["Anio_ocur"].astype(int)
EDR["Entidad"] = EDR["Ent_resid"].astype(str)
EDR["Municipio"] = EDR["Mun_resid"].astype(str)
EDR["Localidad"] = EDR["Loc_resid"].astype(str)
EDR["Edad"].astype(int)
EDR["Causa"] = EDR["Causa_def"].astype(str)
EDR["Edo_civil"] = EDR["Edo_civil"].astype(str)
EDR["Escolarida"]=EDR["Escolarida"].astype(str)
EDR["Sitio_ocur"]=EDR["Sitio_ocur"].astype(str)
EDR["Derechohab"]=EDR["Derechohab"].astype(str)
EDR["Embarazo"]=EDR["Embarazo"].astype(str)
EDR["Rel_emba"]=EDR["Rel_emba"].astype(str)
EDR["Presunto"]=EDR["Presunto"].astype(str)
EDR["Par_agre"]=EDR["Par_agre"].astype(str)
EDR["Edad_agru"]=EDR["Edad_agru"].astype(str)

edo_civil_dict = {
    '1': 'Soltera', '2': 'Divorciada', '3': 'Viuda', 
    '4': 'Unión libre', '5': 'Casada', '6': 'Separada',
    '8': 'No aplica', '9': 'No aplica'}
EDR['Edo_civil'] = EDR['Edo_civil'].map(edo_civil_dict)

escolaridad_dict = {
    '1': 'Sin escolaridad',
    '2': 'Preescolar',
    '3': 'Primaria incompleta',
    '4': 'Primaria completa',
    '5': 'Secundaria incompleta',
    '6': 'Secundaria completa',
    '7': 'Bachillerato o preparatoria incompleto',
    '8': 'Bachillerato o preparatoria completo',
    '9': 'Profesional',
    '10': 'Posgrado'
}

EDR['Escolarida'] = EDR['Escolarida'].map(escolaridad_dict)

sitio_ocur_dict = {
    '1': 'Unidad médica pública',
    '2': 'Unidad médica pública',
    '3': 'Unidad médica pública',
    '4': 'Unidad médica pública',
    '5': 'Unidad médica pública',
    '6': 'Unidad médica pública',
    '7': 'Unidad médica pública',
    '8': 'Unidad médica pública',
    '9': 'Unidad médica privada',
    '10': 'Vía pública',
    '11': 'Hogar',
    '12': 'Otro lugar'
}

EDR['Sitio_ocur'] = EDR['Sitio_ocur'].map(sitio_ocur_dict)

derechohab_dict = {
    '1': 'Ninguna',
    '2': 'IMSS',
    '3': 'ISSSTE',
    '4': 'PEMEX',
    '5': 'SEDENA',
    '6': 'SEMAR',
    '7': 'Seguro Popular',
    '8': 'Otra',
    '9': 'IMSS PROSPERA',
    '99': 'No especificada'
}

EDR['Derechohab'] = EDR['Derechohab'].map(derechohab_dict)

presunto_dict = {
    '1': 'Accidente',
    '2': 'Homicidio',
    '3': 'Suicidio',
    '4': 'Se ignora',
    '5': 'Operaciones legales y de guerra',
    '6': 'No aplica para muerte natural'
}

EDR['Presunto'] = EDR['Presunto'].map(presunto_dict)

#Evaluar si vale la pena traer todo el catalogo o solamente los incidentes, se procede a hacerlo manual solo con incidentes para hacer una 
#normalizacion funcional en el analisis

EDR.groupby('Par_agre').size()

Par_agre_dict = {
    '88':'No aplica', 
    '99':'No aplica',
    '72':'No aplica',
    #----Personas ajenas a la familia
    '33':'Sin parentesco', '66':'Sin parentesco', '71':'Sin parentesco','68':'Sin parentesco',
    #'33':'Cuñado', '66':'Conocido', '71':'Sin parentesco','68':'Amigo', 
    #----Pareja Sentimental
    '45':'Pareja', '51':'Pareja', '11':'Pareja',
    #'45':'Concubino, compañero', '51':'Ex esposo', '11':'Esposo, Cónyuge' 
    #-----Familia
    '1':'Padre', 
     #-----Familia cercana
    '5':'Familia cercana', '4': 'Familia cercana','9':'Familia cercana', '15': 'Familia cercana'
    #'5':'Hijo','4': 'Hermana','9':'Nieto', '15': 'Sobrino'
}

EDR['Par_agre'] = EDR['Par_agre'].map(Par_agre_dict).fillna('No aplica')


