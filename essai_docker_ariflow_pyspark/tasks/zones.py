#from pyspark.sql import SparkSession
import pandas as pd 
from FlightRadar24.api import FlightRadar24API
#spark = SparkSession.builder.getOrCreate()
#from pyspark.sql.types import StructType, StructField, FloatType , StringType , DoubleType
def getzones():
    fr_api = FlightRadar24API()
    zones = fr_api.get_zones()    

    """schema = StructType([
        StructField("zone", StringType(), True),
        StructField("tl_y", FloatType(), True),
        StructField("tl_x", FloatType(), True),
        StructField("br_y", FloatType(), True),
        StructField("br_x", FloatType(), True),
    ])
    df_zones=spark.createDataFrame([],schema)"""
    schema=['zone','tl_y','tl_x','br_y','br_x']
    df_zones=pd.DataFrame(columns=schema)

    for zone in zones: 
        #dfsec=spark.createDataFrame([(zone,float(zones[zone]['tl_y']),float(zones[zone]['tl_x']),float(zones[zone]['br_y']),float(zones[zone]['br_x']))],schema)
        #df_zones=df_zones.union(dfsec)
        dfsec=pd.DataFrame([[zone,float(zones[zone]['tl_y']),float(zones[zone]['tl_x']),float(zones[zone]['br_y']),float(zones[zone]['br_x'])]],columns=schema)
        df_zones=pd.concat([df_zones,dfsec], ignore_index=True)
    #df_zones.toPandas().to_csv('zones.csv',index=False)
    df_zones.to_csv('zones.csv',index=False)
    return df_zones