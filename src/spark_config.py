# -*- coding: utf-8 -*-
"""
Created on Sat Dec  7 22:13:57 2019
@author: MOISES.SILVA
"""
# !pip install findspark
import platform
import os
import webbrowser


mapa_path ={}
mapa_path["windows"] = "C:/spark3"
mapa_path["linux"] = "/home/spark-3.0.0-preview-bin-hadoop2.7"

import findspark

findspark.init(mapa_path[platform.system().lower()])
# Initialize and provide path


from pyspark.sql import SparkSession

from pyspark import SparkContext
from pyspark import SparkConf

APP_NAME = "Flight Anilizer"
MASTER = "local[*]"


class SparkApp:
    def __init__(self,spark_path=mapa_path[platform.system().lower()]):
        
        self.mapa = {}
        
    def config(self):      
        
        self.config_spark_conf()       
        conf = self.mapa['conf']        
        self.config_spark_context(conf)       
        self.config_session()    
           
           
    def config_spark_conf(self):
        print('config_spark_conf')
        conf = SparkConf()
        conf.setAppName(APP_NAME)\
        .setMaster(MASTER)\
        .set("spark.driver.allowMultipleContexts","true")
        
        self.mapa['conf'] = conf
        return conf
    
    def config_spark_context(self,conf):
        sc = SparkContext(conf=conf)
        # sc.setLogLevel("DEBUG")
        self.mapa['sc'] = sc
        return sc
        
    
    def get_session(self):
        return self.mapa['session']
    
    def config_session(self):
        #
        session:SparkSession = SparkSession.builder\
        .master("local")\
        .appName(APP_NAME)\
        .config("spark.executor.memory", "1gb")\
        .getOrCreate() 
        
        self.mapa["session"] = session
        return session
    
    def stop_app(self):
        self.mapa["sc"].stop()
        
    def load_tsv(self,file_name,view_name):
        spark:SparkSession = self.get_session()
        df = spark.read.format("csv")\
        .option("head","true")\
        .option("mode", "DROPMALFORMED")\
        .option("delimiter", "\t")\
        .option("encoding", "utf-8")\
        .load(file_name)
        
        df.createOrReplaceTempView(view_name)
        return df
    
    def load_csv(self,file_name,view_name,sep):
        spark:SparkSession = self.get_session()
        
        df = spark.read.format("csv")\
        .option("header","true")\
        .option("mode", "DROPMALFORMED")\
        .option("delimiter", ",")\
        .load(file_name)
        
        df.createOrReplaceTempView(view_name)
        return df
    
