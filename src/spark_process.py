# -*- coding: utf-8 -*-
"""
Created on Sat Dec  7 22:13:57 2019

@author: MOISES.SILVA
"""
import platform
import os
import webbrowser


mapa_path ={}
mapa_path["windows"] = "C:/spark"
mapa_path["linux"] = "/home/moises/spark"

import findspark

findspark.init(mapa_path[platform.system().lower()])
# Initialize and provide path


from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import DataFrame
import folium
from folium.plugins import HeatMap
from IPython.core.display import HTML
from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator
import matplotlib.pyplot as plt
#%matplotlib inline
import datetime
import pandas as pd

APP_NAME = "Flight Anilizer"
MASTER = "local[*]"



    
    
    


        
        
        
try:
    p = ProcessData()
finally:
    pass

p.load_voo()
p.top_10_nivel_servico(ordem='asc')
p.top_10_nivel_servico(ordem='desc')
p.criar_word_cloud_destinos()
p.criar_mapa_calor()  
p.print_finish()
#p.app.stop_app()

dados = p.execute_sql("""
SELECT 
    `Codigo.Justificativa` AS JUSTIFICATIVA,
    COUNT(1) AS TOTAL
FROM VOO  
WHERE   `Partida.Prevista` LIKE '2017%' 
AND LOWER(`Pais.Destino`) NOT LIKE LOWER("brasil%") 
AND `Companhia.Aerea` NOT LIKE 'NAO INFORMADO' 
AND `Codigo.Justificativa` NOT LIKE 'NA'
AND `Situacao.Voo` NOT LIKE 'Cancelado' 
GROUP BY 1 
ORDER BY TOTAL DESC
LIMIT 10
""").toPandas()

import numpy as np
index = np.arange(len(label))
label = dados['JUSTIFICATIVA'].values.tolist()
quantidades = dados['TOTAL'].values.tolist()
index = np.arange(len(label))
plt.figure(figsize=(15,8))
plt.bar(index, quantidades)
plt.xlabel('Genre', fontsize=15)
plt.ylabel('No of Movies', fontsize=15)
plt.xticks(index, label, fontsize=12, rotation=180)
plt.title('Market Share for Each Genre 1995-2017')
plt.barh(x=dados['JUSTIFICATIVA'].values.tolist(),y=dados['TOTAL'].values.tolist(),width=100)
plt.show()

import seaborn
df = pd.DataFrame({"x":label,"y":quantidades})
df.plot.barh()
seaborn.lineplot(df)
dados.plot.barh(x=label,y=quantidades)



        
        
    
        

