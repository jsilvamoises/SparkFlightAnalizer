# -*- coding: utf-8 -*-
"""
Created on Sat Dec  7 22:13:57 2019
@author: MOISES.SILVA
"""

from src.spark_config import SparkApp
import datetime
from pyspark.sql import SparkSession

from wordcloud import WordCloud
import folium
from folium.plugins import HeatMap
import matplotlib.pyplot as plt
plt.style.use('ggplot')
import webbrowser
import os
# import matplotlib.pyplot as plt; plt.rcdefaults()
import numpy as np
# import matplotlib.pyplot as plt

class FlightAnalizer:
    def __init__(self):
        self.app = SparkApp()
        self.app.config()
        self.start = datetime.datetime.now()
        self.end = None
    
    def load_data(self,file=None,view_name='default',sep=","):
        self.app.load_csv(file,view_name,sep)
        
    def process_all(self):
        pass
    
    def print_finish(self):
        
        info = f"""
        ############################################################### 
        START: {self.start}
        _______________________________________________________________
        END..: {datetime.datetime.now()} 
        _______________________________________________________________
        TEMPO: {datetime.datetime.now() - self.start}               
        ###############################################################        
        """
        print(info)
        
    def execute_sql(self,query,show=False):
        """[ EXECUTA UMA CONSULTA SQL ]"""
        
        spark:SparkSession = self.app.mapa['session']
        if show:
            df = spark.sql(query).show()
            
        else:
            df = spark.sql(query)
        
        return df
    
    def criar_word_cloud_destinos(self,show=False):
        """[ CRIA UMA NUVEM DE PALAVRAS COM OS PRINCIPAIS PAÍSES DE DESTINO ]"""
        
        words = self.execute_sql("""
        select 
            (`Pais.Destino`) from voo 
        where lower(`Pais.Destino`) not like lower("brasil%") 
        -- and `Partida.Prevista` LIKE "2017%"
        and `Companhia.Aerea` not like 'NAO INFORMADO'
        """)
        
        words_df = words.toPandas()
        paises = words_df['Pais.Destino'].values.tolist()


        font = {'family': 'arial',
                'color':  'black',
                'weight': 'normal',
                'size': 24,
                }

        strings = ",#".join(paises)
        strings = strings.replace(" ","_").upper()
        wordcloud = WordCloud(
            width = 1000, 
            height = 500,
            background_color='white',
            collocations=False).generate(strings)
        plt.figure(figsize=(15,8))
        plt.title("Principais destinos internacionais de 2015 à 2017 ",fontdict=font)
        plt.imshow(wordcloud,interpolation='bicubic')
        plt.axis("off")
        plt.savefig("img/principais_destinos_word_cloud"+".png", bbox_inches='tight')
        if show:
            plt.show()
        plt.close()
    
    def create_path_if_not_exists(self,path) :
        if not os.path.exists(path):
            os.makedirs(path)
        
        
    def criar_mapa_calor(self,show=False):
        df = self.execute_sql("""
        select 
                LatDest,
                LongDest
        from voo 
        where lower(`Pais.Destino`) not like lower("brasil%") 
        
        and `Companhia.Aerea` not like 'NAO INFORMADO'
        """)
        # -- and `Partida.Real` LIKE "2017%"
        df.head()
        df_pandas = df.toPandas()
        
        latlong = df_pandas[['LatDest','LongDest']].values.tolist()



        mapa = folium.Map(location=latlong[0],zoom_start=2,)

        HeatMap(latlong,radius=12,name="Principais destinos de voos do Brasil").add_to(mapa)
        if not os.path.exists('html'):
            os.makedirs('html')
        mapa.save('html/mapa_de_calor.html')
        
        if show:
            webbrowser.open_new_tab(f'{os.path.dirname(os.path.abspath("*"))}/html/mapa_de_calor.html')
        return mapa
    
    def top_10_nivel_servico(self,ordem='asc',show=False):
        if ordem == 'asc':
            filename = "Top 10 Piores Nível de Serviço"
        else:
            filename = "Top 10 Melhores Nível de Serviço"
            
        df =  self.execute_sql(f"""
        select 
            `Companhia.Aerea` as Companhia,
            COUNT(case when `Partida.Prevista` = `Partida.Real` then 1 END) AS NoHorario, 
            COUNT(case when `Partida.Prevista` > `Partida.Real` then 1 END) AS Antecipado,
            COUNT(case when `Partida.Prevista` < `Partida.Real` then 1 END) AS Atrasado,
            ((COUNT(case when `Partida.Prevista` > `Partida.Real`  then 1 END) + COUNT(case when `Partida.Real` = `Partida.Prevista`  then 1 END)) / COUNT(1))  as NivelServico,
            COUNT(1) AS TotalDeVoos
        from voo  
        where 
         `Situacao.Voo` NOT LIKE 'Cancelado' 
        -- and `Partida.Prevista` LIKE '2017%' 
        and lower(`Pais.Destino`) not like lower("brasil%") 
        and `Companhia.Aerea` not like 'NAO INFORMADO'
        GROUP BY 1
        ORDER BY NivelServico {ordem}
        LIMIT 10
        """)
        self.create_path_if_not_exists('csv')
        df_pd = df.toPandas()
        df.toPandas().to_csv(f'csv/{filename}.csv',sep=";",index=False,decimal=",")
        
        
        objects = df_pd['Companhia'].tolist()
        y_pos = np.arange(len(objects))
        ns = df_pd['NivelServico']*100
        
        plt.barh(y_pos, ns, align='center', alpha=0.7,)
        plt.yticks(y_pos, objects)
        plt.xlabel('NS')
        plt.title(filename)
        
        self.create_path_if_not_exists('img')
        plt.savefig(f'img/{filename}.png')
        if show:
            plt.show()
        return df_pd
            
    def top_10_motivos_atrasos(self,show=False):
        dados = self.execute_sql("""
        SELECT 
                 
                LEFT(`Codigo.Justificativa`,40) AS JUSTIFICATIVA, 
                 X.QTD,
                 COUNT(1) AS TOTAL,
                 (COUNT(1) / QTD) * 100 AS PERC_TOTAL
                
        FROM VOO  
        LEFT JOIN(
                SELECT     
                    SUM(1) AS QTD
                FROM VOO  
                WHERE   `Partida.Prevista` LIKE '2017%' 
                AND LOWER(`Pais.Destino`) NOT LIKE LOWER("brasil%") 
                AND `Companhia.Aerea` NOT LIKE 'NAO INFORMADO' 
                AND `Codigo.Justificativa` NOT LIKE 'NA'
                -- AND `Situacao.Voo` NOT LIKE 'Cancelado'
                AND `Partida.Real` > `Partida.Prevista`
                
        )AS X  
        WHERE   `Partida.Prevista` NOT LIKE '2020%' 
        AND LOWER(`Pais.Destino`) NOT LIKE LOWER("brasil%") 
        AND `Companhia.Aerea` NOT LIKE 'NAO INFORMADO' 
        AND `Codigo.Justificativa` NOT LIKE 'NA'
        AND `Situacao.Voo` NOT LIKE 'Cancelado' 
        AND `Partida.Real` > `Partida.Prevista`
        GROUP BY 1,2
        ORDER BY PERC_TOTAL DESC
        LIMIT 10
        """)
        df = dados.toPandas()
        
        objects = df['JUSTIFICATIVA']
        y_pos = np.arange(len(objects))
        performance = df['TOTAL']
        
        plt.barh(y_pos, performance, align='center', alpha=0.7,)
        plt.yticks(y_pos, objects)
        plt.xlabel('Qtd. Atrasos')
        plt.title('Top 10 Principais Motivos de Atrasos')
        
        self.create_path_if_not_exists('img')
        plt.savefig('img/top_10_motivos_atrasos.png')
        if show:
            plt.show()
            
            
    def cancelamentos_por_ano_mes(self,show=False):
        query = """
        -- CANCELAMENTOS AO LONGO DOS ANOS
        SELECT    
            DATE_FORMAT(`Partida.Prevista`,'MM') AS mes,
            COUNT(CASE WHEN YEAR(`Partida.Prevista`) = 2015 THEN 1 END) AS _2015 ,
            COUNT(CASE WHEN YEAR(`Partida.Prevista`) = 2016 THEN 1 END) AS _2016 ,
            COUNT(CASE WHEN YEAR(`Partida.Prevista`) = 2017 THEN 1 END) AS _2017 
            
        FROM voo
        WHERE UPPER(`Pais.Origem`) LIKE '%BRASIL%'
        AND UPPER(`Pais.Destino`) NOT LIKE '%BRASIL%'
        -- AND `Partida.Prevista` LIKE '2017%' 
        AND `Situacao.Voo` LIKE '%Cancelado%'
        GROUP BY 1
        ORDER BY 1 
        
        """
        
        dados = self.execute_sql(query).toPandas()
        
        columns = dados.columns.drop(['mes'])
        x_data = range(0, dados.shape[0])
        fig, ax = plt.subplots()
        
        leg = []
        for column in columns:
            ax.plot(x_data, dados[column])
            leg.append(column.replace("_",""))
        ax.set_title('Cancelamentos de Voos Internacionais')
        
        ax.legend(leg)
        plt.savefig('img/cancelamentos_por_ano.png')
        
        if show:
            plt.show()
            
    def total_de_voos_ano_mes(self,show=False):
        query = """
        -- VOOS AO LONGO DOS ANOS
        SELECT    
            DATE_FORMAT(`Partida.Prevista`,'MM') AS mes,
            COUNT(CASE WHEN YEAR(`Partida.Prevista`) = 2015 THEN 1 END) AS _2015 ,
            COUNT(CASE WHEN YEAR(`Partida.Prevista`) = 2016 THEN 1 END) AS _2016 ,
            COUNT(CASE WHEN YEAR(`Partida.Prevista`) = 2017 THEN 1 END) AS _2017 
            
        FROM voo
        WHERE UPPER(`Pais.Origem`) LIKE '%BRASIL%'
        AND UPPER(`Pais.Destino`) NOT LIKE '%BRASIL%'
        
        GROUP BY 1
        ORDER BY 1 
        
        """
        
        
        
        dados = self.execute_sql(query).toPandas()
        
        columns = dados.columns.drop(['mes'])
        x_data = range(0, dados.shape[0])
        fig, ax = plt.subplots()
        
        leg = []
        for column in columns:
            ax.plot(x_data, dados[column])
            leg.append(column.replace("_",""))
        ax.set_title('Quantidade de Voos Internacionais')
        
        ax.legend(leg)
        plt.savefig('img/qtd_voos_por_ano.png')
        if show:
            plt.show()



            
       

        
        
        