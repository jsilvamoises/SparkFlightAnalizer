# SparkFlightAnalizer
Analisador de voos do Brasil

## Dependencias
para executar esse projeto siga as dicas abaixo
1) Verifique se possui o Java 8 Instalado se não baixo no site da oracle
2) Instale o Anaconda atravéz do site 
3) Baixe o Spark atravéz do site e descompate em uma pasta no C:/spark no windows ou no /home/usuario/spark no linux
4) Clone esse repositório em sua maquina
5) abra o terminal ou cmd e execute os comando abaixo:


```python
pip install folium
pip install findspark
pip install wordcloud
```

6) Abra o arquivo src/spark_config.py e veja se está apontando corretamente para a pasta do spark
se não altere para a pasta que descompacou o spark
7) Se estiver no windows coloque o arquivo winutils.exe dentro da pasta bin do spark
8) Crie uma pasta com nome data na raiz do projeto e baixe o csv https://www.dropbox.com/sh/tr65659z6xa8dzj/AABdNKMO7YhoCaKMEiM5_9z7a?dl=1 dentro dela
9) atravez do terminal vá até o root do projeto e digite o comando abaixo

```python
python start.py -s false
# false não abre os graficos gerados durante o processo
# true mostra os graficos conforme vai gerando

```
10) Se arquivo deve conter todas essas pastas
![Image description](https://www.dropbox.com/s/ijkoseisqkuqbr0/captura_pastas.PNG?dl=1)
* as pastas csv, html e img são criadas na execução do sistema
