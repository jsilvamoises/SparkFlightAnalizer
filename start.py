# -*- coding: utf-8 -*-
from src.flight_analizer import FlightAnalizer
import argparse


if __name__== '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-show',help='Mostrar graficos')
    args = parser.parse_args()
    if args.show:
        s = args.show.lower() == 'true'.lower()
    else:
        s = True


    try:
        fa = FlightAnalizer()
    except:
        print('Já existe uma instância em execução: ')
    
    print('Loading data....')
    fa.load_data(file='data/br_flights.csv',view_name='voo',sep=',')
    print('Processando piores NS')
    fa.top_10_nivel_servico(ordem='asc',show=s)
    print('Processando melhores NS')
    fa.top_10_nivel_servico(ordem='desc',show=s)
    print('Criando mapa de calor com destinos')
    fa.criar_mapa_calor(s)
    print('Criando nuvem de palavras com principais destinos')
    fa.criar_word_cloud_destinos(s)
    print('Processando 10 principais motivos de atraso')
    fa.top_10_motivos_atrasos(s)
    print('Processando cancelamentos por ano / mês')
    fa.cancelamentos_por_ano_mes(show=s)
    print('Processando qtd voos internacionais ano / mês ')
    fa.total_de_voos_ano_mes(show=s)
    
    fa.print_finish()
    
    fa.app.stop_app()

