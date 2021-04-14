#!/usr/bin/env python
# coding: utf-8

# ## Captura das informações
# 
# 1. Coletar todos dos videos da query
# 2. Coletar todas de cada video a description completa
# 3. Montar um banco de dados com essas informações
# 4. Coletar diáriamente a quantidade de views, likes e dislikes de cada video
# 5. Montar uma série temporal com a informação acima
# 6. Semanalmente pegar todos os videos novamente e incluir os novos
# 
# ## Output dos dados
# 
# 1. EAD: Quantidade de videos, qt de views, qt de produtores de videos
# 2. Variação dos itens acima por dia, semana, mês
# 3. ML: criar modelo para prever a quantidade de novos views
# 4. NLP: descobrir cortes com mais videos
# 5. NLP: descobrir novos "canais" na descrição
# 6. Coletar tags para novas querys

import youtube_dlc
import pandas as pd
import numpy as np
import datetime
import db

class Coletor():

    def __init__(self):
        self.ydl = youtube_dlc.YoutubeDL({"ignoreerrors": True})
    
    def busca_query(self, query):
        '''Passa lista com querys que devem ser procuradas'''
        coletas = dict()
        for q in query:
            coletas[q] = self.ydl.extract_info("ytsearch:{}".format(q), download=False) #mudar para searchall
        return coletas
    
    def get_db_id(self):
        '''Pega os ids no banco de dados e passa para o get_descricao'''
        df = db.load_dimensao_youtube()
        df = df[['id']]
        return df
    
    def get_descricao(self, df):
        '''Pegar a descrição do video'''
        coletas = dict()
        for i in df['id']:
            info = self.ydl.extract_info('https://www.youtube.com/watch?v={}'.format(i), download=False)
            coletas[i] = info['description']
        coletas = pd.DataFrame.from_dict(coletas, orient='index')
        coletas.reset_index(inplace=True)
        coletas.columns = ['id','description']
        return coletas
    
    def get_views(self, df):
        '''Pegar a descrição do video'''
        coletas = dict()
        for i in df['id']:
            info = self.ydl.extract_info('https://www.youtube.com/watch?v={}'.format(i), download=False)
            coletas[i] = info['view_count'],info['like_count'],info['dislike_count'],info['average_rating']
        coletas = pd.DataFrame.from_dict(coletas, orient='index')
        coletas.reset_index(inplace=True)
        coletas.columns = ['id','view_count','like_count','dislike_count','average_rating']
        coletas['data'] = datetime.datetime.now()
        return coletas
