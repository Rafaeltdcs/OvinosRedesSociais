#!/usr/bin/env python
# coding: utf-8

import sqlite3
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Criando conexão com BD
def db_conection_eng():
    '''retorna a engine de conexao'''
    engine = create_engine('sqlite:///ovinos.db', echo=False)
    sqlite_connection = engine.connect()
    return sqlite_connection

def db_conection_session():
    '''retorna a session da conexao'''
    engine = create_engine('sqlite:///ovinos.db', echo=False)
    sqlite_connection = engine.connect()
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

# Carrega os dados
def load_dimensao_youtube():
    '''carrega os dados no banco dimensao_youtube'''
    sqlite_conn = db_conection_eng()
    df = pd.read_sql('dimensao_youtube', sqlite_conn)
    return df

def load_description_youtube():
    '''carrega os dados no banco description_youtube'''
    sqlite_conn = db_conection_eng()
    df = pd.read_sql('description_youtube', sqlite_conn)
    return df

def load_views_youtube():
    '''carrega os dados no banco views_youtube'''
    sqlite_conn = db_conection_eng()
    df = pd.read_sql('views_youtube', sqlite_conn)
    return df

# Escreve os dados
def wr_dimensao_youtube(df):
    '''escreve os dados no banco dimensao_youtube'''
    df.set_index('id',drop=True, inplace=True)
    df = df.convert_dtypes(infer_objects=False)
    sqlite_conn = db_conection_eng()
    df = df.to_sql('dimensao_youtube', sqlite_conn, if_exists='append')
    session = db_conection_session()
    session.commit()
    return df

def wr_description_youtube(df):
    '''escreve os dados no banco description_youtube'''
    df.set_index('id', drop=True, inplace=True)
    df = df.convert_dtypes(infer_objects=False)
    sqlite_conn = db_conection_eng()
    df = df.to_sql('description_youtube', sqlite_conn, if_exists='append')
    session = db_conection_session()
    session.commit()
    return df

def wr_views_youtube(df):
    '''escreve os dados no banco views_youtube'''
    df.set_index('id', drop=True, inplace=True)
    df = df.convert_dtypes(infer_objects=False)
    sqlite_conn = db_conection_eng()
    df = df.to_sql('views_youtube', sqlite_conn, if_exists='append')
    session = db_conection_session()
    session.commit()
    return df

