from asyncore import read
import requests
from  decouple import config
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, DateTime,Integer,String
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
import pandas as pd
import os
import shutil
from datetime import datetime


Base=declarative_base()

engine = create_engine(config('POSTGRESQL_URI')

class Registros(Base):
    __tablename__ = 'Registros'
    id=Column(Integer,primary_key=True)
    cod_localidad=Column(String(50),nullable=False)
    id_provincia=Column(String(50))
    id_departamento=Column(String(50))
    categoría=Column(String(50),nullable=False)
    provincia=Column(String(50),nullable=False)
    localidad=Column(String(50),nullable=False)
    nombre=Column(String(50),nullable=False)
    domicilio=Column(String(50),nullable=False)
    código_postal=Column(String(50),nullable=False)
    teléfono=Column(String(50),nullable=False)
    mail=Column(String(50),nullable=False)
    web=Column(String(50),nullable=False)
    create_at=Column(DateTime(),default=datetime.now())


    def __str__(self):
        return self.cod_localidad


def guardarRegistros(csv):

    csv.to_sql('Registros', con=engine, index=True, index_label='id', if_exists='append')

def get_cvs (name,URL):    
    data_prueba=requests.get(URL)
    soup=BeautifulSoup(data_prueba.content,'html.parser')
    tags=soup('a')
    flag=True
    for tag in tags:
         data=tag.get('href')
         if(data.find(name)>0 and flag):

             month=datetime.today().strftime('%Y-%B')
             today=datetime.today().strftime("%Y-%m-%d")
             
             if(os.path.exists(f"{name}")):
                 shutil.rmtree(f"{name}")
                 path=f'{name}\{month}'
                 os.makedirs(path)
                 open(f'{path}\{name}-{today}.csv', 'wb').write(requests.get(data).content)
                 dataCsv=pd.read_csv(f'{path}\{name}-{today}.csv')
                 guardarRegistros(dataCsv)  
                 flag=False
             else :
                 path=f'{name}\{month}\{name}-{today}'
                 os.makedirs(path)           

print(datetime.today().strftime('%Y-%m-%d'))


cine= get_cvs('cine',config('URL_CINE'))
museo=get_cvs('museo',config('URL_MUSEO'))
biblioteca=get_cvs('biblioteca_popular',config('URL_BIBLIOTECA'))

Session=sessionmaker(engine)
session=Session()


if __name__ == '__main__':
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    
