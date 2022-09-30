"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd
import re

def ingest_data():
    raw_list=list()
    with open('clusters_report.txt') as file:
        raw_list=list(file.readlines())
        file.close()


    lista=[' '.join(e.split()) for e in raw_list]
    lista=[i.replace('\n','').lower().strip() for i in lista][4:]
    lista=[e.replace(' % ',' ') for e in lista]
    lista=[e.replace('.','') for e in lista]
    lista=[e.replace('-','~') for e in lista]

    lista_f=list()
    for e in lista:
        if len(re.findall(r'[0-9](,)[0-9]',e))>0:
            e=re.sub(r'(,)','.',e,count=1)
        e=re.sub(r'(, )','|',e)
        e=re.sub(r'(,)','|',e)
        if len(re.findall(r'\s+',e))>0:
            e=re.sub(r'\s+','|',e,count=3)
        e=e.replace(chr(32),'-')
        lista_f.append(e)

    my_list=list()
    for i in lista_f:
        g=list(i.split('|'))
        my_list.append(g)

    for subl in my_list:
        for k in subl:
            if k=='':
                subl.remove(k)

    for subl in my_list:
        if len(subl)==0:
            my_list.remove(subl)


    resultado = []
    auxiliar = []
    for sublista in my_list:
        if not sublista[0].isdigit():
            auxiliar=auxiliar+sublista
        else:
            auxiliar = []
            auxiliar=sublista
        resultado.append(auxiliar)


    df=pd.DataFrame()
    for row in resultado:
        row=pd.DataFrame(row).T
        df=pd.concat([df,row],axis=0,copy=False)

    cols=list()
    for t in range(df.shape[1]):
        c='col'+str(t)
        cols.append(c)

    df.set_axis(cols,axis=1,inplace=True)
    
    df['final_col']=pd.Series()

    max=df.shape[1]-1
    for r in range(3,max):
        df['final_col']=df['final_col'].apply(str)+','+df['col'+str(r)].apply(str)

    df['final_col']=df['final_col'].apply(lambda x: re.sub(r'nan,','',x))
    df['final_col']=df['final_col'].apply(lambda x: re.sub(r',nan','',x))
    df=df[['col0','col1','col2','final_col']]
    df.set_axis(['cluster','cantidad_de_palabras_clave','porcentaje_de_palabras_clave','principales_palabras_clave'],axis=1,inplace=True)
    df.drop_duplicates(subset='cluster',keep='last',inplace=True)
    
    df['cluster']=df['cluster'].astype('int64')
    df['cantidad_de_palabras_clave']=df['cantidad_de_palabras_clave'].astype('int64')
    df['porcentaje_de_palabras_clave']=df['porcentaje_de_palabras_clave'].astype('float64')
    df['principales_palabras_clave']=df['principales_palabras_clave'].apply(lambda x: x.replace('-',chr(32)))
    df['principales_palabras_clave']=df['principales_palabras_clave'].apply(lambda x: x.replace('~','-'))

    return df

