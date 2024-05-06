#!/usr/bin/env python
# coding: utf-8

# In[54]:


import requests
import pandas as pd
import math
import pyodbc
from datetime import datetime
from pytz import timezone

#####################################################################################
## BUSQUEDAD DEL ID SEGUN EL ESTADO DEL TICTECK
#####################################################################################

# URL de la API que queremos consumir
url = 'https://mda.dpworldchile.cl/api/v1/incidents.by.status?'

# Credenciales de autenticación
username = 'conexion'
password = '7teOmvKNzymGsFUeAkHJA7FL'

# Datos a enviar en el body de la solicitud
data = {
    "status_ids": [1,2,3,4,5,6,7,8,9,10]
}

# Encabezados de la solicitud
headers = {
    "Content-Type": "application/json"
}

# Realizar una solicitud POST a la API con autenticación básica y body RAW
response = requests.get(url, auth=(username, password), headers=headers, json=data)
print(data)

# # Realizar una solicitud GET a la API con autenticación básica
# response = requests.get(url, auth=(username, password))

# Verificar el código de estado de la respuesta
if response.status_code == 200:
    # Si la respuesta es exitosa, mostrar los datos obtenidos
    data = response.json()
  #  print(data)
    campo={'requestIds': data['requestIds'] }
    df=pd.DataFrame(campo)     
else:
    # Si la respuesta no es exitosa, mostrar el código de estado y el mensaje de error
    print(f'Error: {response.status_code} - {response.reason}')
    
    
#####################################################################################
## DETALLE DE TICTECK
#####################################################################################
    

df['requestIds']=df['requestIds'].astype(str)

    
row = len(df['requestIds'])
print(math.ceil(row/5000))    

for i in range(math.ceil(row/5000)):
 ids= ','.join(df['requestIds'][:5000].tolist())
 

 # URL de la API que queremos consumir
 url = 'https://mda.dpworldchile.cl/api/v1/incidents'

 # Credenciales de autenticación
 username = 'conexion'
 password = '7teOmvKNzymGsFUeAkHJA7FL'



 # Datos a enviar en el body de la solicitud
 data = {
    "ids": '['+ids+']'
 }

 # Encabezados de la solicitud
 headers = {
    "Content-Type": "application/json"
 }



 valor = data['ids']
 lista = eval(valor)
 data['ids'] = lista


 # Realizar una solicitud POST a la API con autenticación básica y body RAW
 response = requests.get(url, auth=(username, password), headers=headers, json=data)

 # Verificar el código de estado de la respuesta
 if response.status_code == 200:
    # Si la respuesta es exitosa, mostrar los datos obtenidos
    dat = response.json()
    dfa = pd.DataFrame.from_dict(dat, orient='columns')
    df_transpuesto = dfa.transpose()
    
    
    
 # conectarse a la base de datos
    server = '10.40.190.178'
    database = 'AppInvgate'
    username = 'AppInvgateUser'
    password = 'InvgateApp.2022'
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

# crear un cursor para la conexión
    cursor = cnxn.cursor()


    df_transpuesto['title'] = df_transpuesto['title'].apply(lambda x: '' if str(x) == "'" else str(x))
    df_transpuesto['description'] = df_transpuesto['description'].apply(lambda x: '' if str(x) == "'" else str(x))
    df_transpuesto['description'] = df_transpuesto['description'].apply(lambda x: '' if str(x) == '"' else str(x))
    df_transpuesto['description'] = df_transpuesto['description'].apply(lambda x: x.replace("'",''))
    df_transpuesto['title'] = df_transpuesto['title'].apply(lambda x: x.replace("'",''))
    df_transpuesto['date_ocurred'] = pd.to_datetime(df_transpuesto['date_ocurred'], unit='s')
    df_transpuesto['created_at'] = pd.to_datetime(df_transpuesto['created_at'], unit='s')
    df_transpuesto['last_update'] = pd.to_datetime(df_transpuesto['last_update'], unit='s')
    df_transpuesto['solved_at'] = pd.to_datetime(df_transpuesto['solved_at'], unit='s')
    df_transpuesto['closed_at'] = pd.to_datetime(df_transpuesto['closed_at'], unit='s')
    df_transpuesto['solved_at'] = df_transpuesto['solved_at'].fillna('1999-01-01 00:00:00')
    df_transpuesto['closed_at'] = df_transpuesto['closed_at'].fillna('1999-01-01 00:00:00')
    df_transpuesto['solved_at'] = pd.to_datetime(df_transpuesto['solved_at'])
    df_transpuesto['closed_at'] = pd.to_datetime(df_transpuesto['closed_at'])
    df_transpuesto['process_id'] = df_transpuesto['process_id'].fillna(0)
    df_transpuesto['closed_reason'] = df_transpuesto['closed_reason'].fillna(0)
    df_transpuesto['location_id'] = df_transpuesto['location_id'].fillna(0)
    df_transpuesto['rating'] = df_transpuesto['rating'].fillna(0)
    df_transpuesto['priority_id'] = df_transpuesto['priority_id'].fillna(0)
    df_transpuesto['user_id'] = df_transpuesto['user_id'].fillna(0)
    df_transpuesto['creator_id'] = df_transpuesto['creator_id'].fillna(0)
    df_transpuesto['assigned_id'] = df_transpuesto['assigned_id'].fillna(0)
    df_transpuesto['assigned_group_id'] = df_transpuesto['assigned_group_id'].fillna(0)
    df_transpuesto['source_id'] = df_transpuesto['source_id'].fillna(0)
    df_transpuesto['status_id'] = df_transpuesto['status_id'].fillna(0)
    df_transpuesto['type_id'] = df_transpuesto['type_id'].fillna(0)
    df_transpuesto['category_id'] = df_transpuesto['category_id'].fillna(0)
    df_transpuesto['date_ocurred'] = df_transpuesto['date_ocurred'].apply(lambda x: datetime.strptime(x.strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S'))
    df_transpuesto['created_at'] = df_transpuesto['created_at'].apply(lambda x: datetime.strptime(x.strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S'))
    df_transpuesto['last_update'] =df_transpuesto['last_update'].apply(lambda x: datetime.strptime(x.strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S'))
    df_transpuesto['solved_at'] = df_transpuesto['solved_at'].apply(lambda x: datetime.strptime(x.strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S'))
    df_transpuesto['closed_at'] = df_transpuesto['closed_at'].apply(lambda x: datetime.strptime(x.strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S'))
    df_transpuesto['date_ocurred'] = df_transpuesto['date_ocurred'].dt.strftime('%d-%m-%Y %H:%M:%S')
    df_transpuesto['created_at'] = df_transpuesto['created_at'].dt.strftime('%d-%m-%Y %H:%M:%S')
    df_transpuesto['last_update'] = df_transpuesto['last_update'].dt.strftime('%d-%m-%Y %H:%M:%S')
    df_transpuesto['solved_at'] = df_transpuesto['solved_at'].dt.strftime('%d-%m-%Y %H:%M:%S')
    df_transpuesto['closed_at'] = df_transpuesto['closed_at'].dt.strftime('%d-%m-%Y %H:%M:%S')

# establecer la zona horaria de Chile
#chile_tz = timezone('Chile/Continental')
#df_transpuesto['created_at'] = df_transpuesto['created_at'].dt.tz_localize(chile_tz)


    Cadena2 = "INSERT INTO INV_TICKET_INCIDENT_Prueba (id, TITLE, CATEGORY_ID,DESCRIPTION,PRIORITY_ID,USER_ID,CREATOR_ID,ASSIGNED_ID,ASSIGNED_GROUP_ID,DATE_OCURRED,SOURCE_ID,STATUS_ID,TYPE_ID,CREATED_AT,LAST_UPDATE,PROCESS_ID,SOLVED_AT,CLOSED_AT,CLOSED_REASON,DATA_CLEANED,LOCATION_ID,RATING,SLA_INCIDENT_RESOLUTION,SLA_INCIDENT_FIRST_REPLY,CUSTOM_FIELDS) VALUES "
    i=0
    while i<len(df_transpuesto):
                        Cadena1 = ''
                        for s in range(i, len(df_transpuesto)):
                            Cadena1 = Cadena1 + "," + "('%s', '%s', '%s', '%s', '%s', '%s','%s','%s','%s', '%s', '%s', '%s', '%s', '%s','%s','%s','%s', '%s', '%s', '%s', '%s', '%s','%s','%s','%s')" %(str(df_transpuesto.iloc[s]['id']), str(df_transpuesto.iloc[s]['title']), str(df_transpuesto.iloc[s]['category_id']), str(df_transpuesto.iloc[s]['description']), str(df_transpuesto.iloc[s]['priority_id']), str(df_transpuesto.iloc[s]['user_id']), str(df_transpuesto.iloc[s]['creator_id']),  str(df_transpuesto.iloc[s]['assigned_id']),  str(df_transpuesto.iloc[s]['assigned_group_id']),  str(df_transpuesto.iloc[s]['date_ocurred']),  str(df_transpuesto.iloc[s]['source_id']),  str(df_transpuesto.iloc[s]['status_id']),  str(df_transpuesto.iloc[s]['type_id']),  str(df_transpuesto.iloc[s]['created_at']),  str(df_transpuesto.iloc[s]['last_update']),  str(df_transpuesto.iloc[s]['process_id']),  str(df_transpuesto.iloc[s]['solved_at']),  str(df_transpuesto.iloc[s]['closed_at']),  str(df_transpuesto.iloc[s]['closed_reason']),  str(df_transpuesto.iloc[s]['data_cleaned']),  str(df_transpuesto.iloc[s]['location_id']),  str(df_transpuesto.iloc[s]['rating']),  str(df_transpuesto.iloc[s]['sla_incident_resolution']),  str(df_transpuesto.iloc[s]['sla_incident_first_reply']),  str(df_transpuesto.iloc[s]['custom_fields']))
                            i = i+1
                            if (i%1000) ==0:
                                break
                        Cadena1 = Cadena1[1:]
                        Cadena = Cadena2 + Cadena1
                        print(Cadena)
                        #source.__insert__(Cadena)
                       # cursor.execute(Cadena)


# confirmar los cambios en la base de datos
    cnxn.commit()

# cerrar el cursor y la conexión
    cursor.close()
    cnxn.close()
    print(math.ceil(row/5000))

 else:
    # Si la respuesta no es exitosa, mostrar el código de estado y el mensaje de error
    print(f'Error: {response.status_code} - {response.reason}')
 df.drop(df.index[:5000],axis=0,inplace=True)    

    
    


# In[47]:






# In[ ]:




