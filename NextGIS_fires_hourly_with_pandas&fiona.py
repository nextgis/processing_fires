#!/usr/bin/env python
# -*- coding: utf-8 -*-

# In[ ]:


import requests
import pandas as pd
import json
import fiona
import pyproj
import os
import sys
import argparse
import datetime
import tempfile
from requests.auth import HTTPBasicAuth
from shapely.geometry import shape, Point, Polygon
from functools import partial
from shapely.ops import transform
from requests_futures.sessions import FuturesSession

#python NextGIS_fires_hourly.py --name MODIS --ymin 46 --xmin 46 --ymax 50 --xmax 50 --url elina-usmanova.nextgis.com --login administrator --password 123456 --parentid 175

parser = argparse.ArgumentParser()
parser.add_argument('--name',type=str,required=True)
parser.add_argument('--ymin',type=float,required=True)
parser.add_argument('--xmin',type=float,required=True)
parser.add_argument('--ymax',type=float,required=True)
parser.add_argument('--xmax',type=float,required=True)
parser.add_argument('--url',type=str,required=True)
parser.add_argument('--login',type=str,required=True,default='administrator')
parser.add_argument('--password',type=str,required=True)
parser.add_argument('--parentid',type=int,required=True)

args = parser.parse_args()

path = tempfile.gettempdir()
path = path.replace('\\','/')
path = path + '/'
date = datetime.datetime.now().strftime('%d-%m-%Y %H:%M')

def download_files(file):
    #file = 'c6/csv/MODIS_C6_Russia_and_Asia_24h.csv'

        #Скачивание файлов
    response = requests.get('https://firms.modaps.eosdis.nasa.gov/data/active_fire/' + file)
    name = file.split('/')
    file = open(path + date + '_' + name[2],'wb')
    n = name[2].split('.')
    file.write(response.content)
    file.close()
    return name


def create_geojson():
    #Создание geojson
    df = pd.read_csv(path + date + '_' + name[2],
                    infer_datetime_format = True,
                    na_values = [''])
    df[df.columns[:5]].head()
    json_result_string = df.to_json(
        orient = 'records',
        double_precision = 12)
    json_result = json.loads(json_result_string)

    geojson = {
        'type': 'FeatureCollection',
        'name': name[2].split('.')[0],
        'features': []
    }
    for record in json_result:
        geojson['features'].append({
            'type': 'Feature',
            'geometry': {
                'type': 'Point',
                'coordinates': [record['longitude'], record['latitude']],
            },
            'properties': record,
        })

    with open(path + date + '_' + name[2].split('.')[0] + '_source.geojson', 'w') as f:
        f.write(json.dumps(geojson, indent = 2))
    



def extent(ymin, xmin, ymax, xmax):
        #Функция обрезки по экстенту
    with fiona.open(path + date + '_' + name[2].split('.')[0] + '_source.geojson') as source:
        meta = source.meta
        meta['schema']['geometry'] = 'Point'

        with fiona.open(path + date + '_' + name[2].split('.')[0] + '.geojson', 'w', **meta) as out:

            for f in source.filter(bbox=(ymin, xmin, ymax, xmax)):

                f['geometry'] = {
                    'type': 'Point',
                    'coordinates': [f['geometry']['coordinates'][0], f['geometry']['coordinates'][1]]}

                out.write(f)
                

    
def request_post(url):
    #Загрузка на веб-ГИС
    URL = 'http://' + url + '/api'
    AUTH = HTTPBasicAuth(args.login, args.password)
    NGW_FIELD_TYPES_MAP = {'str': 'STRING', 'int': 'INTEGER', 'float': 'REAL', 'date': 'STRING', 'time': 'TIME', 'datetime': 'DATETIME'}

    session = FuturesSession(max_workers=5)

    payload = {'vector_layer': {'fields': [], 'geometry_type': None, 'srs': {'id': 3857}}, 'resource': {'display_name': None, 'keyname': None, 'parent': {'id': args.parentid}, 'description': None, 'cls': 'vector_layer'}, 'resmeta': {'items': {}}}
    
    with fiona.open(path + date + '_' + name[2].split('.')[0] + '.geojson') as source:
        project = partial(
            pyproj.transform,
            pyproj.Proj(init=source.crs['init']),
            pyproj.Proj(init='epsg:3857'))
        payload['resource']['display_name'] = source.name
        payload['vector_layer']['geometry_type'] = source.schema['geometry'].upper()
        for k, v in source.schema['properties'].items():
            payload['vector_layer']['fields'].append({
                'keyname': k,
                'datatype': NGW_FIELD_TYPES_MAP[v.split(':')[0]]
            })

        r = requests.post('%s/resource/' % (URL,), data=json.dumps(payload), auth=AUTH)
        #vectstyle = requests.post('%s/resource/' % (URL,), json = dict(resource=dict(cls='mapserver_style', parent={'id' : r.json()['id'], 'parent' : {'id': args.parentid}}, display_name='map_style'), mapserver_style=dict(xml="<map><layer><class><style><color red=\"255\" green=\"240\" blue=\"189\"/><outlinecolor red=\"255\" green=\"196\" blue=\"0\"/></style></class></layer></map>")))
        #print(vectstyle.text)
            #mapserver_style=dict(xml="<map><layer><class><style><color red=\"255\" green=\"240\" blue=\"189\"/><outlinecolor red=\"255\" green=\"196\" blue=\"0\"/></style></class></layer></map>")}
                   
        for f in source:
            try:
                payload = {
                    'geom': transform(project, shape(f['geometry'])).wkt,
                    'fields': f['properties']
                }
            except:
                print(f['geometry'])
                raise
            session.post('%s/resource/%d/feature/' % (URL, r.json()['id']), data=json.dumps(payload), auth=AUTH)
            current_id = r.json()['id']
            
            return current_id
        
def find_previous_parent_id(url):
    #Находим id предыдущего слоя, в котором лежал стиль
    URL = 'http://' + url + '/api'
    AUTH = HTTPBasicAuth(args.login, args.password)
    v = requests.get('%s/resource/%s' % (URL, styleid), auth=AUTH)      
    dic = json.loads(v.text)
    previous_parent_id = dic['resource']['parent']['id']
    return previous_parent_id

def update_parent_id():
    payload = {"resource": {'parent': {'id': current_id}}}
    req = requests.put('http://' + args.url + '/api/resource/%s' % (styleid,), data=json.dumps(payload), auth=HTTPBasicAuth(args.login, args.password))

def delete_previous_layer(url):
    #Удаление предыдущего слоя
    URL = 'http://' + url + '/api'
    AUTH = HTTPBasicAuth(args.login, args.password)
    req = requests.delete('%s/resource/%s' % (URL, previous_parent_id), auth=AUTH)

def delete(name):
    #Удаление созданных файлов
    os.remove(path + date + '_' + name[2])
    name = name[2].split('.')
    os.remove(path + date + '_' + name[0] + '_source.geojson')
    os.remove(path + date + '_' + name[0] + '.geojson')

if __name__ == '__main__':
    #files = ['c6/csv/MODIS_C6_Russia_and_Asia_24h.csv', 'c6/csv/MODIS_C6_Russia_and_Asia_48h.csv', 'c6/csv/MODIS_C6_Russia_and_Asia_7d.csv',
        #'viirs/csv/VNP14IMGTDL_NRT_Russia_and_Asia_24h.csv', 'viirs/csv/VNP14IMGTDL_NRT_Russia_and_Asia_7d.csv']
    #for file in files:
    if args.name == 'MODIS':
        file = 'c6/csv/MODIS_C6_Russia_and_Asia_24h.csv'
        styleid = '181'
        display_name = "current_fire_MODIS"
    else:
        file = 'viirs/csv/VNP14IMGTDL_NRT_Russia_and_Asia_24h.csv'
        styleid = '184'
        display_name = "current_fire_VIIRS"
        
    print(file)
    name = download_files(file)
    print(name)
    create_geojson()
    extent(args.ymin, args.xmin, args.ymax, args.xmax)
    current_id = request_post(args.url)
    print(current_id)
    previous_parent_id = find_previous_parent_id(args.url)
    update_parent_id()
    print(current_id)
    delete_previous_layer(args.url)
    delete(name)

