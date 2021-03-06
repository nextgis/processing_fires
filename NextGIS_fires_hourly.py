#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import json
import pyproj
import os
import argparse
import datetime
import tempfile
import csv
import geojson
from geojson import Feature, FeatureCollection, Point
from requests.auth import HTTPBasicAuth
from shapely.geometry import shape, Point
from functools import partial
from shapely.ops import transform
from requests_futures.sessions import FuturesSession

#python NextGIS_fires_hourly.py --name MODIS --latmin 40 --lonmin 40 --latmax 60 --lonmax 60 --url elina-usmanova --login administrator --password 123456 --parentid 175 --styleid 454
#python NextGIS_fires_hourly.py --name VIIRS --latmin 40 --lonmin 40 --latmax 60 --lonmax 60 --url elina-usmanova --login administrator --password 123456 --parentid 175 --styleid 456

parser = argparse.ArgumentParser()
parser.add_argument('--name',type=str,required=True)
parser.add_argument('--latmin',type=float,required=True)
parser.add_argument('--lonmin',type=float,required=True)
parser.add_argument('--latmax',type=float,required=True)
parser.add_argument('--lonmax',type=float,required=True)
parser.add_argument('--url',type=str,required=True)
parser.add_argument('--login',type=str,required=True,default='administrator')
parser.add_argument('--password',type=str,required=True)
parser.add_argument('--parentid',type=int,required=True)
parser.add_argument('--styleid',type=int,required=True)

args = parser.parse_args()

path = tempfile.gettempdir()
path = path.replace('\\','/')
path = path + '/'
date = datetime.datetime.now().strftime('%d-%m-%Y %H-%M')

def download_files(file):

        #Скачивание файлов
    response = requests.get('https://firms.modaps.eosdis.nasa.gov/data/active_fire/' + file)
    name = file.split('/')
    file = open(path + date + '_' + name[2],'wb')
    n = name[2].split('.')
    file.write(response.content)
    file.close()
    return name

def columns_name():
    file = open(path + date + '_' + name[2],'r')
    read = file.readline()
    linename = read.split(',')
    return linename

def extent(latmin, lonmin, latmax, lonmax):
    #Функция обрезки по экстенту
    rb = open(path + date + '_' + name[2])
    lines = rb.readlines()

    data = []
    for f in range(1,len(lines)):
        if float(lines[f].split(',')[0]) > latmin and float(lines[f].split(',')[0]) < latmax:
            if float(lines[f].split(',')[1]) > lonmin and float(lines[f].split(',')[1]) < lonmax:
                data.append(lines[f])
    rb.close()
    file = open(path + date + '_' + name[2].split('.')[0] + '_extent.csv', "w", newline='')
    file.write(lines[0])
    for d in range(len(data)):
        file.write(data[d])
    file.close()

def format_time():
    #Смена формата времени
    f = path + date + '_' + name[2].split('.')[0] + '_extent.csv'
    f_out = path + date + '_' + name[2].split('.')[0] + '_extent_out.csv'
    f_csv = csv.DictReader(open(f))
    f_out = csv.DictWriter(open(f_out,'w'), fieldnames = f_csv.fieldnames)
    for row in f_csv:
        time = (row['acq_time'])
        row['acq_time'] = time[:2] + ':' + time[-2:]
        f_out.writerow(row)
    f2 = open(path + date + '_' + name[2].split('.')[0] + '_for_geojson.csv', 'w')
    with open(path + date + '_' + name[2].split('.')[0] + '_extent_out.csv', 'r') as f1:
        for line in f1.readlines():
            if line.strip() == '':
                continue
            f2.write(line)
    f2.close()

def create_geojson():
    #Создание geojson
    file = open(path + date + '_' + name[2].split('.')[0] + '_for_geojson.csv', 'r')
    rlines = file.readlines()

    features = []
    for line in rlines:
        latitude = float(line.split(',')[0])
        longitude = float(line.split(',')[1])
        features.append(Feature(
            geometry = Point((longitude, latitude)),
            properties = {
                linename[0]: line.split(',')[0],
                linename[1]: line.split(',')[1],
                linename[2]: line.split(',')[2],
                linename[3]: line.split(',')[3],
                linename[4]: line.split(',')[4],
                linename[5]: line.split(',')[5],
                linename[6]: line.split(',')[6],
                linename[7]: line.split(',')[7],
                linename[8]: line.split(',')[8],
                linename[9]: line.split(',')[9],
                linename[10]: line.split(',')[10],
                linename[11]: line.split(',')[11],
                linename[12][:-1]: line.split(',')[12][:-1]
            }
        ))

    collection = FeatureCollection(features)
    with open(path + date + '_' + name[2].split('.')[0] + '.geojson', "w") as f:
        f.write('%s' % collection)

def request_post(url):
    #Загрузка на веб-ГИС
    URL = 'http://' + url + '.nextgis.com/api'
    AUTH = HTTPBasicAuth(args.login, args.password)
    session = FuturesSession(max_workers=5)

    payload = {'vector_layer': {'fields': [], 'geometry_type': None, 'srs': {'id': 3857}}, 'resource': {'display_name': None, 'keyname': None, 'parent': {'id': args.parentid}, 'description': None, 'cls': 'vector_layer'}, 'resmeta': {'items': {}}}

    with open(path + date + '_' + name[2].split('.')[0] + '.geojson') as file:
        gj = geojson.load(file)
        print(((len(gj['features']))))
        project = partial(
            pyproj.transform,
            pyproj.Proj(init='epsg:4326'),
            pyproj.Proj(init='epsg:3857'))
        payload['resource']['display_name'] = date + '_' + name[2].split('.')[0]
        payload['vector_layer']['geometry_type'] = "POINT"
        l = [(linename[0], 'REAL'), (linename[1], 'REAL'), (linename[2], 'REAL'), (linename[3], 'REAL'), (linename[4], 'REAL'), (linename[5], 'STRING'), (linename[6], 'STRING'), (linename[7], 'STRING'), (linename[8], 'STRING'), (linename[9], 'STRING'), (linename[10], 'REAL'), (linename[11], 'REAL'), (linename[12][:-1], 'STRING')]
        for g in l:
            payload['vector_layer']['fields'].append({
                'keyname': g[0],
                'datatype': g[1]
            })
        r = requests.post('%s/resource/' % (URL,), data=json.dumps(payload), auth=AUTH)
        print(r.text)

        for h in range(len(gj['features'])):
            gj[h]['id'] = h + 1
        for f in gj['features']:
            try:
                payload = {
                    'geom': str(transform(project, shape(f['geometry']))),
                    'fields': f['properties']
                }
            except:
                print(shape(f['geometry']))
                raise
            session.post('%s/resource/%d/feature/' % (URL, r.json()['id']), data=json.dumps(payload), auth=AUTH)
            
        current_id = r.json()['id']
        return current_id

def find_previous_parent_id(url):
    #Находим id предыдущего слоя, в котором лежал стиль
    URL = 'http://' + url + '.nextgis.com/api'
    AUTH = HTTPBasicAuth(args.login, args.password)
    v = requests.get('%s/resource/%s' % (URL, args.styleid), auth=AUTH)
    dic = json.loads(v.text)
    previous_parent_id = dic['resource']['parent']['id']
    return previous_parent_id

def update_parent_id():
    payload = {"resource": {'parent': {'id': current_id}}}
    req = requests.put('http://' + args.url + '.nextgis.com/api/resource/%s' % (args.styleid,), data=json.dumps(payload), auth=HTTPBasicAuth(args.login, args.password))

def delete_previous_layer(url):
    #Удаление предыдущего слоя
    URL = 'http://' + url + '.nextgis.com/api'
    AUTH = HTTPBasicAuth(args.login, args.password)
    req = requests.delete('%s/resource/%s' % (URL, previous_parent_id), auth=AUTH)
    
def delete(name):
    os.remove(path + date + '_' + name[2])
    name = name[2].split('.')
    os.remove(path + date + '_' + name[0] + '_extent.csv')
    os.remove(path + date + '_' + name[0] + '_extent_out.csv')
    os.remove(path + date + '_' + name[0] + '_for_geojson.csv')
    os.remove(path + date + '_' + name[0] + '.geojson')

if __name__ == '__main__':
    if args.name == 'MODIS':
        file = 'c6/csv/MODIS_C6_Russia_and_Asia_24h.csv'
        display_name = "map_style_MODIS"
    else:
        file = 'viirs/csv/VNP14IMGTDL_NRT_Russia_and_Asia_24h.csv'
        display_name = "map_style_VIIRS"
    print(file)
    name = download_files(file)
    print(name)
    linename = columns_name()
    extent(args.latmin, args.lonmin, args.latmax, args.lonmax)
    format_time()
    create_geojson()
    current_id = request_post(args.url)
    previous_parent_id = find_previous_parent_id(args.url)
    update_parent_id()
    delete_previous_layer(args.url)
    print(current_id)
    delete(name)

