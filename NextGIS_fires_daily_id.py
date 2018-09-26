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

#python NextGIS_fires_daily_id.py --name MODIS --latmin 40 --lonmin 40 --latmax 60 --lonmax 60 --url elina-usmanova --login administrator --password 123456 --parentid 617 --styleid 624
#python NextGIS_fires_daily_id.py --name VIIRS --latmin 40 --lonmin 40 --latmax 60 --lonmax 60 --url elina-usmanova --login administrator --password 123456 --parentid 342 --styleid 350

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
    print(len(features))
    collection = FeatureCollection(features)
    with open(path + date + '_' + name[2].split('.')[0] + '.geojson', "w") as geoj:
        geoj.write('%s' % collection)

def request_post(url):
    #Загрузка на веб-ГИС
    URL = 'http://' + url + '.nextgis.com/api'
    AUTH = HTTPBasicAuth(args.login, args.password)
    with open(path + date + '_' + name[2].split('.')[0] + '.geojson', 'rb') as file:
        file_r = open(path + date + '_' + name[2].split('.')[0] + '.geojson')
        gj = geojson.load(file_r)
        if len(gj['features']) > 0:
            t = requests.put("%s/component/file_upload/upload" %(URL,), data=file)
            f = t.text
            my_dict = json.loads(f)
            qgisid = my_dict["id"]
            r = requests.post('%s/resource/' % (URL,), auth=AUTH, json = {"resource":{"cls":"vector_layer","parent":{"id":args.parentid}, "display_name":date + '_' + name[2].split('.')[0],"keyname":None, "description":None}, "resmeta":{"items":{}}, "vector_layer":{"srs":{"id":3857}, "source":{"id":qgisid, "name":date + '_' + name[2].split('.')[0] + '.geojson', "mime_type":"application/octet-stream","size":308961,"encoding":"utf-8"}}})
            current_id = r.json()['id']
            return current_id
        else:
            payload = {'vector_layer': {'fields': [], 'geometry_type': None, 'srs': {'id': 3857}}, 'resource': {'display_name': None, 'keyname': None, 'parent': {'id': args.parentid}, 'description': None, 'cls': 'vector_layer'}, 'resmeta': {'items': {}}}
            payload['resource']['display_name'] = date + '_' + name[2].split('.')[0]
            payload['vector_layer']['geometry_type'] = "POINT"
            r = requests.post('%s/resource/' % (URL,), data=json.dumps(payload), auth=AUTH)
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

def create_vectstyle(url):
    URL = 'http://' + url + '.nextgis.com/api'
    AUTH = HTTPBasicAuth(args.login, args.password)
    v = requests.post('%s/resource/' % (URL,), auth=AUTH, json = {"resource":{"cls":"mapserver_style","parent":{"id":previous_parent_id},"display_name":display_name,"keyname":None,"description":None},"resmeta":{"items":{}},"mapserver_style":{"xml":"<map>\n  <symbol>\n    <type>ellipse</type>\n    <name>circle</name>\n    <points>1 1</points>\n    <filled>true</filled>\n  </symbol>\n  <layer>\n    <class>\n      <style>\n        <color blue=\"211\" green=\"177\" red=\"128\"/>\n        <outlinecolor blue=\"64\" green=\"64\" red=\"64\"/>\n        <symbol>circle</symbol>\n        <size>6</size>\n      </style>\n    </class>\n  </layer>\n  <legend>\n    <keysize y=\"15\" x=\"15\"/>\n    <label>\n      <size>12</size>\n      <type>truetype</type>\n      <font>regular</font>\n    </label>\n  </legend>\n</map>\n"}})

def update_parent_id(url):
    URL = 'http://' + url + '.nextgis.com/api'
    AUTH = HTTPBasicAuth(args.login, args.password)
    payload = {"resource": {'parent': {'id': current_id}}}
    req = requests.put('%s/resource/%s' % (URL,args.styleid,), data=json.dumps(payload), auth=AUTH)

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
    update_parent_id(args.url)
    create_vectstyle(args.url)
    print(current_id)
    delete(name)

