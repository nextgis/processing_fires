#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import json
import os
import pyproj
import argparse
import geojson
import tempfile
from requests.auth import HTTPBasicAuth
from shapely.geometry import shape, Point
from functools import partial
from shapely.ops import transform
from requests_futures.sessions import FuturesSession

#python NextGIS_Create_source_data.py --url elina-usmanova --login administrator --password 123456 --parentid 0 --display_name Fires1 --latmin 40 --lonmin 40 --latmax 50 --lonmax 50


parser = argparse.ArgumentParser()
parser.add_argument('--url',type=str,required=True)
parser.add_argument('--login',type=str,required=True,default='administrator')
parser.add_argument('--password',type=str,required=True)
parser.add_argument('--parentid',type=int,required=True)
parser.add_argument('--display_name',type=str,required=True)
parser.add_argument('--latmin',type=float,required=True)
parser.add_argument('--lonmin',type=float,required=True)
parser.add_argument('--latmax',type=float,required=True)
parser.add_argument('--lonmax',type=float,required=True)

args = parser.parse_args()

path = tempfile.gettempdir()
path = path.replace('\\','/')
path = path + '/'
def source_data(url, login, password, parentid, display_name):
    URL = 'http://' + url + '.nextgis.com/api'
    AUTH = HTTPBasicAuth(login, password)
    v = requests.post('%s/resource/' % (URL,), auth=AUTH, json = {"resource":{"cls":"resource_group","parent":{"id":parentid},"display_name":display_name,"keyname":None,"description":None},"resmeta":{"items":{}}})
    folder_id = v.json()['id']
    d = requests.post('%s/resource/' % (URL,), auth=AUTH, json = {"resource":{"cls":"resource_group","parent":{"id":folder_id},"display_name":'Архив (обновление раз в день)',"keyname":None,"description":None},"resmeta":{"items":{}}})
    h = requests.post('%s/resource/' % (URL,), auth=AUTH, json = {"resource":{"cls":"resource_group","parent":{"id":folder_id},"display_name":'Мониторинг (обновление раз в час)',"keyname":None,"description":None},"resmeta":{"items":{}}})
    parentid_d = d.json()['id']
    parentid_h = h.json()['id']
    w = {'parentid_d':None}
    w['parentid_d'] = d.json()['id']
    w['parentid_h'] = h.json()['id']
    with open(path + 'source_data.geojson', "w") as f:
        p = geojson.Point((4985032, 8500000))
        f.write('%s' % p)

    session = FuturesSession(max_workers=5)

    payload_MODIS_d = {'vector_layer': {'fields': [], 'geometry_type': None, 'srs': {'id': 3857}}, 'resource': {'display_name': 'template_MODIS_d', 'keyname': None, 'parent': {'id': parentid_d}, 'description': None, 'cls': 'vector_layer'}, 'resmeta': {'items': {}}}
    payload_MODIS_h = {'vector_layer': {'fields': [], 'geometry_type': None, 'srs': {'id': 3857}}, 'resource': {'display_name': 'Current_MODIS_h', 'keyname': None, 'parent': {'id': parentid_h}, 'description': None, 'cls': 'vector_layer'}, 'resmeta': {'items': {}}}
    payload_VIIRS_d = {'vector_layer': {'fields': [], 'geometry_type': None, 'srs': {'id': 3857}}, 'resource': {'display_name': 'template_VIIRS_d', 'keyname': None, 'parent': {'id': parentid_d}, 'description': None, 'cls': 'vector_layer'}, 'resmeta': {'items': {}}}
    payload_VIIRS_h = {'vector_layer': {'fields': [], 'geometry_type': None, 'srs': {'id': 3857}}, 'resource': {'display_name': 'Current_VIIRS_h', 'keyname': None, 'parent': {'id': parentid_h}, 'description': None, 'cls': 'vector_layer'}, 'resmeta': {'items': {}}}
    
    dic = {0:0}
    payloads = [payload_MODIS_d, payload_MODIS_h, payload_VIIRS_d, payload_VIIRS_h]
    for payload in payloads:
        with open(path + 'source_data.geojson') as file:
            gj = geojson.load(file)
            payload['vector_layer']['geometry_type'] = "POINT"

            r = requests.post('%s/resource/' % (URL,), data=json.dumps(payload), auth=AUTH)
            try:
                payl = {'geom': str(gj['coordinates'])}
            except:
                raise
            session.post('%s/resource/%d/feature/' % (URL, r.json()['id']), data=json.dumps(payl), auth=AUTH)
            dic[payload['resource']['display_name']] = r.json()['id']
    del dic[0]
    
    with open("QGIS_Style.qml", 'rb') as file:
        t = requests.put("http://" + url + ".nextgis.com/api/component/file_upload/upload", data=file)
        f = t.text
        my_dict = json.loads(f)
        qgisid = my_dict["id"]

    for k in dic.keys():
        if 'MODIS' in k:
            #s = requests.post('%s/resource/' % (URL,), auth=AUTH, json = {"resource":{"cls":"mapserver_style","parent":{"id":dic[k]},"display_name":'Mapstyle_MODIS',"keyname":None,"description":None},"resmeta":{"items":{}},"mapserver_style":{"xml":"<map>\n  <symbol>\n    <type>ellipse</type>\n    <name>circle</name>\n    <points>1 1</points>\n    <filled>true</filled>\n  </symbol>\n  <layer>\n    <class>\n      <style>\n        <color blue=\"211\" green=\"177\" red=\"128\"/>\n        <outlinecolor blue=\"64\" green=\"64\" red=\"64\"/>\n        <symbol>circle</symbol>\n        <size>6</size>\n      </style>\n    </class>\n  </layer>\n  <legend>\n    <keysize y=\"15\" x=\"15\"/>\n    <label>\n      <size>12</size>\n      <type>truetype</type>\n      <font>regular</font>\n    </label>\n  </legend>\n</map>\n"}})
            q = requests.post('%s/resource/' % (URL,), auth=AUTH, json = {"resource":{"cls":"qgis_vector_style","parent":{"id":dic[k]},"display_name":"Mapstyle_MODIS_QGIS","keyname":None,"description":None},"resmeta":{"items":{}},"qgis_vector_style":{"file_upload":{"id":qgisid,"name":"QGIS_style.qml","mime_type":"application/octet-stream","size":18703}}})
            if '_d' in k:
                #w['styleid_d_MODIS'] = s.json()['id']
                w['styleid_d_MODIS_qgis'] = q.json()['id']
            else:
                #w['styleid_h_MODIS'] = s.json()['id']
                w['styleid_h_MODIS_qgis'] = q.json()['id']
        else:
            #s = requests.post('%s/resource/' % (URL,), auth=AUTH, json = {"resource":{"cls":"mapserver_style","parent":{"id":dic[k]},"display_name":'Mapstyle_VIIRS',"keyname":None,"description":None},"resmeta":{"items":{}},"mapserver_style":{"xml":"<map>\n  <symbol>\n    <type>ellipse</type>\n    <name>circle</name>\n    <points>1 1</points>\n    <filled>true</filled>\n  </symbol>\n  <layer>\n    <class>\n      <style>\n        <color blue=\"211\" green=\"177\" red=\"128\"/>\n        <outlinecolor blue=\"64\" green=\"64\" red=\"64\"/>\n        <symbol>circle</symbol>\n        <size>6</size>\n      </style>\n    </class>\n  </layer>\n  <legend>\n    <keysize y=\"15\" x=\"15\"/>\n    <label>\n      <size>12</size>\n      <type>truetype</type>\n      <font>regular</font>\n    </label>\n  </legend>\n</map>\n"}})
            q = requests.post('%s/resource/' % (URL,), auth=AUTH, json = {"resource":{"cls":"qgis_vector_style","parent":{"id":dic[k]},"display_name":"Mapstyle_MODIS_QGIS","keyname":None,"description":None},"resmeta":{"items":{}},"qgis_vector_style":{"file_upload":{"id":qgisid,"name":"QGIS_style.qml","mime_type":"application/octet-stream","size":18703}}})
            if '_d' in k:
                #w['styleid_d_VIIRS'] = s.json()['id']
                w['styleid_d_VIIRS_qgis'] = q.json()['id']
            else:
                #w['styleid_h_VIIRS'] = s.json()['id']
                w['styleid_h_VIIRS_qgis'] = q.json()['id']
    
    wm = requests.post('%s/resource/' % (URL,), auth=AUTH, json = {"resource":{"cls":"webmap","parent":{"id":folder_id},"display_name":"Очаги пожаров","keyname":None,"description":None},"resmeta":{"items":{}},"webmap":{"bookmark_resource":None,"extent_left":args.lonmin,"extent_right":args.lonmax,"extent_top":args.latmax,"extent_bottom":args.latmin,"root_item":{"item_type":"root","children":[
        #{"item_type":"layer","display_name":"MODIS Очаги пожаров","layer_style_id":w['styleid_d_MODIS'],"layer_enabled":True,"layer_transparency":None,"layer_min_scale_denom":None,"layer_max_scale_denom":None,"layer_adapter":"image","children":[]},
        {"item_type":"layer","display_name":"MODIS Очаги пожаров (QGIS)","layer_style_id":w['styleid_d_MODIS_qgis'],"layer_enabled":True,"layer_transparency":None,"layer_min_scale_denom":None,"layer_max_scale_denom":None,"layer_adapter":"image","children":[]},
        #{"item_type":"layer","display_name":"VIIRS Очаги пожаров","layer_style_id":w['styleid_d_VIIRS'],"layer_enabled":True,"layer_transparency":None,"layer_min_scale_denom":None,"layer_max_scale_denom":None,"layer_adapter":"image","children":[]},
        {"item_type":"layer","display_name":"VIIRS Очаги пожаров (QGIS)","layer_style_id":w['styleid_d_VIIRS_qgis'],"layer_enabled":True,"layer_transparency":None,"layer_min_scale_denom":None,"layer_max_scale_denom":None,"layer_adapter":"image","children":[]},
        #{"item_type":"layer","display_name":"MODIS Очаги пожаров (24ч)","layer_style_id":w['styleid_h_MODIS'],"layer_enabled":True,"layer_transparency":None,"layer_min_scale_denom":None,"layer_max_scale_denom":None,"layer_adapter":"image","children":[]},
        {"item_type":"layer","display_name":"MODIS Очаги пожаров (24ч) (QGIS)","layer_style_id":w['styleid_h_MODIS_qgis'],"layer_enabled":True,"layer_transparency":None,"layer_min_scale_denom":None,"layer_max_scale_denom":None,"layer_adapter":"image","children":[]},
        #{"item_type":"layer","display_name":"VIIRS Очаги пожаров (24ч)","layer_style_id":w['styleid_h_VIIRS'],"layer_enabled":True,"layer_transparency":None,"layer_min_scale_denom":None,"layer_max_scale_denom":None,"layer_adapter":"image","children":[]},
        {"item_type":"layer","display_name":"VIIRS Очаги пожаров (24ч) (QGIS)","layer_style_id":w['styleid_h_VIIRS_qgis'],"layer_enabled":True,"layer_transparency":None,"layer_min_scale_denom":None,"layer_max_scale_denom":None,"layer_adapter":"image","children":[]}]},"draw_order_enabled":False}})
    return w
                       
if __name__ == '__main__':
    w = source_data(args.url, args.login, args.password, args.parentid, args.display_name)
    os.remove(path + 'source_data.geojson')
    print(w)

