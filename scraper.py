#!/usr/bin/env python3

import sys
from bs4 import BeautifulSoup as bs
import csv
import requests
import re
import os
import pickle
from threading import Thread
import time

MIN_ID                = 1
MAX_ID                = 6225
CRAWL_DELAY           = 0.1
SAVED_DATA_FILE       = '/Users/Kelsey/Desktop/Berkeley/projects/mexico_gustavo/dam_scraper/saved_dams.pickle'
HUMAN_OUTPUT          = '/Users/Kelsey/Desktop/dam_readable.csv'
SAVE_FILES_TO         = '/Users/Kelsey/Desktop/Berkeley/projects/mexico_gustavo/dam_scraper/attachments'
GENERAL_URL           = 'https://presas.conagua.gob.mx/inventario/tgeneralidades.aspx?DSP,{id}'
UBICACION_URL         = 'https://presas.conagua.gob.mx/inventario/tubicacion.aspx?DSP,{id}'
PROPOSITOS_URL        = 'https://presas.conagua.gob.mx/inventario/tpropositoobra.aspx?DSP,{id}'
USO_AGUA_URL          = 'https://presas.conagua.gob.mx/inventario/tuso_agua_display.aspx?DSP,{id}'
CUENCA_ESCUR_URL      = 'https://presas.conagua.gob.mx/inventario/tcuenca.aspx?DSP,{id}'
ARCHIVOS_ESCUR_URL    = 'https://presas.conagua.gob.mx/inventario/archivos_presa.aspx?{id}'
CAUCE_AGUAS_ABAJO_URL = 'https://presas.conagua.gob.mx/inventario/tcauce_aguas_abajo.aspx?DSP,{id}'

general_table = [
  ("numero_id",           "span_NOMBRE_PRESA_ID"),
  ("nombre_oficial",      "span_NOMBRE_PRESA_NOMBRE_ODICIAL"),
  ("nombre_comun",        "span_NOMBRE_PRESA_NOMBRECOMUN"),
  ("ano_de_construccion", "span_GENERALIDADES_TERMINACION"),
  ("disenador",           "span_GENERALIDADES_DISENADOR"),
  ("constructor",         "span_GENERALIDADES_CONSTRUCTOR"),
  ("prog_con_inst",       "span_PROGINSTCONST_ID_PROGINSTCONST"),
  ("organismo_resp",      "span_GENERALIDADES_RESPONSABLE"),
  ("organismo_resp2",     "span_GENERALIDADES_RESPONSABLE_OTRO"),
  ("via_de_acceso",       "span_VIAACCESO_ID_VIAACCESO"),
  ("rep_y_mod",           "span_GENERALIDADES_REPARACIONES"),
]


ubicacion_table = [
  ("estado_id",           "span_ESTADOS_ID"), 
  ("municipio",           "span_MUNICIPIOS_CLAVE"),
  ("region_cna",          "span_ORGANISMOCUENCA_ID_ORGCUENCA"),
  ("region_hidr",         "span_REGIONES_HIDR_CLAVE"),
  ("latitud_grados",      "span_UBICACION_LATITUD_DEC"),
  ("longitud_grados",     "span_UBICACION_LONG_DEC"),
  ("carta_INEGI",         "span_UBICACION_CARTA_INEGI"),
  ("zona_sismica",        "span_ZONASISMICA_ID_ZONASISMICA"),
]


propositos_table = [
  ("proposito",           "span_OBJETIVOPRESA_ID_OBJETIVOPRESA_0001"), 
]


cuenca_table = [
  ("region_hidr_2",               "span_CUENCA_ESQ_RH"), 
  ("cuenca",                      "span_CUENCAS_ID"), #COMMENT
  ("area_1",                      "span_CUENCA_AREA"),
  ("area_2",                      "TEXTBLOCK8"),
  ("vol_max_escurr_anual_1",      "span_CUENCA_VESMAX"),
  ("vol_max_escurr_anual_2",      "TEXTBLOCK9"),
  ("vol_med_escurr_anual_1",      "span_CUENCA_VESMED"),
  ("vol_med_escurr_anual_2",      "TEXTBLOCK10"),
  ("corriente",                   "span_CUENCA_RIO"),
  ("afluente_de",                 "span_CUENCA_AFLUEN"),
]


cauce_aguas_abajo_table = [
  ("capacidad_cauce_1",    "span_CAUCE_AGUAS_ABAJO_QCAU"),
  ("capacidad_cauce_2",    "TEXTBLOCK1"),
  ("presas_abajo",         "span_CAUCE_AGUAS_ABAJO_PREAAB"),
  ("instruc_operacion",    "span_CAUCE_AGUAS_ABAJO_INSOPE"),
  ("comentarios",          "span_CAUCE_AGUAS_ABAJO_COMCAU"),
]

#COMMENT: skipped Cortinas
#COMMENT: skipped Galerias
#COMMENT: skipped Diques
#COMMENT: skipped Vertedores
#COMMENT: skipped Niveles de Vaso
#COMMENT: skipped Gastos de Diseño
#COMMENT: skipped Obras de Toma
#COMMENT: skipped Otros Desfogues
#COMMENT: want to grab docs in Archivos
#COMMENT: skipped Uso Agua

dam_data_template = [
    {"name": "GENERAL", "url": GENERAL_URL, "table": general_table},
    {"name": "UBICACION", "url": UBICACION_URL, "table": ubicacion_table},
#    {"name": "PROPOSITOS", "url": PROPOSITOS_URL, "table": propositos_table},
    {"name": "CUENCA_ESCUR", "url": CUENCA_ESCUR_URL, "table": cuenca_table},
    {"name": "CAUCE_AGUAS_ABAJO", "url": CAUCE_AGUAS_ABAJO_URL, "table": cauce_aguas_abajo_table},
    {"name": "archive", "url": ARCHIVOS_ESCUR_URL, "table": None}
]

table_entries = []
for x in dam_data_template:
    if x['table'] is None:
        continue
    for row in x['table']:
        table_entries.append(row[0])
assert len(set(table_entries))==len(table_entries)



def GetDataFromPage(url,table,dam_id):
    time.sleep(CRAWL_DELAY)
    url = url.format(id=dam_id)
    page = requests.get(url)
    if page.status_code!=200:
        raise ImportError("Couldn't download page!")

    soup = bs(page.text, 'html.parser')

    ret = {}
    for store_name, id_name in table:
        #If id_name is not present in the page, the following line
        #will raise an exception
        try:
            ret[store_name] = soup.find("", {"id": id_name}).text.strip()
        except:
            raise AttributeError("Failed to find {id} in page {url}".format(id=id_name,url=url))
        #print("{0:20} = {1}".format(store_name,val))

    return ret


if os.path.isfile(SAVED_DATA_FILE):
    dam_data = pickle.load( open(SAVED_DATA_FILE, "rb" ) )
else:
    dam_data = {}

def SaveToDisk(data):
  pickle.dump(data, open(SAVED_DATA_FILE, "wb" ) )

  def MergeDictionaries(dicts):
      super_dict = {}
      for d in dicts:
          for k, v in d.items():
              super_dict[k] = v
      return super_dict

  def Flatten(list_of_lists):
    return [item for sublist in list_of_lists for item in sublist]  

  with open(HUMAN_OUTPUT, 'w', newline='') as csvfile:
      fieldnames = Flatten([list(x.keys()) for x in data[list(data.keys())[0]].values()])
      writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
      writer.writeheader()
      for dam_id, this_dams_data in data.items():
          merged_dicts = MergeDictionaries([v for k,v in this_dams_data.items()])
          writer.writerow(merged_dicts)



for dam_id in range(1,MAX_ID+1):
    print("Fetching dam {0}...".format(dam_id))
    if dam_id not in dam_data:
        dam_data[dam_id] = {}
    for x in dam_data_template:
        if x['name'] in dam_data[dam_id]:
            continue
        if x['table'] is None:
            continue
        try:
            vals = GetDataFromPage(x['url'], x['table'], dam_id)
            dam_data[dam_id][x['name']] = vals
        except ImportError as e:
            pass
    #Save to disk after each successful capture
    #Threads cannot be interrupted, so the following makes the save robust
    #against user-initiated interrupts
    a = Thread(target=SaveToDisk, args=(dam_data,))
    a.start()
    a.join()

# page = requests.get(ARCHIVOS_ESCUR.format(id=1))
# assert page.status_code==200
# files=list(set(re.findall(r'/sisp_v2/img/ADJUNTOS/Ags/[^"]+\.(?:pdf|jpg)',page.text)))
# for filename in files:
#   download_link   = "https://presas.conagua.gob.mx"+filename
#   base_name       = os.path.basename(filename)
#   downloaded_file = requests.get(download_link).content
#   open(os.path.join(SAVE_FILES_TO,base_name), 'wb').write(downloaded_file)


