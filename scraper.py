#!/usr/bin/env python3

import sys
from bs4 import BeautifulSoup as bs
import requests

MIN_ID       = 1
MAX_ID       = 6225
GENERAL_INFO = 'https://presas.conagua.gob.mx/inventario/tgeneralidades.aspx?DSP,{id}'

ps = [
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
  ("rep_y_mod",           "span_GENERALIDADES_REPARACIONES")
]

page = requests.get(GENERAL_INFO.format(id=1))
assert page.status_code==200

soup = bs(page.text, 'html.parser')

for store_name, id_name in ps:
  val = soup.find("", {"id": id_name}).text.strip()
  print("{0:20} = {1}".format(store_name,val))
