#!/usr/bin/python
import psycopg2
from dbfpy import dbf

db = dbf.Dbf("osm.names", new=True)
db.addField(
  ("NAME", "C", 64),
  ("FULL_CODE", "C", 32),
  ("MAJOR_CODE", "C", 32),
  ("LATITUDE", "C", 12),
  ("LONGITUDE", "C", 12),
)

pg = psycopg2.connect("dbname='osm_shp' user='guest' password='guest' host='gis-lab.info'")
cc = pg.cursor()

q = "SELECT place, COALESCE(tags->'name:ru', name) AS name, ST_Y(way) AS lat, ST_X(way) AS lon FROM osm_point WHERE place IS NOT NULL"
cc.execute(q)

i=0

while 1:
  i = i+1
  row = cc.fetchone()
  if not row:
    break

  place, name, lat, lon = row
  try:
    name = name.decode("utf-8").encode("cp866")
  except:
    continue

  rec = db.newRecord()
  rec["NAME"] = name
  rec["LATITUDE"] = "%.9lf" % lat
  rec["LONGITUDE"] = "%.9lf" % lon
  rec.store()
  if i % 1000 == 0:
    print "%d records processed" % i

db.close()
