from sqlite3 import Connection
from utilities import listFields,getShp, parseFieldType,getProjCode,getProjDetails,getOID,makeParseProp,makeInter,statusMessage
from arcpy import SpatialReference,SearchCursor,AddMessage,GetArgumentCount,GetParameterAsText,Describe
from wkb import getWKBFunc
from functools import partial

def makeDB(out):
	conn=Connection(out)
	c=conn.cursor()
	c.execute("""CREATE TABLE geometry_columns (     f_table_name VARCHAR,      f_geometry_column VARCHAR,      geometry_type INTEGER,      coord_dimension INTEGER,      srid INTEGER,     geometry_format VARCHAR )""")
	c.execute("""CREATE TABLE spatial_ref_sys        (     srid INTEGER UNIQUE,     auth_name TEXT,     auth_srid TEXT,     srtext TEXT)""")
	conn.commit()
	c.close()
	return True

def insertFunc(db,name):
	conn=Connection(db)
	c=conn.cursor()
	def returnFunc(fc):
		keys = fc.keys()
		values = fc.values()
		c.execute("""insert into {0}({1})
				values({2})
				""".format(name,", ".join(keys),makeInter(len(values))),values)
		conn.commit()
	def closefunc():
		c.close()
	return [returnFunc,closefunc]

def addRows(featureClass,db,name):
	AddMessage('add rows')
	[insert,close] = insertFunc(db,name)
	[shp,shpType]=getShp(featureClass)
	fields=listFields(featureClass)
	oid=getOID(fields)
	status = statusMessage(featureClass)
	sr=SpatialReference()
	wkt = getProjDetails(featureClass)[1]
	sr.loadFromString(wkt)
	#the search cursor
	rows=SearchCursor(featureClass,"",sr)
	parseGeo=getWKBFunc(shpType,shp)
	parseProp = partial(makeParseProp,fields,shp)
	try:
		for row in rows:
			status.update()
			fc = parseProp(row)
			try:
				fc["geometry"]=parseGeo(row)
			except:
				continue
			insert(fc)
	except Exception as e:
		AddMessage("OH SNAP! " + str(e))
	finally:
		del row
		del rows
		close()

def prepareFeature(featureClass,db,name):
	desc = Describe(featureClass)
	shp = desc.ShapeFieldName
	shpType= desc.shapeType.lower()
	if shpType == "point":
		gType = 1
	elif shpType == "multipoint":
		gType = 4
	elif shpType == "polyline":
		gType = 5
	elif shpType == "polygon":
		gType = 6
	fields=listFields(featureClass)
	fieldNames = []
	fieldNames.append("OGC_FID INTEGER PRIMARY KEY")
	fieldNames.append("GEOMETRY blob")
	for field in fields:
		if (fields[field] != u'OID') and field.lower() !=shp.lower():
			fieldNames.append(parseFieldType(field,fields[field]))
	projCode = getProjCode(featureClass)
	conn=Connection(db)
	c=conn.cursor()
	c.execute("""insert into geometry_columns( f_table_name, f_geometry_column, geometry_type, coord_dimension, srid, geometry_format) values(?,?,?,?,?,?)""",(name,"GEOMETRY",gType,2,projCode,"WKB"))
	c.execute("select srid from spatial_ref_sys where srid=:code",{'code':projCode})
	if not len(c.fetchall()):
		[auth,wkt]=getProjDetails(featureClass)
		c.execute("insert into spatial_ref_sys(srid ,auth_name ,auth_srid ,srtext) values(?,?,?,?)",(projCode, auth, projCode,wkt))
	c.execute("create table {0}({1})".format(name,", ".join(fieldNames)))
	conn.commit()
	c.close()
	return addRows(featureClass,db,name)
	
if GetArgumentCount()==1:
	makeDB(GetParameterAsText(0))
elif GetArgumentCount()==3:
	prepareFeature(GetParameterAsText(0),GetParameterAsText(1),GetParameterAsText(2))