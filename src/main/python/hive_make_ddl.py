################################################################################################
# Connect to Hiveserver2 and generate various hql statements (ddl and dml)
# Todo: Cleanup parms, factor out into class or module
################################################################################################
import argparse
import jaydebeapi
import json
import sys
from pprint import pprint

# Need to export classpath before running
# export CLASSPATH=$CLASSPATH:$(hadoop classpath):/usr/hdp/current/hadoop-client/*:/usr/hdp/current/hive-client/lib/*:/usr/hdp/current/hadoop-client/client/*
#
password="M~F<Y$XvS26'sSy"
databaseFilter="hcclnw"
tableFilter="*"
region="CN"
excludeCols=['adf_row_key', 'kp_region_code', 'adf_create_date', 'adf_active_row', 'adf_end_date']

def parse_arguments():
	parser = argparse.ArgumentParser()
	parser.add_argument('action')
	parser.add_argument('database')
	parser.add_argument('table')
	
	parser.add_argument('--debug', action='store_true', default=False)
	return parser.parse_args()

def make_counts(tdef):
	tableName = tdef["tableInfo"]["tableName"]

	result =  "INSERT INTO default.counts SELECT '${db_name}' as database_name, '" + tableName + "' as table_name, " 
	result += "SELECT COUNT(*) FROM ${db_name}." + tableName + ";"
	return result
	
def make_drops(tdef):
	return "DROP TABLE IF EXISTS ${db_name}." + tdef["tableInfo"]["tableName"] + ";"

def make_insert(tdef):
	result = "\n"
	td = tdef["tableInfo"]
	columns = tdef["columns"]

	if (len(td["partitionKeys"]) > 0):
		result += "-" * 100 + "\n-- PARTITIONED TABLE - SPECIAL HANDLING REQUIRED --\n" + "-" * 100 + "\n"

	result += "DROP TABLE IF EXISTS ${target_db}." + td["tableName"] + ";\n"
	result += "CREATE TABLE ${target_db}." + td["tableName"] + "\nAS\nSELECT\n"
	
	# Todo remove filter and just convert to list (Little kludge here)
	goodCols = list(filter(lambda x: (x["name"] not in []), columns))
	l=len(goodCols)
	i=0
	for col in goodCols:
		i=i+1
		colName = col["name"]
		dataType = col["type"]
		etlColumn = (colName in excludeCols)
		terminator = "" if i==l else ","
		if (etlColumn):
			if (colName == "kp_region_code"):
				result=result + "CAST ('" + region + "' AS " + dataType + ") AS `" + colName + "`" + terminator + "\n"
			else:
				result=result + "CAST (NULL AS " + dataType + ") AS `" + colName + "`" + terminator + "\n"
		else:
			if (dataType=="timestamp"):
				result=result + "from_unixtime(unix_timestamp(" + "`" + colName + "`" + ",'dd-MMM-yy HH:mm:SS')) AS " + "`" + colName + "`" + terminator + "\n"
			else:
				result=result + "`" + colName + "`" + terminator + "\n"
		
	result += "FROM `${db_name}." + td["tableName"] + "`;\n\n"

	return result

def print_ddl(tdef):
	td = tdef["tableInfo"]
	columns = tdef["columns"]

	if (len(td["partitionKeys"]) > 0):
		print "-" * 100 + "\n-- PARTITIONED TABLE - SPECIAL HANDLING REQUIRED --\n" + "-" * 100

	print "DROP TABLE IF EXISTS `${db_name}." + td["tableName"] + "`;"
	print "CREATE EXTERNAL TABLE `${db_name}." + td["tableName"] + "` ("
	goodCols = list(filter(lambda x: (x["name"] not in excludeCols), columns))
	l = len(goodCols)
	i=0
	for col in goodCols:
		i=i+1
		dataType = "string" if col["type"] == "timestamp" else col["type"]
		suffix = " -- timestamp" if col["type"] == "timestamp" else ""
		if (i==l):
			print "`" + col["name"] + "` " + dataType + ")" + suffix
		else:
			print "`" + col["name"] + "` " + dataType + "," + suffix
	print "ROW FORMAT DELIMITED\n  FIELDS TERMINATED BY '\\t'\n  LINES TERMINATED BY '\\n'"
	print "LOCATION '${base_loc}/" + td["tableName"].upper() + "';\n\n"

def get_table_info(tdef):
	ti = {}
	td = tdef["tableInfo"]
	ti["dbName"] = td["dbName"]
	ti["tableName"] = td["tableName"]
	ti["owner"] = td["owner"]
	ti["columnStatsAccurate"] = td["parameters"]["COLUMN_STATS_ACCURATE"]
	ti["numFiles"] = td["parameters"]["numFiles"]
	ti["numRows"] = td["parameters"]["numRows"]
	ti["rawDataSize"] = td["parameters"]["rawDataSize"]
	ti["totalSize"] = td["parameters"]["totalSize"]
	ti["orc.bloom.filter.columns"] = td["parameters"].get("orc.bloom.filter.columns")
	partitionKeys=[]
	for pk in td["partitionKeys"]:
		partitionKeys.append(pk["name"])
	if (len(partitionKeys) == 0):
		ti["partitionKeys"] = ""
	else:
		ti["partitionKeys"] = ",".join(partitionKeys)
	ti["partitionCount"] = ""
	if (len(td["sd"]["bucketCols"]) == 0):
		ti["bucketCols"] = ""
	else:
		ti["bucketCols"] = ",".join(td["sd"]["bucketCols"])
	ti["compressed"] = td["sd"]["compressed"]
	ti["inputFormat"] = td["sd"]["inputFormat"]
	ti["outputFormat"] = td["sd"]["outputFormat"]
	ti["location"] = td["sd"]["location"]
	sortCols=[]
        for sc in td["sd"]["sortCols"]:
                sortCols.append(sc["col"])
        if (len(sortCols) == 0):
                ti["sortCols"] = ""
        else:
                ti["sortCols"] = ",".join(sortCols)
	ti["tableType"] = td["tableType"]

	return ti
	
	
################################################################################################
# Main
################################################################################################
args=parse_arguments()
action=args.action
databaseFilter=args.database
tableFilter=args.table

inputFileName = 'tables.txt'
inFile = open(inputFileName, "r")

tableList=[]
line = inFile.readline()
while (line != ""):
        tableList.append(line.replace("\n",""))
        line = inFile.readline()

inFile.close()
#pprint(tableList)

url="jdbc:hive2://kpph10llapprdsupusc01-int.azurehdinsight.net:443/default;transportMode=http;httpPath=hive2;ssl=true?hive.ddl.output.format=json"
creds=["svcespadfprdsup@kp.org",password]
conn=jaydebeapi.connect("org.apache.hive.jdbc.HiveDriver", url, creds)

sys.stderr.write("action: " + action + "\n")
sys.stderr.write("database: " + databaseFilter + "\n")
sys.stderr.write("table: " + tableFilter + "\n")

sql = "show databases like '" + databaseFilter + "'"
curs_hive = conn.cursor()
curs_hive.execute(sql)

row = curs_hive.fetchone()
databases = json.loads(row[0])
for database in databases[u'databases']:
	columnList=[]
	sql = "show tables in " + database + " like '" + tableFilter + "'"
	tbl_curs = conn.cursor()
	tbl_curs.execute(sql)
	row = tbl_curs.fetchone()
	tables = json.loads(row[0])
	for table in tables["tables"]:
		if (table in tableList):
			sql = "describe extended " + database + "." + table
			col_curs = conn.cursor()
			col_curs.execute(sql)
			row  = col_curs.fetchone()
			columns = json.loads(row[0])
	 		columnList.append(columns)

col_curs.close()
tbl_curs.close()
curs_hive.close() 
conn.close()

sys.stderr.write("Found " + str(len(columnList)) + " tables:\n")

if (action=="count" or action=="all"):	 		
	for columns in columnList:
		print make_counts(columns)
		
if (action=="ddl" or action=="all"):	 		
	for columns in columnList:
		print_ddl(columns)

if (action=="hql" or action=="all"):	 		
	for columns in columnList:
		print make_insert(columns)

if (action=="drop" or action=="all"):	 		
	for columns in columnList:
		print make_drops(columns)
