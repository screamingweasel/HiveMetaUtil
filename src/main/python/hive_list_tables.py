import jaydebeapi
import json
from pprint import pprint

# Need to export classpath before running
# export CLASSPATH=$CLASSPATH:$(hadoop classpath):/usr/hdp/current/hadoop-client/*:/usr/hdp/current/hive-client/lib/*:/usr/hdp/current/hadoop-client/client/*
#
headers=[
"database",
"table",
"tableType",
"inputFormat",
"outpuFormat",
"location",
"numRows",
"numFiles",
"totalSize",
"rawDataSize",
"partions"
]
DELIM="\t"
template="%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s"

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

def print_formatted(ti):
	rec = template % (ti["dbName"], ti["tableName"], ti["tableType"], ti["inputFormat"], ti["outputFormat"], ti["location"], ti["numRows"], ti["numFiles"], ti["totalSize"], ti["rawDataSize"], ti["partitionCount"])
	print rec

url="jdbc:hive2://kpph10llapprdsupusc01-int.azurehdinsight.net:443/default;transportMode=http;httpPath=hive2;ssl=true?hive.ddl.output.format=json"
creds=["James.M.Barrett@kp.org","Chimpanzee1!"]
conn=jaydebeapi.connect("org.apache.hive.jdbc.HiveDriver", url, creds)

database="default"
sql = "show databases like 'hcclcn_20'"
curs_hive = conn.cursor()
curs_hive.execute(sql)

row = curs_hive.fetchone()
print DELIM.join(headers)
databases = json.loads(row[0])
for database in databases[u'databases']:
	sql = "show tables in " + database
	sql = "show tables in " + database + " like 'pat_enc'"
        #print "\n---- %s ----" % (database)
        tbl_curs = conn.cursor()
        tbl_curs.execute(sql)
	row = tbl_curs.fetchone()
	tables = json.loads(row[0])
	for table in tables["tables"]:
		print "  %s" % table

col_curs.close()
tbl_curs.close()
curs_hive.close() 

conn.close()

