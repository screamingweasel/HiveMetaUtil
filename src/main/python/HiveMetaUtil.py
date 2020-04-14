import json
from pprint import pprint

def getValue(dict, key, default):
    value = str(default)
    
    if (dict != None):
        if (key in dict):
            if (dict[key] != None):
                value = str(dict[key])
	return str(value)

def get_table_info(tdef):
	ti = {}
	td = tdef["tableInfo"]
	ti["dbName"] = td["dbName"]
	ti["tableName"] = td["tableName"]
	ti["owner"] = td["owner"]
	parameters=td.get("parameters",{})
	ti["columnStatsAccurate"] = getValue(parameters, "COLUMN_STATS_ACCURATE","")
	ti["numFiles"] = getValue(parameters, "numFiles","")
	ti["numRows"] = getValue(parameters, "numRows","")
	ti["rawDataSize"] = getValue(parameters, "rawDataSize","")
	ti["totalSize"] = getValue(parameters, "totalSize","")
	ti["orc.bloom.filter.columns"] = getValue(parameters, "orc.bloom.filter.columns","")
	partitionKeys=[]
	for pk in td["partitionKeys"]:
		partitionKeys.append(pk["name"])

	if (len(partitionKeys) == 0):
		ti["partitionCount"] = ""
		ti["partitionKeys"] = ""
	else:
		ti["partitionCount"] = ti["partitionKeysSize"]
		ti["partitionKeys"] = ",".join(partitionKeys)

	if (len(td["sd"]["bucketCols"]) == 0):
		ti["bucketCols"] = ""
	else:
		ti["bucketCols"] = ",".join(td["sd"]["bucketCols"])

	ti["numBuckets"] = td["sd"].get("numBuckets","0")
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
	
	# Shortcut Helpers (concatenated strings, etc. for code generation)
	helpers = {}
	cols = td["sd"].get("cols",[])
	helpers["qualifiedName"] = 	ti["dbName"] + "." + td["tableName"]
	helpers["bucketColumns"] = ",".join(map(lambda x: x["name"], td["sd"].get("bucketCols",[])))	
	helpers["allColumns"] = ",".join(map(lambda x: x["name"], td["sd"].get("cols",[])))
	helpers["sortColumns"] = ",".join(map(lambda x: x["name"], td.get("partitionKeys",[])))
	helpers["partColumns"] = ",".join(map(lambda x: x["name"], td.get("sortCols",[])))
	partKeys = map(lambda x: x["name"], td.get("partitionKeys",[]))
	helpers["nonKeyColumns"] = filter (lambda x: x not in set(partKeys), helpers["allColumns"])

	helpers["columnDefs"] = ",\n".join(map(lambda x: x["name"] + " " + x["type"], td["sd"].get("cols",[])))
	nkc = filter(lambda x: x["name"] not in set(partKeys), cols)
	helpers["nonKeyDefs"] = ",\n".join(map(lambda x: x["name"] + " " + x["type"], nkc))
	nkc = filter(lambda x: x["name"] in set(partKeys), cols)
	helpers["partDefs"] = ",\n".join(map(lambda x: x["name"] + " " + x["type"], nkc))

	ti["helpers"] = helpers

	return ti

def print_create_table(ti, drop=True):
	h = ti["helpers"]
	if (drop):
		print "DROP TABLE IF EXISTS " + h["qualifiedName"] + ";"
		
	tableType = "EXTERNAL " if ti["tableType"] == "EXTERNAL_TABLE" else ""
	print "CREATE " + tableType + "TABLE " + h["qualifiedName"] + " ("
	print h["nonKeyDefs"] +")"

	if (h["partColumns"] != ''):
		print "PARTITIONED BY (" + h["partColumns"] + ")"

	if (h["bucketColumns"] != ''):
		print "CLUSTERED BY (" + h["bucketColumns"] + ")"
		print "INTO " + ti["numBuckets"] + " BUCKETS "

	if (h["sortColumns"] != ''):
		print "SORTED BY (" + h["sortColumns"] + ")"
	
	print ";\n"
	
def print_csv(lines):
	headers=["database", "table", "tableType", "inputFormat", "outputFormat", "location", "numRows", "numFiles", "totalSize", "rawDataSize", "partions"," buckets"]
	DELIM="\t"
	template="%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s"

	print DELIM.join(headers)
	for line in lines:
		j = json.loads(line)
		ti = get_table_info(j)
		rec = template % (ti["dbName"], ti["tableName"], ti["tableType"], ti["inputFormat"], ti["outputFormat"], ti["location"], ti["numRows"], ti["numFiles"], ti["totalSize"], ti["rawDataSize"], ti["partitionCount"], ti["numBuckets"])
		print rec