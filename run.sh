clear
CONN="jdbc:hive2://c316-node3:10000/default;user=hive?hive.ddl.output.format=json"
#export CLASSPATH=$CLASSPATH:$(hadoop classpath):/usr/hdp/current/hadoop-client/*:/usr/hdp/current/hive-client/lib/*:/usr/hdp/current/hadoop-client/

export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_231.jdk/Contents/Home/
XTRAS="/Users/jbarnett/lib/:${JAVA_HOME}/jre/lib/ext"
java -Djava.ext.dirs=${XTRAS} -cp SimpleJdbc-1.0-SNAPSHOT.jar com.hortonworks.ps.SimpleJdbc \
"org.apache.hive.jdbc.HiveDriver" \
"$CONN" \
"xxxxxx" \
'xxxxxx' \
0 \
"default" \
"*"
