clear
rm -rf target/*
mvn clean package -Dmaven.test.skip=true
rm -f ~/git/HiveTest/SimpleJdbc-1.0-SNAPSHOT.jar 
cp target/HiveMetaUtil.jar .
