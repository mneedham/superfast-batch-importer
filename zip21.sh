mvn clean install -DskipTests
mvn dependency:copy-dependencies
rm -rf lib
mkdir lib
SRC=target/dependency
cp $SRC/log4j-*.jar $SRC/opencsv-*.jar $SRC/neo4j-kernel-*.jar $SRC/neo4j-lucene-*.jar $SRC/neo4j-primitive-*.jar $SRC/neo4j-consistency-*.jar  $SRC/mapdb-*.jar $SRC/lucene-*.jar \
  $SRC/geronimo-jta_*.jar \
  target/batch-import*.jar lib
rm batch_importer_21.zip
zip -9 -r batch_importer_21.zip lib generate.sh import.sh import.bat readme.md sample
s3cmd put -P batch_importer_21.zip s3://dist.neo4j.org/batch-import/batch_importer_21.zip
