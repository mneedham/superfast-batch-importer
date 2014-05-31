export HEAP=2G
export MAVEN_OPTS="-Xmn1G -Xmx$HEAP -Xms$HEAP -Dfile.encoding=UTF-8"
mvn test-compile exec:java -Dexec.mainClass="org.neo4j.batchimport.DataGenerator" \
   -Dexec.args="$@"
