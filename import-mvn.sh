DB=${1-target/graph.db}
shift
NODES=${1-nodes.csv}
shift
RELS=${1-rels.csv}
shift
export HEAP=4G
export MAVEN_OPTS="-Xmn1G -Xmx$HEAP -Xms$HEAP -Dfile.encoding=UTF-8"
mvn compile exec:java -Dexec.mainClass="org.neo4j.batchimport.NewImporter" \
   -Dexec.args="batch.properties $DB $NODES $RELS $*" | grep -iv '\[\(INFO\|debug\)\]'
