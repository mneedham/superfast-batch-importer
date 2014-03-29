#!/bin/bash
if [ ! -d lib ]; then
  echo lib directory of binary download missing. Please download the zip or run import-mvn.sh
  exit 1
fi
# Detect Cygwin
case `uname -s` in
CYGWIN*)
    cygwin=1
esac
arguments=""
# Loop until all parameters are used up
while [ "$1" != "" ]; do
    arguments="$arguments $1"
    shift
done
base=`dirname "$0"`
if [ \! -z "$cygwin" ]; then
    wbase=`cygpath -w "$base"`
fi
curdir=`pwd`
cd "$base"
for i in lib/*.jar; do
    if [ -z "$cygwin" ]; then
        CP="$CP":"$base/$i"
    else
        i=`cygpath -w "$i"`
        CP="$CP;$wbase/$i"
    fi
done
cd "$curdir"
echo java -classpath $CP -Xmx1g -Xms1g -Dfile.encoding=UTF-8 org.neo4j.batchimport.NewTestDataGenerator $arguments
java -classpath "$CP" -Xmx1g -Xms1g -Dfile.encoding=UTF-8 org.neo4j.batchimport.NewTestDataGenerator $arguments 