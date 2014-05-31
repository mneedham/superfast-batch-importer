# Neo4j (CSV) Batch Importer

This software is licensed under the [GPLv3](http://www.gnu.org/licenses/gpl-3.0.en.html) for now. 
You can ask [Neo Technology](http://neotechnology.com) about a different licensing agreement.

__Works with Neo4j 2.1.1__

*Note: The initial reference node is gone in Neo4j 2.0, so node-id numbering changed, see below*

## Binary Download

To simply use it (no source/git/maven required):
* [download zip](http://dist.neo4j.org/batch-import/batch_importer_21.zip)
* unzip
* run `import.sh -db-directory test.db -nodes nodes.csv -rels rels.csv` (on Windows: `import.bat`)

You provide one **tab separated** csv file for nodes and one for relationships (optionally more for indexes)

Example data for the files is provided in the `sample` directory. Test the importer using:

`import.sh -db-directory test.db -nodes sample/nodes.csv -rels sample/rels.csv`

## File format

* **tab separated** csv files
* Property names in first row.
* If only one file is initially imported, the row number corresponds to the node-id (*starting with 0*)
* Property values not listed will not be set on the nodes or relationships.
* Optionally property fields can have a type (defaults to String) indicated with name:type where type is one of
  (int, long, float, double, boolean, byte, short, char, string). The string value is then converted to that type.
  Conversion failure will result in abort of the import operation.
* There is a separate "label" type, which should be used for relationship types and/or node labels, (`labels:label`)
* Property fields may also be arrays by adding "_array" to the types above and separating the data with commas.
* for non-ascii characters make sure to add `-Dfile.encoding=UTF-8` to the commandline arguments
* Optionally automatic indexing of properties can be configured with a header like `name:string:users` and a configured index in `batch.properties` like `batch_import.node_index=exact`
  then the property `name` will be indexed in the `users` index for each row with a value there
* multiple files for nodes and rels, comma separated, without spaces like "node1.csv,node2.csv"
* you can specify concrete, externally provided node-id's with: `i:id`, both in the node and relationship-files
* csv files can be zipped individually as *.gz or *.zip

## Examples


````sh
$ ./generate.sh

Usage: TestDataGenerator nodes=1000000 relsPerNode=50 relTypes=KNOWS,FOLLOWS propsPerNode=5 propsPerRel=2
0 nodes in 0secs with [0] long property strings
Creating 1000000 Nodes and 5501648 Relationships with [6501648:0] took 8 seconds.

$ ./import.sh -db-directory test.db -nodes nodes.csv -rels rels.csv

Neo4j Data Importer
Importer -db-directory <graph.db> -nodes <nodes.csv> -rels <rels.csv> -debug <debug config>

Using Existing Configuration File
[Current time:2014-05-31 03:43:54.63][Compile Time:Importer $ batch-import-2.1.0 $ 31/05/2014 03:42:10]
...
[2014-05-31 03:44:06.148]Node Import complete in 10 secs - [Property[1000000] Node[1000000] Relationship[0] Label[100]]
...
[2014-05-31 03:44:06.148]Relationship Import: [8] Property[6501648] Node[1000000] Relationship[5501648] Label[100] Disk[446 mb, 37 mb/sec] FreeMem[2344 mb]
...
[2014-05-31 03:44:24.103]Relationship Import complete in 17 secs - [Property[6501648] Node[1000000] Relationship[5501648] Label[100]]
[2014-05-31 03:44:24.103]Import complete with Indexing pending in 29 secs - [Property[6501648] Node[1000000] Relationship[5501648] Label[100]]
Total import time: 38 seconds
Total time taken: 39472 ms
````

There is also a `sample` directory, please run from the main directory `./import.sh -db-directory test.db -nodes sample/nodes.csv -rels sample/rels.csv`

````
head sample/nodes.csv

Node:ID	Rels	Property	Label:label	Counter:int
0	3	ONE	TwoHundredFortySix	0
1	3	ONE	FourHundredSeventyFive,TwoHundredFortyFour,NineHundredEightyFive	1
2	2	ONE	SevenHundredSeventyFive,SixHundredNinetySeven,FourHundredThirteen,SixHundredTwentyOne	2
3	0	ONE	OneHundredFourteen	3
4	1	ONE	Eighty,TwoHundredFortyFour,EightHundredEightyFive,SixHundredTwentyOne	4
5	4	ONE	NineHundredFortyTwo	5
6	0	ONE	FourHundredFortyNine	6
7	1	ONE	SixHundredSeventyThree,SixHundredNinetySeven	7
8	2	ONE	FourHundredSeventyFour	8

head sample/rels.csv

Start	End	Type	Property	Counter:long
61	42	FOUR	ONE	1
16	68	THREE	ONE	2
42	51	TWO	ONE	3
56	47	ONE	ONE	4
61	16	FOUR	ONE	5
72	69	THREE	ONE	6
80	67	TWO	ONE	7
92	54	ONE	ONE	8
54	99	THREE	ONE	9
````


## Execution

Just use the provided shell script `import.sh` or `import.bat` on Windows

    import.sh -db-directory test.db -nodes nodes.csv -rels rels.csv


### For Developers

If you want to work on the code and run the importer after making changes:

    mvn clean compile exec:java -Dexec.mainClass="org.neo4j.batchimport.Importer" -Dexec.args="-db-directory test.db -nodes nodes.csv -rels rels.csv"
    
    or
    
    java -server -Dfile.encoding=UTF-8 -Xmx4G -jar target/batch-import-jar-with-dependencies.jar -db-directory test.db -nodes nodes.csv -rels rels.csv


    $ rm -rf target/db
    $ mvn clean compile assembly:single
    [INFO] Scanning for projects...
    [INFO] ------------------------------------------------------------------------
    [INFO] Building Simple Batch Importer
    [INFO]    task-segment: [clean, compile, assembly:single]
    [INFO] ------------------------------------------------------------------------
    ...
    [INFO] Building jar: /Users/mh/java/neo/batchimport/target/batch-import-jar-with-dependencies.jar
    [INFO] ------------------------------------------------------------------------
    [INFO] BUILD SUCCESSFUL
    [INFO] ------------------------------------------------------------------------
    $ java -server -Xmx4G -jar target/batch-import-jar-with-dependencies.jar -db-directory test.db -nodes nodes.csv -rels rels.csv
    Physical mem: 16384MB, Heap size: 3640MB

## Parameters

* -db-directory test.db - the graph database directory, a new db will be created in the directory except when `batch_import.keep_db=true` is set in `batch.properties`.

* -nodes nodes.csv - a comma separated list of *node-csv-files*

* -rels rels.csv - a comma separated list of *relationship-csv-files*

#### Optional

* -config batch.properties - config file for setup
* -max-cpu 4 - number of CPU's to use
* -check - consistency check the generated store after import (takes a while)
* -debug - debug mode

It is also possible to specify those two file-lists in the config:

````
batch_import.nodes_files=nodes1.csv[,nodes2.csv]
batch_import.rels_files=rels1.csv[,rels2.csv]
````

## Schema indexes

Currently schema indexes are not created by the batch-inserter, you could create them upfront and use `batch_import.keep_db=true` to work with the existing database.

You have the option of specifying labels for your nodes using a column header like `type:label` and a comma separated list of label values.

On shutdown of the import Neo4j will populate the schema indexes with nodes with the appropriate labels and properties automatically.
(The index creation is As a rough estimate the index creation will

## Configuration

The Importer uses a supplied `batch.properties` file to be configured:

#### Memory Mapping I/O Config

Most important is the memory config, you should try to have enough RAM map as much of your store-files to memory as possible.

At least the node-store and large parts of the relationship-store should be mapped. The property- and string-stores are mostly
append only so don't need that much RAM. Below is an example for about 6GB RAM, to leave room for the heap and also OS and OS caches.

````
cache_type=none
use_memory_mapped_buffers=true
# 14 bytes per node
neostore.nodestore.db.mapped_memory=200M
# 33 bytes per relationships
neostore.relationshipstore.db.mapped_memory=3G
# 38 bytes per property
neostore.propertystore.db.mapped_memory=500M
# 60 bytes per long-string block
neostore.propertystore.db.strings.mapped_memory=500M
neostore.propertystore.db.index.keys.mapped_memory=5M
neostore.propertystore.db.index.mapped_memory=5M
````

#### Indexes (experimental)

````
batch_import.node_index.users=exact
batch_import.node_index.articles=fulltext
batch_import.relationship_index.friends=exact
````

#### CSV (experimental)

````
batch_import.csv.quotes=true // default, set to false for faster, experimental csv-reader
batch_import.csv.delim=,
````

##### Index-Cache (experimental)

````
batch_import.mapdb_cache.disable=true
````

##### Keep Database (experimental)

````
batch_import.keep_db=true
````

## Utilities

### DataGenerator

    ./generate.sh dir=<> nodes=<> relsPerNode=<> relTypes=<> sorted maxLabels=<> relTypes=<> columns=<> \
                  propsPerNode=<> propsPerRel=<> maxPropSize=<>:<> startNodeId=<>

     <> means user input

Note that all these parameters can in any order and is not case sensitive.

* dir 		- location where nodes.csv and rels.csv is generated
* nodes 		- number of nodes
* relsPerNode - number of relationships per node
* relTypes	- number of relationship types
* maxLabels	- maximum number of labels
* sorted		- sorted or unsorted data (default is unsorted)
* columns		- a tab separated column names
* propsPerNode- properties per node
* propsPerRel	- properties per relationship
* maxPropSize	- x:y, x is maximum size in bytes, y is percentage of properties > x in size
* startNodeId	- the lowest node id. Useful in creating multiple node and relationship files.

for instance:

    ./generate.sh dir=d:\data nodes=1000000000 relsPerNode=8 relTypes=20 maxlabels=10000 propsPerNode=3 propsPerRel=2 maxPropSize=32:5


**TODO rework legacy indexing section, it is not up to date!**

## (Legacy) Indexing


### Indexing of inserted properties

You can automatically index properties of nodes and relationships by adding ":indexName" to the property-column-header.
Just configure the indexes in `batch.properties` like so:

````
batch_import.node_index.users=exact
````

````
name:string:users    age works_on
Michael 37  neo4j
Selina  14
Rana    6
Selma   4
````

**If you use `node_auto_index` as the index name, you can also initially populate Neo4j's automatic node index which is then
later used and and updated while working with the database.**


In the relationships-file you can optionally specify that the start and end-node should be looked up from the index in the same way

````
name:string:users	name:string:users	type	    since   counter:int
Michael     Selina   FATHER_OF	1998-07-10  1
Michael     Rana   FATHER_OF 2007-09-15  2
Michael     Selma   FATHER_OF 2008-05-03  3
Rana     Selma   SISTER_OF 2008-05-03  5
Selina     Rana   SISTER_OF 2007-09-15  7
````

### Explicit Indexing

Optionally you can add nodes and relationships to indexes.

Add four arguments per each index to command line:

To create a full text node index called users using nodes_index.csv:

````
node_index users fulltext nodes_index.csv
````

To create an exact relationship index called worked using rels_index.csv:

````
rel_index worked exact rels_index.csv
````

Example command line:

````
./import.sh -db-directory test.db -nodes nodes.csv -rels rels.csv
````

### Using Neo4j's Automatic Indexing

The auto-indexing elsewhere in this file pertains to the *batch inserter's* ability to automatically index. If you want to
use this cool feature from the batch inserter, there's a little gotcha. You still need to enable the batch inserter's feature
with `batch_import.node_index` but then instead of specifying the name of a regular index, specify the auto index's name like so:

````
batch_import.node_index.node_auto_index=exact
````

And you have to make sure to also enable automatic indexing in your regular Neo4j database's (`conf/neo4j.properties`) and
specify the correct node properties to be indexed.

## Examples

### nodes_index.csv

````
id	name	language
0	Victor Richards	West Frisian
1	Virginia Shaw	Korean
2	Lois Simpson	Belarusian
3	Randy Bishop	Hiri Motu
4	Lori Mendoza	Tok Pisin
````

### rels_index.csv

````
id	property1	property2
0	cwqbnxrv	rpyqdwhk
1	qthnrret	tzjmmhta
2	dtztaqpy	pbmcdqyc
````
