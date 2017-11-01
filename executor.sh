#!/bin/bash
javac -cp $(hadoop classpath) src/org/dennis/utils/Utils.java src/org/dennis/pagerank/LinkGraph.java src/org/dennis/pagerank/PageRank.java -d build/pagerank/ -Xlint
jar -cvf jars/pagerank.jar -C build/pagerank/ .
hadoop jar jars/ org.dennis.pagerank.PageRank data/micro_wiki/ output/pagerank
rm -r output/ jars/pagerank.jar build/
mkdir -p build/pagerank jars/
javac -cp $(hadoop classpath) src/org/dennis/utils/Utils.java src/org/dennis/pagerank/LinkGraph.java src/org/dennis/pagerank/PageRank.java -d build/pagerank/ -Xlint
jar -cvf jars/pagerank.jar -C build/pagerank/ .
hadoop jar jars/pagerank.jar org.dennis.pagerank.PageRank data/micro_wiki/ output/pagerank
