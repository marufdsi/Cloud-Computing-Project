intermediatefile=/user/cloudera/louvain/preprocess
output=/user/cloudera/louvain/output

graph=/user/cloudera/louvain/as-22july06
preprocessedGraph=/user/cloudera/louvain/preprocessedAs-22july06

preprocess: build jar
	hadoop fs -rm -f -r  $(preprocessedGraph)
	hadoop jar louvainmethod.jar uncc.edu.maruf.louvain.LouvainMethod $(graph) $(preprocessedGraph)

run: build jar
	hadoop fs -rm -f -r  $(intermediatefile)
	hadoop fs -rm -f -r  $(output)
	hadoop jar louvainmethod.jar uncc.edu.maruf.louvain.LouvainMethod $(preprocessedGraph) $(intermediatefile) $(output)

louvainmethod-jar: jar

jar: build/uncc/edu/maruf/louvain/*.class
	jar -cvf louvainmethod.jar -C build/ .

build: src/uncc/edu/maruf/louvain/*.java
	mkdir -p build/
	javac -cp library/*:/usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* src/uncc/edu/maruf/louvain/*.java -d build -Xlint


clean:
	rm -rf build louvainmethod.jar
	hadoop fs -rm -f -r  $(intermediatefile)
	hadoop fs -rm -f -r  $(output)
