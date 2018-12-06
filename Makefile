graph=/user/cloudera/louvain/graph
intermediatefile=/user/cloudera/louvain/preprocess
output=/user/cloudera/louvain/output


run: build-louvainmethod louvainmethod-jar
	hadoop fs -rm -f -r  $(intermediatefile)
	hadoop fs -rm -f -r  $(output)
	hadoop jar louvainmethod.jar uncc.edu.maruf.louvain.LouvainMethod $(graph) $(intermediatefile) $(output)

louvainmethod-jar: jar

jar: build/uncc/edu/maruf/louvain/*.class
	jar -cvf louvainmethod.jar -C build/ .

build: src/uncc/edu/maruf/louvain/*.java
	mkdir -p build/
	javac -cp library/*:/usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* src/uncc/edu/maruf/louvain/*.java -d build -Xlint

run-preprocess: preprocess.jar
	java -cp preprocessdata.jar PreprocessData smallworld.graph updatedGraph.txt

preprocess.jar: preprocess/*.class
	jar -cvf preprocessdata.jar -C preprocess/ .

build-preprocess:   src/PreprocessData.java
	mkdir -p preprocess/
	javac src/PreprocessData.java -d preprocess -Xlint


clean:
	rm -rf build louvainmethod.jar
	hadoop fs -rm -f -r  $(graph)
	hadoop fs -rm -f -r  $(intermediatefile)
	hadoop fs -rm -f -r  $(output)
