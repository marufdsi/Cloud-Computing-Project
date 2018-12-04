graph=/user/cloudera/louvain/graph
intermediatefile=/user/cloudera/louvain/preprocess
output=/user/cloudera/louvain/output


run-louvain: build-louvainmethod louvainmethod-jar
	hadoop fs -rm -f -r  $(output)
	hadoop jar louvainmethod.jar uncc.edu.maruf.louvain.LouvainMethod $(graph) $(intermediatefile) $(output)

louvainmethod-jar: louvainmethod.jar

louvainmethod.jar: build/uncc/edu/maruf/louvain/*.class
	jar -cvf louvainmethod.jar -C build/ .

build-louvainmethod: src/uncc/edu/maruf/louvain/*.java
	mkdir -p build/
	javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* src/uncc/edu/maruf/louvain/*.java -d build -Xlint


clean:
	rm -rf build louvainmethod.jar
	hadoop fs -rm -f -r  $(graph)
	hadoop fs -rm -f -r  $(intermediatefile)
	hadoop fs -rm -f -r  $(output)
