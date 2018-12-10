intermediatefile=/user/cloudera/louvain/preprocess
output=/user/cloudera/louvain/output

graph1=/user/cloudera/louvain/smallworld
preprocessedGraph1=/user/cloudera/louvain/preprocessedSmallWorld
graph2=/user/cloudera/louvain/as-22july06
preprocessedGraph2=/user/cloudera/louvain/preprocessedAs-22july06
graph3=/user/cloudera/louvain/cnr-2000
preprocessedGraph3=/user/cloudera/louvain/preprocessedCnr-2000
graph4=/user/cloudera/louvain/email
preprocessedGraph4=/user/cloudera/louvain/preprocessedEmail
graph5=/user/cloudera/louvain/G_n_pout
preprocessedGraph5=/user/cloudera/louvain/preprocessedG_n_pout

graph6=/user/cloudera/louvain/astro-ph
preprocessedGraph6=/user/cloudera/louvain/preprocessedAstro-ph
graph7=/user/cloudera/louvain/cond-mat-2005
preprocessedGraph7=/user/cloudera/louvain/preprocessedCond-mat-2005
graph8=/user/cloudera/louvain/power
preprocessedGraph8=/user/cloudera/louvain/preprocessedPower

preprocess: build jar
	hadoop fs -rm -f -r  $(preprocessedGraph3)
	hadoop jar louvainmethod.jar uncc.edu.maruf.louvain.LouvainMethod $(graph3) $(preprocessedGraph3)

run: build jar
	hadoop fs -rm -f -r  $(intermediatefile)
	hadoop fs -rm -f -r  $(output)
	hadoop jar louvainmethod.jar uncc.edu.maruf.louvain.LouvainMethod $(preprocessedGraph1) $(intermediatefile) $(output)

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
