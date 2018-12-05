package uncc.edu.maruf.louvain;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.hadoop.fs.Path;

public class Graph {
    private static final Logger LOG = Logger.getLogger(Graph.class);
    int nodes;
    int edges;
    double defaultEdgeWeight = 1.0;
    List<Integer> degree;
    List<List<Integer>> outEdge;
    List<List<Double> > outEdgeWeight;
    public static List<Integer> zeta;
    public static Double totalEdgeWeight=0.0;
    public static Double gamma = 1.0;
    public static List<Double> volumeOfNode;
    public static List<Double> volumeOfCommunity;
    public Graph(Integer n, Integer e){
        nodes = n;
        edges = e;
        degree = new ArrayList<Integer>(Collections.nCopies(n, 0));
        outEdge = new ArrayList<>(n);
        outEdgeWeight = new ArrayList<>(n);
        for (int i=0; i<n; ++i){
            outEdge.add(new ArrayList<Integer>());
            outEdgeWeight.add(new ArrayList<Double>());
        }
    }
    public void addAnEdge(int u, int v){
        addAnEdge(u, v, 1.0);
    }
    public void addAnEdge(int u, int v, double w){
        degree.set(u, degree.get(u)+1);
//        degree.set(v, degree.get(v)+1);
        List<Integer> adjacent = outEdge.get(u);
        adjacent.add(v);
        outEdge.set(u, adjacent);
        List<Double> adjacentWeight = outEdgeWeight.get(u);
        adjacentWeight.add(w);
        outEdgeWeight.set(u, adjacentWeight);
        totalEdgeWeight += w;
    }
    public void singletonCommunity(){
        zeta = new ArrayList<>(nodes);
        volumeOfNode = new ArrayList<>(nodes);
        volumeOfCommunity = new ArrayList<>(nodes);
        for (int u=0; u<nodes; ++u){
            zeta.add(u);
            volumeOfNode.add(defaultEdgeWeight * degree.get(u));
            volumeOfCommunity.add(defaultEdgeWeight * degree.get(u));
        }
    }

    public void saveGraphIntoHadoopFormat(String filePath) throws Exception{
        String outputData = "";
        for (int u=0; u<nodes; u++){
            String adjacency = "";
            for (int j=0; j<degree.get(u); j++){
                if (j==degree.get(u)-1) {
                    adjacency += outEdge.get(u).get(j) + ":::" + outEdgeWeight.get(u).get(j);
                } else {
                    adjacency += outEdge.get(u).get(j) + ":::" + outEdgeWeight.get(u).get(j) + "###";
                }
            }
            outputData += u + " " + adjacency + "\n";
        }
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path outputFile = new Path(filePath);
        if (fs.exists(outputFile)) {
            LOG.info("Graph output file exist");
            fs.delete(outputFile, true);
        }

        FSDataOutputStream wr = fs.create(outputFile);
        wr.writeBytes(outputData);
        wr.close();
    }

}
