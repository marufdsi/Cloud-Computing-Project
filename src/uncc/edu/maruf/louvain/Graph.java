/**
 * Copyright:
 * Md Maruf Hossain
 * Department of Computer Science
 * University of North Carolina at Charlotte(UNCC) 2018
 */
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
    public int nodes;
    public int edges;
    public static double defaultEdgeWeight = 1.0;
    public List<Integer> vertexSet;
    public List<Integer> degree;
    public List<List<Integer>> outEdge;
    public List<List<Double>> outEdgeWeight;
    public List<Integer> zeta;
    public Double totalEdgeWeight = 0.0;
    public Double gamma = 1.0;
    public List<Double> volumeOfNode;
    public List<Double> volumeOfCommunity;

    public Graph(Integer n, Integer e) {
        nodes = n;
        edges = e;
        vertexSet = new ArrayList<>();
        degree = new ArrayList<Integer>(Collections.nCopies(n, 0));
        outEdge = new ArrayList<>(n);
        zeta = new ArrayList<Integer>(Collections.nCopies(n, 0));
        volumeOfNode = new ArrayList<Double>(Collections.nCopies(n, 0.0));
        volumeOfCommunity = new ArrayList<Double>(Collections.nCopies(n, 0.0));
        outEdgeWeight = new ArrayList<>(n);
        for (int i = 0; i < n; ++i) {
            outEdge.add(new ArrayList<Integer>());
            outEdgeWeight.add(new ArrayList<Double>());
        }
    }
    public void addVertex(Integer vertex){
        vertexSet.add(vertex);
    }

    public void addAnEdge(int u, int v) {
        addAnEdge(u, v, 1.0);
    }

    public void addAnEdge(int u, int v, double w) {
        degree.set(u, degree.get(u) + 1);
//        degree.set(v, degree.get(v)+1);
        List<Integer> adjacent = outEdge.get(u);
        adjacent.add(v);
        outEdge.set(u, adjacent);
        List<Double> adjacentWeight = outEdgeWeight.get(u);
        adjacentWeight.add(w);
        outEdgeWeight.set(u, adjacentWeight);
        totalEdgeWeight += w;
    }
    public void addToCommunity(int node, int community) {
        zeta.set(node, community);
    }

    public void singletonCommunity() {
        zeta = new ArrayList<>(nodes);
        volumeOfNode = new ArrayList<>(nodes);
        volumeOfCommunity = new ArrayList<>(nodes);
        for (int u = 0; u < nodes; ++u) {
            zeta.add(u);
            volumeOfNode.add(defaultEdgeWeight * degree.get(u));
            volumeOfCommunity.add(defaultEdgeWeight * degree.get(u));
        }
    }

    public void initializeVolume() {
        for (int u : vertexSet) {
            double sum = 0.0;
            List<Integer> neighbors = outEdge.get(u);
            for (int i=0; i<degree.get(u); ++i){
                sum += outEdgeWeight.get(u).get(i);
            }
            volumeOfNode.set(u, sum);
            volumeOfCommunity.set(zeta.get(u), sum);
        }
    }

    public void saveGraphIntoHadoopFormat(String filePath) throws Exception {
        String outputData = "header " + nodes + "::##::" + edges + "\n";
        for (int u = 0; u < nodes; u++) {
            String adjacency = "";
            for (int j = 0; j < degree.get(u); j++) {
                if (j == degree.get(u) - 1) {
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
