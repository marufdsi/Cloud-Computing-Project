/**
 * Copyright:
 * Md Maruf Hossain
 * Department of Computer Science
 * University of North Carolina at Charlotte(UNCC) 2018
 */
package uncc.edu.maruf.louvain;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.hadoop.fs.FileSystem;

import java.io.BufferedReader;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.io.*;

public class Preprocess {
    private static final Logger PreprocessLOG = Logger.getLogger(Preprocess.class);
    public int nodes;
    public int edges;
    public static double defaultEdgeWeight = 1.0;
    public List<Integer> degree;
    public List<List<Integer>> outEdge;
    public List<List<Double>> outEdgeWeight;
    public List<Integer> zeta;
    public Double totalEdgeWeight = 0.0;
    public Double gamma = 1.0;
    public List<Double> volumeOfNode;
    public List<Double> volumeOfCommunity;

    public Preprocess(Integer n, Integer e) {
        nodes = n;
        edges = e;
        degree = new ArrayList<Integer>(Collections.nCopies(n, 0));
        outEdge = new ArrayList<>(n);
        outEdgeWeight = new ArrayList<>(n);
        for (int i = 0; i < n; ++i) {
            outEdge.add(new ArrayList<Integer>());
            outEdgeWeight.add(new ArrayList<Double>());
        }
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
            fs.delete(outputFile, true);
        }

        FSDataOutputStream wr = fs.create(outputFile);
        wr.writeBytes(outputData);
        wr.close();
    }

}
