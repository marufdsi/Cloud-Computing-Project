/**
 * Copyright:
 * Md Maruf Hossain
 * Department of Computer Science
 * University of North Carolina at Charlotte(UNCC) 2018
 */
package uncc.edu.maruf.louvain;

import java.io.*;

import com.google.gson.Gson;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import org.apache.hadoop.fs.FileSystem;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;



public class LouvainMethod{
    private static final Logger LouvainLog = Logger.getLogger(LouvainMethod.class);
    public static Graph G;
    private static Graph originalGraph;
    private static Double gTotalEdgeWeight;
    public static void main(String[] args) throws Exception{
        if (args.length>=3){
            performLouvainMethod(args);
        } else {
            performPreproces(args);
        }
    }
    public static void performLouvainMethod(String[] args) throws Exception{
        GraphReader reader = new GraphReader(args[0]);
        G = reader.buildGraph();
        G.initializeVolume();
        originalGraph = G;
        gTotalEdgeWeight = G.totalEdgeWeight;
        System.out.println("Graph Creation Done");
        String finalOutputPath = MovePhase.detectCommunity(args);
        double modularity = getModularity(finalOutputPath);
        System.out.println("Modularity: " + modularity);
    }
    public static void performPreproces(String[] args) throws Exception{
        GraphReader reader = new GraphReader(args[0]);
        Preprocess preprocess = reader.preprocess(args[0]);
        preprocess.singletonCommunity();
        preprocess.saveGraphIntoHadoopFormat(args[1]);
        System.out.println("Graph Pre-process Done");
    }

    public static double getModularity(String filePath) throws Exception{
        double modularity = 0.0;
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(filePath))));
        String line = br.readLine();
        List<Integer> vertexWiseComm = new ArrayList<Integer>(Collections.nCopies(originalGraph.nodes, 0));
        List<Double> intraEdgeWeight = new ArrayList<Double>(Collections.nCopies(originalGraph.nodes, 0.0));
        List<Double> incidentWeightSum = new ArrayList<Double>(Collections.nCopies(originalGraph.nodes, 0.0));
        List<Integer> communities = new ArrayList<>();
        while (line != null) {
            String[] vertexInfo = line.split("\\s+");
            int u;
            if (vertexInfo.length <2){
                continue;
            }
            String fromVertex = vertexInfo[0];
            if(fromVertex.contains("::##::")){
                String[] communityInfo = fromVertex.split("::##::");
                if(communityInfo[1].contains("@::@")) {
                    String[] elements = communityInfo[1].split("@::@");
                    int C = Integer.parseInt(elements[0]);
                    communities.add(C);
                    String[] vertices = elements[1].split("!#!");
                    for (String vertex : vertices){
                        vertexWiseComm.set(Integer.parseInt(vertex), C);
                    }
                }
            }
            line = br.readLine();
        }
        for (int i=0; i<originalGraph.vertexSet.size(); i++){
            int u = originalGraph.vertexSet.get(i);
            List<Integer> neighbors = originalGraph.outEdge.get(u);
            List<Double> neighborsWeight = originalGraph.outEdgeWeight.get(u);
            int C = vertexWiseComm.get(u);
            double incidentWeight = 0.0;
            for (int j=0; j<neighbors.size(); j++){
                int v = neighbors.get(j);
                double w = neighborsWeight.get(j);
                int D = vertexWiseComm.get(v);
                if (C==D){
                    intraEdgeWeight.set(C, intraEdgeWeight.get(C) + w);
                }
                incidentWeight += w;
            }
            incidentWeightSum.set(C, incidentWeightSum.get(C)+incidentWeight);
        }
        double intraEdgeWeightSum = 0.0;
        double expCov = 0.0;
        int numberOfCommunity = communities.size();
        for (int C : communities){
            intraEdgeWeightSum += intraEdgeWeight.get(C);
            expCov += ((incidentWeightSum.get(C) / gTotalEdgeWeight) * (incidentWeightSum.get(C) / gTotalEdgeWeight )) / 4;
        }
        double cov = intraEdgeWeightSum / gTotalEdgeWeight;
        modularity = cov - expCov;
        br.close();
        System.out.println("Number of Community: " + numberOfCommunity);
        return modularity;
    }

}
