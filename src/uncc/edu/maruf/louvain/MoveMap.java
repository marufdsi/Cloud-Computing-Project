/**
 * Copyright:
 * Md Maruf Hossain
 * Department of Computer Science
 * University of North Carolina at Charlotte(UNCC) 2018
 */
package uncc.edu.maruf.louvain;

import java.io.*;

import com.google.gson.Gson;
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

import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MoveMap extends Mapper<LongWritable, Text, Text, Text> {
    private static final Logger MapperLog = Logger.getLogger(MoveMap.class);
    private String input;
    Graph graph;
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String graphObject = conf.get("graphObject");
        Gson gson = new Gson();
        graph = gson.fromJson(graphObject, Graph.class);
        if (context.getInputSplit() instanceof FileSplit) {
            this.input = ((FileSplit) context.getInputSplit()).getPath().toString();
        } else {
            this.input = context.getInputSplit().toString();
        }
    }

    private final Pattern WORD_BOUNDARY = Pattern.compile(" ");

    @Override
    public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        /// get the input line as string and trim it.
        String line = lineText.toString();
        line = line.trim();

        String[] lineSegments = line.split("\\s+");
        int u;
        if (lineSegments.length <2){
            return;
        }
        if (!lineSegments[1].trim().contains(":::"))
            return;

        String fromVertex = lineSegments[0];
        String elements = "";
        boolean hasElements = false;
        if(fromVertex.contains("::##::")){
            String[] communityInfo = fromVertex.split("::##::");
            u = Integer.parseInt(communityInfo[0]);
            if (communityInfo[1].contains("@::@")){
                elements = communityInfo[1].split("@::@")[1];
                hasElements = true;
            }
        } else {
            u = Integer.parseInt(fromVertex);
        }

        String[] adjacency = lineSegments[1].trim().split("###");
        java.util.Map<Integer, Double> affinity = new HashMap<>();

        affinity.put(graph.zeta.get(u), 0.0);
        for (int i = 0; i < adjacency.length; i++) {
            String[] neighbor = adjacency[i].trim().split(":::");
            if (neighbor.length < 2)
                return;
            int v = Integer.parseInt(neighbor[0]);
            double weight = Double.parseDouble(neighbor[1]);
            if (u != v) {
                int C = graph.zeta.get(v);
                if (affinity.containsKey(C)) {
                    affinity.put(C, affinity.get(C) + weight);
                } else {
                    affinity.put(C, weight);
                }
            }
        }
        int bestCommunity = Integer.MAX_VALUE;
        double deltaBest = -1;
        int C = graph.zeta.get(u);
        double affinityC = affinity.get(C);
        for (int D : affinity.keySet()) {
            if (D != C) {
                double delta = modGain(u, C, D, affinityC, affinity.get(D));
                // TRACE("mod gain: " , delta);
                if (delta > deltaBest) {
                    deltaBest = delta;
                    bestCommunity = D;
                }
            }
        }
        int community = C;
        if (deltaBest > 0) { // if modularity improvement possible
            graph.zeta.set(u, bestCommunity); // move to best cluster
            community = bestCommunity;
            // mod update
            double volN = 0.0;
            volN = graph.volumeOfNode.get(u);
            // update the volume of the two clusters
            graph.volumeOfCommunity.set(C,  graph.volumeOfCommunity.get(C)-volN);
            graph.volumeOfCommunity.set(bestCommunity,  graph.volumeOfCommunity.get(C)+volN);
            Configuration conf = context.getConfiguration();
            conf.set("moved", String.valueOf(true));
        }
        if (hasElements){
            context.write(new Text(String.valueOf(u) + "::##::" + community + "@::@" + elements), new Text(lineSegments[1].trim()));
        } else {
            context.write(new Text(String.valueOf(u) + "::##::" + community), new Text(lineSegments[1].trim()));
        }
    }

    public double modGain(int u, int C, int D, double affinityC, double affinityD) {
        double volN = graph.volumeOfNode.get(u);
        return (affinityD - affinityC) / graph.totalEdgeWeight + graph.gamma * ((volCommunityMinusNode(C, u) - volCommunityMinusNode(D, u)) * volN) / (2 * graph.totalEdgeWeight * graph.totalEdgeWeight);
    }

    public double volCommunityMinusNode(int C, int x) {
        double volN = 0.0;
        double volC = graph.volumeOfCommunity.get(C);
        if (graph.zeta.get(x) == C) {
            volN = graph.volumeOfNode.get(x);
            return volC - volN;
        } else {
            return volC;
        }
    }
}
