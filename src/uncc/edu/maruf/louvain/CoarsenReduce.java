/**
 * Copyright:
 * Md Maruf Hossain
 * Department of Computer Science
 * University of North Carolina at Charlotte(UNCC) 2018
 */
package uncc.edu.maruf.louvain;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

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
import org.apache.hadoop.fs.FileSystem;

public class CoarsenReduce extends Reducer<Text, Text, Text, Text> {
    private static final Logger CoarsenReducerLog = Logger.getLogger(CoarsenReduce.class);
    Graph graph;

    @Override
    public void reduce(Text community, Iterable<Text> adjacencies, Context context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String graphObject = conf.get("graphObject");
        Gson gson = new Gson();
        graph = gson.fromJson(graphObject, Graph.class);
        boolean moved = Boolean.parseBoolean(conf.get("moved"));
        Map<Integer, Double> neighbors = new HashMap<>();
        String elementsOfCommunity = "";
        int C = Integer.parseInt(community.toString());
        for (Text value : adjacencies) {
            if (!value.toString().contains("##::@@::##")) {
                throw new IOException("No elemen information");
            }
            String[] communityInfo = value.toString().split("##::@@::##");
            elementsOfCommunity += communityInfo[0] + "!#!";
            if (communityInfo[1].contains("###")) {
                String[] nodes = communityInfo[1].split("###");
                for (String node : nodes) {
                    if (node.contains(":::")) {
                        String[] nodeValuePair = node.split(":::");
                        if (nodeValuePair.length >= 2) {
                            int v = Integer.parseInt(nodeValuePair[0]);
                            int D = graph.zeta.get(v);
                            if (C != D) {
                                if (neighbors.containsKey(D)) {
                                    neighbors.put(D, neighbors.get(D) + Double.parseDouble(nodeValuePair[1]));
                                } else {
                                    neighbors.put(D, Double.parseDouble(nodeValuePair[1]));
                                }
                            }
                        }
                    }
                }
            } else {
                if (communityInfo[1].contains(":::")) {
                    String[] nodeValuePair = communityInfo[1].split(":::");
                    if (nodeValuePair.length >= 2) {
                        int v = Integer.parseInt(nodeValuePair[0]);
                        int D = graph.zeta.get(v);
                        if (C != D) {
                            if (neighbors.containsKey(D)) {
                                neighbors.put(D, neighbors.get(D) + Double.parseDouble(nodeValuePair[1]));
                            } else {
                                neighbors.put(D, Double.parseDouble(nodeValuePair[1]));
                            }
                        }
                    }
                }
            }
        }
        String newAdjacency = "";
        for (Integer D : neighbors.keySet()) {
            newAdjacency += String.valueOf(D) + ":::" + String.valueOf(neighbors.get(D)) + "###";
        }
        if (elementsOfCommunity.length() > 3) {
            elementsOfCommunity = elementsOfCommunity.substring(0, elementsOfCommunity.length() - 3);
        }
        if (newAdjacency.length() > 3) {
            newAdjacency = newAdjacency.substring(0, newAdjacency.length() - 3);
            context.write(new Text(String.valueOf(C) + "::##::" + String.valueOf(C) + "@::@" + elementsOfCommunity), new Text(newAdjacency));
        }
    }
    /// Cleanup method called at the last of Reduce. So, I perform cleanup and sorting in here.
    public void cleanup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String inputPath = conf.get("InputPath");
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(new Path(inputPath))) {
//            fs.delete(new Path(inputPath), true);
        }
    }
}
