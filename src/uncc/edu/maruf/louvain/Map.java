package uncc.edu.maruf.louvain;

import java.io.*;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Map extends Mapper<LongWritable, Text, Text, Text> {
    private static final Logger MapperLog = Logger.getLogger(Map.class);
    private String input;

    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
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
        String[] lineSegments = line.split(" ");
        if (lineSegments.length < 2)
            return;
        if (!lineSegments[1].trim().contains(":::"))
            return;
        int u = Integer.parseInt(lineSegments[0]);
        String[] adjacency = lineSegments[1].trim().split("###");
        java.util.Map<Integer, Double> affinity = new HashMap<>();
        for (int i = 0; i < adjacency.length; ++i) {
            String[] neighbor = adjacency[i].trim().split(":::");
            if (neighbor.length < 2)
                return;
            int v = Integer.parseInt(neighbor[0]);
            double weight = Integer.parseInt(neighbor[1]);
            if (u != v) {
                int C = LouvainMethod.G.zeta.get(v);
                affinity.put(C, affinity.get(C) + weight);
            }
        }
        int bestCommunity = Integer.MAX_VALUE;
        double deltaBest = -1;
        int C = LouvainMethod.G.zeta.get(u);
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
        if (deltaBest > 0) { // if modularity improvement possible
            LouvainMethod.G.zeta.set(u, bestCommunity); // move to best cluster
            // mod update
            double volN = 0.0;
            volN = LouvainMethod.G.volumeOfNode.get(u);
            // update the volume of the two clusters
            LouvainMethod.G.volumeOfCommunity.set(C,  LouvainMethod.G.volumeOfCommunity.get(C)-volN);
            LouvainMethod.G.volumeOfCommunity.set(bestCommunity,  LouvainMethod.G.volumeOfCommunity.get(C)+volN);
            Configuration conf = context.getConfiguration();
            conf.set("moved", String.valueOf(true));
        }
        context.write(new Text(String.valueOf(C)), new Text(lineSegments[1].trim()));
    }

    public double modGain(int u, int C, int D, double affinityC, double affinityD) {
        double volN = LouvainMethod.G.volumeOfNode.get(u);
        return (affinityD - affinityC) / LouvainMethod.G.totalEdgeWeight + LouvainMethod.G.gamma * ((volCommunityMinusNode(C, u) - volCommunityMinusNode(D, u)) * volN) / (2 * LouvainMethod.G.totalEdgeWeight * LouvainMethod.G.totalEdgeWeight);
    }

    public double volCommunityMinusNode(int C, int x) {
        double volN = 0.0;
        double volC = LouvainMethod.G.volumeOfCommunity.get(C);
        if (LouvainMethod.G.zeta.get(x) == C) {
            volN = LouvainMethod.G.volumeOfNode.get(x);
            return volC - volN;
        } else {
            return volC;
        }
    }
}
