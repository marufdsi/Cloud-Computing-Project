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

import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;



public class LouvainMethod{
    private static final Logger LouvainLog = Logger.getLogger(LouvainMethod.class);
    public static Graph G;
    private static Graph originalGraph;
    public static boolean changed;
    private static Double gTotalEdgeWeight;
    public static void main(String[] args) throws Exception{
        changed = true;
        GraphReader reader = new GraphReader(args[0]);
        G = reader.buildGraph();
        G.initializeVolume();
        originalGraph = G;
        gTotalEdgeWeight = G.totalEdgeWeight;
        System.out.println("Graph Creation Done");
        String finalOutputPath = MovePhase.detectCommunity(args);
    }

    public double getModularity(String filePath){
        double modularity = 0.0;
        return modularity;
    }

}
