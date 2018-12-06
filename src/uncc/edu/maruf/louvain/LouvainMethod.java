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
import java.util.regex.Matcher;
import java.util.regex.Pattern;



public class LouvainMethod{
    private static final Logger LouvainLog = Logger.getLogger(LouvainMethod.class);
    public static Graph G;
    public static boolean changed;
    public static void main(String[] args) throws Exception{
        changed = true;
        GraphReader reader = new GraphReader(args[0]);
        G = reader.buildGraph();
        System.out.println("Graph nodes: " + G.nodes);
        System.out.println("Create Singleton Community");
        G.singletonCommunity();
        System.out.println("Graph Creation Done");
        G.saveGraphIntoHadoopFormat(args[1]);
        System.out.println("Save Graph");
//        MovePhase.detectCommunity(args);
//        Move.tryMove(args);
        mapReduceTask(args);
        /*int res = ToolRunner.run(new LouvainMethod(), args);
        System.exit(res);*/
    }

    public static void mapReduceTask(String[] args) throws Exception {
        int code = 0;
        Configuration conf = new Configuration();
        Gson gson = new Gson();
        String graphObject = gson.toJson(G);
        conf.set("graphObject", graphObject);
        Job job = Job.getInstance(conf, "LouvainMethod");
        job.setJarByClass(LouvainMethod.class);
        /// Set the input file
        FileInputFormat.addInputPath(job, new Path(args[0]));
        /// Set the output file location
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        /// Add Mapper Class
        job.setMapperClass(Map.class);
        /// Add CoarsenReduce Class
        job.setReducerClass(Reduce.class);
        /// Set intermediate output key as Text
        job.setOutputKeyClass(Text.class);
        /// Set intermediate output value as Integer format
        job.setOutputValueClass(Text.class);

        code = job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private static final Logger MapperLog = Logger.getLogger(Map.class);
        private String input;
        Graph graph;
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String graphObject = conf.get("graphObject");
            Gson gson = new Gson();
            graph = gson.fromJson(graphObject, Graph.class);

            System.out.println("Nodes: " + graph.nodes);

            if (context.getInputSplit() instanceof FileSplit) {
                this.input = ((FileSplit) context.getInputSplit()).getPath().toString();
            } else {
                this.input = context.getInputSplit().toString();
            }
        }
        public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
            if (offset.get() == 0){
                return;
            }
            /// get the input line as string and trim it.
            String line = lineText.toString();
            line = line.trim();
            String[] lineSegments = line.split(" ");
            String adjacency = "";
            for (String v : lineSegments){
                MapperLog.debug("Nodes: " + graph.nodes);
                if (!v.trim().isEmpty()) {
                    graph.addAnEdge((int) (offset.get()), Integer.parseInt(v.trim()));
                    adjacency += v + ":::1.0" + "###";
                }
            }
            if (adjacency.length() > 3) {
                adjacency = adjacency.substring(0, adjacency.length() - 3);
                context.write(new Text(offset.toString()), new Text(adjacency));
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private static final Logger ReducerLog = Logger.getLogger(Reduce.class);
        @Override
        public void reduce(Text node, Iterable<Text> adjacency, Context context) throws IOException, InterruptedException {
            for (Text value : adjacency) {
                context.write(node, value);
            }
        }
    }

}
