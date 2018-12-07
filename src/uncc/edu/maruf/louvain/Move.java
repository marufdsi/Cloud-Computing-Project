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

public class Move {
    private static final Logger MoveLog = Logger.getLogger(Move.class);
    public static boolean moved = false;
    private static int maxIteration = 3;
    public static String tryMove(String input, String output) throws Exception{
        int code = 0;
        int iteration = 0;
        String returnPath = "";
        do {
            moved = false;
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            Gson gson = new Gson();
            String graphObject = gson.toJson(LouvainMethod.G);
            conf.set("graphObject", graphObject);
            conf.set("moved", String.valueOf(moved));
            String inputPath = "";
            if (iteration == 0) {
                inputPath = input;
                conf.set("DoNotDelete", String.valueOf(true));
            } else {
                inputPath = returnPath;
                conf.set("DoNotDelete", String.valueOf(false));
            }
            conf.set("InputPath", inputPath);
            Job job = Job.getInstance(conf, "Move");
            job.setJarByClass(Move.class);
            /// Set the input file
            FileInputFormat.addInputPath(job, new Path(inputPath));
            /// Set the output file location
            returnPath = output + iteration;
            Path outputPath = new Path(returnPath);
            if (fs.exists(outputPath)) {
                fs.delete(outputPath, true);
            }
            FileOutputFormat.setOutputPath(job, outputPath);
            /// Add Mapper Class
            job.setMapperClass(MoveMap.class);
            /// Set reducer task
            job.setNumReduceTasks(1);
            /// Add CoarsenReduce Class
            job.setReducerClass(MoveReduce.class);
            /// Set intermediate output key as Text
            job.setOutputKeyClass(Text.class);
            /// Set intermediate output value as Integer format
            job.setOutputValueClass(Text.class);

            code = job.waitForCompletion(true) ? 0 : 1;
            GraphReader reader = new GraphReader();
            LouvainMethod.G = reader.buildGraph(returnPath + "/part-r-00000", LouvainMethod.G.nodes, LouvainMethod.G.edges);
            LouvainMethod.G.initializeVolume();
            System.out.println("Moved: " + conf.get("moved"));
            moved = Boolean.parseBoolean(conf.get("moved"));
            if (moved){
                LouvainMethod.changed = true;
            }
            iteration++;
        } while (iteration<maxIteration /*&& moved*/);
        return returnPath;
    }
}
