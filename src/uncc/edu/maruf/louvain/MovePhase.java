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

public class MovePhase {
    private static final Logger MovePhaseLog = Logger.getLogger(Move.class);
    private static int maxIteration = 2;
    public MovePhase(){

    }
    public static String detectCommunity(String[] args) throws Exception {
        return performMovePhase(args);
    }
    public static String performMovePhase(String[] args) throws Exception {
        int code = 0;
        int iteration = 0;
        do {
            LouvainMethod.changed = false;
            String coarsenPath = "";
            if (iteration == 0) {
                coarsenPath = Move.tryMove(args[0], args[1]);
            } else {
                coarsenPath = Move.tryMove(args[2] + (iteration-1), args[1] + iteration);
            }
            /*if (!LouvainMethod.changed){
                break;
            }*/
            Configuration conf = new Configuration();
            Gson gson = new Gson();
            String graphObject = gson.toJson(LouvainMethod.G);
            conf.set("graphObject", graphObject);
            conf.set("InputPath", coarsenPath);
            Job job = Job.getInstance(conf, "MovePhase");
            conf.set("moved", String.valueOf(true));
            job.setJarByClass(MovePhase.class);
            /// Set the input file
            FileInputFormat.addInputPath(job, new Path(coarsenPath));
            /// Set the output file location
            FileOutputFormat.setOutputPath(job, new Path(args[2] + iteration));
            /// Add Mapper Class
            job.setMapperClass(CoarsenMap.class);
            /// Set reducer task
            job.setNumReduceTasks(1);
            /// Add CoarsenReduce Class
            job.setReducerClass(CoarsenReduce.class);
            /// Set intermediate output key as Text
            job.setOutputKeyClass(Text.class);
            /// Set intermediate output value as Integer format
            job.setOutputValueClass(Text.class);

            code = job.waitForCompletion(true) ? 0 : 1;
            GraphReader reader = new GraphReader();
            LouvainMethod.G = reader.buildGraph(args[2] + iteration  + "/part-r-00000", LouvainMethod.G.nodes, LouvainMethod.G.edges);
            LouvainMethod.G.initializeVolume();
            iteration++;
        } while (iteration<maxIteration /*&& LouvainMethod.changed*/);
        return args[2] + (iteration-1)  + "/part-r-00000";
    }
}
