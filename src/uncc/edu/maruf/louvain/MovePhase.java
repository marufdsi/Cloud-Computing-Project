/**
 * Copyright:
 * Md Maruf Hossain
 * Department of Computer Science
 * University of North Carolina at Charlotte(UNCC) 2018
 */
package uncc.edu.maruf.louvain;

import java.io.*;
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

public class MovePhase extends Configured implements Tool{
    private static final Logger MovePhaseLog = Logger.getLogger(Move.class);
    private int maxIteration = 15;
    public MovePhase(){

    }
    public static void detectCommunity(String[] args) throws Exception {
        int res = ToolRunner.run(new Move(), args);
        System.exit(res);
    }
    public int run(String[] args) throws Exception {
        int code = 0;
        int iteration = 0;
        do {
            LouvainMethod.changed = false;
            Move.tryMove(args);
            if (!LouvainMethod.changed){
                break;
            }
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "MovePhase");
            job.setJarByClass(this.getClass());
            /// Set the input file
            FileInputFormat.addInputPath(job, new Path(args[1]));
            /// Set the output file location
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            /// Add Mapper Class
            job.setMapperClass(CoarsenMap.class);
            /// Add CoarsenReduce Class
            job.setReducerClass(CoarsenReduce.class);
            /// Set intermediate output key as Text
            job.setOutputKeyClass(Text.class);
            /// Set intermediate output value as Integer format
            job.setOutputValueClass(Text.class);

            code = job.waitForCompletion(true) ? 0 : 1;
            iteration++;
        } while (iteration<maxIteration && LouvainMethod.changed);
        return code;
    }
}
