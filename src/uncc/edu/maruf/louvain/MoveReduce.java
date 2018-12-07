/**
 * Copyright:
 * Md Maruf Hossain
 * Department of Computer Science
 * University of North Carolina at Charlotte(UNCC) 2018
 */
package uncc.edu.maruf.louvain;

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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MoveReduce extends Reducer<Text, Text, Text, Text> {
    private static final Logger MapperLog = Logger.getLogger(CoarsenReduce.class);

    @Override
    public void reduce(Text nodeinfo, Iterable<Text> adjacency, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        boolean moved = Boolean.parseBoolean(conf.get("moved"));
        for (Text value : adjacency) {
            context.write(nodeinfo, value);
        }
    }
    /// Cleanup method called at the last of Reduce. So, I perform cleanup and sorting in here.
    public void cleanup(Context context) throws IOException, InterruptedException{
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        String inputPath = conf.get("InputPath");
        boolean doNotDelete = Boolean.parseBoolean(conf.get("DoNotDelete"));
        if(!doNotDelete && fs.exists(new Path(inputPath))) {
//            fs.delete(new Path(inputPath), true);
        }
    }
}
