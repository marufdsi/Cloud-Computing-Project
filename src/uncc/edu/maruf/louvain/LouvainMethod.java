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
public class LouvainMethod extends Configured implements Tool {
    public static Graph G;
    public static void main(String[] args) throws Exception{
        GraphReader reader = new GraphReader(args[0]);
        G = reader.buildGraph();
        System.out.println("Create Singleton Community");
        G.singletonCommunity();
        System.out.println("Graph Creation Done");
        G.saveGraphIntoHadoopFormat(args[1]);
        System.out.println("Save Graph");
        int res = ToolRunner.run(new LouvainMethod(), args);
        System.exit(res);
    }
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "LouvainMethod");
        job.setJarByClass(this.getClass());
        /// Set the input file
        FileInputFormat.addInputPath(job, new Path(args[1]));
        /// Set the output file location
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        /// Add Mapper Class
        job.setMapperClass(Map.class);
        /// Add Reduce Class
        job.setReducerClass(Reduce.class);
        /// Set intermediate output key as Text
        job.setOutputKeyClass(Text.class);
        /// Set intermediate output value as Integer format
        job.setOutputValueClass(Text.class);

        int code =  job.waitForCompletion(true) ? 0 : 1;
        return code;
    }
}
