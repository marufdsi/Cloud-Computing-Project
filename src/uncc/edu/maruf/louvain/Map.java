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
        if (lineSegments.length<2)
            return;
        if (!lineSegments[1].trim().contains(":::"))
            return;
        context.write(new Text(lineSegments[0].trim()), new Text(lineSegments[1].trim()));
    }
}
