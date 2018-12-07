/**
 * Copyright:
 * Md Maruf Hossain
 * Department of Computer Science
 * University of North Carolina at Charlotte(UNCC) 2018
 */
package uncc.edu.maruf.louvain;

import java.io.*;

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

import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CoarsenMap extends Mapper<LongWritable, Text, Text, Text> {
    private static final Logger CoarsenMapperLog = Logger.getLogger(MoveMap.class);
    private String input;

    Graph graph;
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String graphObject = conf.get("graphObject");
        Gson gson = new Gson();
        graph = gson.fromJson(graphObject, Graph.class);

        if (context.getInputSplit() instanceof FileSplit) {
            this.input = ((FileSplit) context.getInputSplit()).getPath().toString();
        } else {
            this.input = context.getInputSplit().toString();
        }
    }

    private final Pattern WORD_BOUNDARY = Pattern.compile(" ");

    @Override
    public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
        String line = lineText.toString();
        line = line.trim();

        String[] lineSegments = line.split("\\s+");
        if (lineSegments.length <2){
            throw new IOException("No neighbor information");
        }
        if (!lineSegments[1].trim().contains(":::")) {
            throw new IOException("No edge weight");
        }

        String fromVertex = lineSegments[0];
        if(!fromVertex.contains("::##::")) {
            throw new IOException("No community information");
        }
        String[] communityInfo = fromVertex.split("::##::");
        String node = communityInfo[0];
        String elementsOfCommunity = "";
        String community = "";
        if (communityInfo[1].contains("@::@")){
            String[] tokens = communityInfo[1].split("@::@");
            community = tokens[0];
            elementsOfCommunity = tokens[1];
        } else {
            community = communityInfo[1];
            elementsOfCommunity = node;
        }

        String nodeInfo = lineSegments[0];
        context.write(new Text(community), new Text(elementsOfCommunity + "##::@@::##" + lineSegments[1]));
    }
}
