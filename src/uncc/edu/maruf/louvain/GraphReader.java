/**
 * Copyright:
 * Md Maruf Hossain
 * Department of Computer Science
 * University of North Carolina at Charlotte(UNCC) 2018
 */
package uncc.edu.maruf.louvain;

import java.io.BufferedReader;
import java.io.*;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class GraphReader {
    private String path;
    public GraphReader(String filePath){
        path = filePath;
    }
    public Integer getGraphInfo() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
        String line = br.readLine();
        StringTokenizer graphInfo = new StringTokenizer(line);
        // Return number of vertices belongs to graph
        return Integer.parseInt(graphInfo.nextToken());
    }
}
