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
    public Graph buildGraph() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
        String line = br.readLine();
        StringTokenizer graphInfo = new StringTokenizer(line);
        Graph graphBuilder = new Graph(Integer.parseInt(graphInfo.nextToken()), Integer.parseInt(graphInfo.nextToken()));
        line = br.readLine();
        int u = 0;
        while (line != null) {
            StringTokenizer adjacencyList = new StringTokenizer(line);
            while (adjacencyList.hasMoreTokens()) {
                String neighbor = adjacencyList.nextToken();
                int v = Integer.parseInt(neighbor)-1;
                graphBuilder.addAnEdge(u, v);
            }
            u++;
            line = br.readLine();
        }
        return graphBuilder;
    }
}
