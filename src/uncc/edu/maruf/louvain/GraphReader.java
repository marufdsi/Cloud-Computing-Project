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
    public Preprocess preprocess(String filePath)throws Exception {
        this.path = filePath;
        return preprocess();
    }
    public Preprocess preprocess()throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
        String line = br.readLine();
        StringTokenizer graphInfo = new StringTokenizer(line);
        Preprocess graphBuilder = new Preprocess(Integer.parseInt(graphInfo.nextToken()), Integer.parseInt(graphInfo.nextToken()));
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
        br.close();
        return graphBuilder;
    }
    public Graph buildGraph(String filePath)throws Exception {
        this.path = filePath;
        return buildGraph();
    }
    public Graph buildGraph() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
        String line = br.readLine();
        Graph graphBuilder;
        if (line.contains("header")) {
            String[] graphInfo = line.split(" ");
            if (graphInfo.length <2){
                throw new Exception("Graph format invalid");
            }
            String[] nodesAndVertices = graphInfo[1].split("::##::");
            if (nodesAndVertices.length <2){
                throw new Exception("Graph format invalid");
            }
            graphBuilder = new Graph(Integer.parseInt(nodesAndVertices[0]), Integer.parseInt(nodesAndVertices[1]));
        } else {
            throw new Exception("Graph format invalid");
        }
        line = br.readLine();
        while (line != null) {
            String[] vertexInfo = line.split(" ");
            int u;
            if (vertexInfo.length <2){
                continue;
            }
            String fromVertex = vertexInfo[0];
            if(fromVertex.contains("::##::")){
                String[] communityInfo = fromVertex.split("::##::");
                u = Integer.parseInt(communityInfo[0]);
                graphBuilder.addToCommunity(u, Integer.parseInt(communityInfo[1]));
            } else {
                u = Integer.parseInt(fromVertex);
                graphBuilder.addToCommunity(u, u);
            }
            graphBuilder.addVertex(u);
            StringTokenizer adjacencyList = new StringTokenizer(vertexInfo[1], "###");
            while (adjacencyList.hasMoreTokens()) {

                String[] neighborInfo = adjacencyList.nextToken().split(":::");
                int v = Integer.parseInt(neighborInfo[0])-1;
                if (neighborInfo.length>=2){
                    graphBuilder.addAnEdge(u, v, Double.parseDouble(neighborInfo[1]));
                } else {
                    graphBuilder.addAnEdge(u, v);
                }
            }
            line = br.readLine();
        }
        br.close();
        return graphBuilder;
    }
}
