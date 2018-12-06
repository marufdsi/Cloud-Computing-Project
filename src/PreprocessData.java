import java.io.*;
import java.util.StringTokenizer;

public class PreprocessData {
    public static void main(String[] args) throws Exception{
        System.out.println("Start Pre-process");
        String outputString = "";
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(args[0])));
        outputString = "Header:#:#:" + br.readLine() + "\n";
        String line = br.readLine();
        int u = 0;
        while (line != null) {
            StringTokenizer adjacencyList = new StringTokenizer(line);
            String adjacency = "";
            while (adjacencyList.hasMoreTokens()) {
                String neighbor = adjacencyList.nextToken();
                int v = Integer.parseInt(neighbor)-1;
                adjacency += v + ":::" + 1.0 + "###";
            }
            if (adjacency.length() > 3) {
                adjacency = adjacency.substring(0, adjacency.length() - 3);
                outputString += u + " " + adjacency + "\n";
            }
            u++;
            line = br.readLine();
        }
        System.out.println("Graph Reading Done");
        br.close();
        FileWriter fw = new FileWriter(new File(args[1]));
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(outputString);
        System.out.println("Graph Writing Done");
    }
}
