package uncc.edu.maruf.louvain;
public class LouvainMethod {
    public static void main(String[] args) throws Exception{
        System.out.println("Main Class");
        Mapper.map();
        Reducer.reduce();
        Graph.buildGraph();
    }
}
