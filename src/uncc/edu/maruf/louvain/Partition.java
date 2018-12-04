package uncc.edu.maruf.louvain;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
public class Partition {
    List<Integer> zeta;
    public Partition(Graph G){
        zeta = new ArrayList<>(G.nodes);
    }
    public void singletonCommunity(Graph G){
        zeta = new ArrayList<>(G.nodes);
        for (int u=0; u<G.nodes; ++u){
            zeta.add(u);
        }
    }
}
