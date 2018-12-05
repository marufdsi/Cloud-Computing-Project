/**
 * Copyright:
 * Md Maruf Hossain
 * Department of Computer Science
 * University of North Carolina at Charlotte(UNCC) 2018
 */
package uncc.edu.maruf.louvain;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
public class Partition {
    public Partition(){
    }
    public static void testGraph(){
        System.out.println("Degree of node 5 by Louvain method: " + LouvainMethod.G.degree.get(5));
        System.out.println("Out Edge of node 5 by Louvain method: " + LouvainMethod.G.outEdge.get(5).get(1));
        System.out.println("Edge Weight of node 5 by Louvain method: " + LouvainMethod.G.outEdgeWeight.get(5).get(1));
    }
}
