import org.apache.spark.SparkConf;                 
import org.apache.spark.api.java.JavaSparkContext; 
import org.apache.spark.api.java.JavaRDD;          
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;                  
import java.io.*;
import java.util.Map.Entry; 

public class TriangleCount{
    public static void main(String[] args) {
        // Parse command line arguments
        String inputFile = args[0];

        SparkConf conf = new SparkConf( ).setAppName( "Count Number Of Triangle" ); 
        JavaSparkContext jsc = new JavaSparkContext( conf );
        JavaRDD<String> lines = jsc.textFile( inputFile );

      
        // Initializing of the edges 
        JavaPairRDD<Integer, Integer> edges = lines.flatMapToPair( line -> {
            String[] parts = line.split(" ");
            String vertexId1 = parts[0].trim();
            String vertexId2 = parts[1].trim();

            Integer vertex1 = Integer.parseInt(vertexId1);
            Integer vertex2 = Integer.parseInt(vertexId2);

            // Create two pairs for bidirectional edges
            List<Tuple2<Integer, Integer>> pairs = new ArrayList<>();
            pairs.add(new Tuple2<>(vertex1, vertex2));
            pairs.add(new Tuple2<>(vertex2, vertex1));

            return pairs.iterator();
        });

        //Create Adjacency list in RDD
        JavaPairRDD<Integer, Iterable<Integer>> adjList = edges.groupByKey();

        //Create a map with adjaceny list for easy excess within RDD computations
        Map<Integer, ArrayList<Integer>> adjacentList = new HashMap<>();
        List<Tuple2<Integer, Iterable<Integer>>> edgeList = adjList.collect();
        for(Tuple2<Integer, Iterable<Integer>> node : edgeList) {
            for(Integer n : node._2){
                
               if (adjacentList.containsKey(node._1)) {
                adjacentList.get(node._1).add(n);
                } else {
                   ArrayList<Integer> newList = new ArrayList<>();
                    newList.add(n);
                    adjacentList.put(node._1, newList);
                }
                
            } 
        }

        //step1 : Travel through the edge where sourceVertex > intermediateVertex.
        JavaPairRDD<Integer, Iterable<Integer>> triangle = adjList.mapToPair(
            vertex -> {
            int source = vertex._1;
            ArrayList<Integer> neighbours = adjacentList.get(source);
            ArrayList<Integer> result = new ArrayList(); 

                for(Integer nbr :  vertex._2) {
                    int current = nbr;
                    if(current < source) {
                        result.add(current);
                    }
                }
                return new Tuple2<> (source, result);
        });

        // step2 : Travel through the edge where intermediateVertex > destinationVertex.
        triangle = triangle.mapToPair(vertex -> {
            int source = vertex._1;
            ArrayList<Integer> resultFinal = new ArrayList(); 
            for(Integer nbr :  vertex._2) {
                int intermediate = nbr;
                ArrayList<Integer> neighbours = adjacentList.get(intermediate);
              
                for(int j = 0; j < neighbours.size(); j++) {
                    int current = neighbours.get(j);
                    if(current < intermediate) {
                        resultFinal.add(current);
                    }
                }
                
            }
            return new Tuple2<> (source, resultFinal);
        });

        //Checking the adjacentList of destinationVertex to see if sourceVertex can be reached 
        //If yes, save triangle count
        JavaPairRDD<Integer, Integer> triangleCount = triangle.mapToPair(vertex -> {
            int source = vertex._1;
            int totalTriangle = 0;
            for(Integer nbr :  vertex._2) {
                int finalNode = nbr;
                ArrayList<Integer> neighbours = adjacentList.get(finalNode);
    
                for(int j = 0; j < neighbours.size(); j++) {
                    int current = neighbours.get(j);
                    if(source == current) {
                        totalTriangle++;
                        break;
                    }
                }
                
            }
            return new Tuple2<> (source, totalTriangle);
        });

        //Getting the global sum of number of triangles starting at different source
        int numberOfTriangles = 0;
        List<Tuple2<Integer, Integer>> resList = triangleCount.collect();
        for(Tuple2<Integer, Integer> node : resList) {
            numberOfTriangles += node._2;
        }
        
        System.out.println("Total number of triangles " + numberOfTriangles);

    }
}