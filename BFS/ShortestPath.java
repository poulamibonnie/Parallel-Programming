import org.apache.spark.SparkConf;                 // Spark Configuration
import org.apache.spark.api.java.JavaSparkContext; // Spark Context created from SparkConf
import org.apache.spark.api.java.JavaRDD;          // JavaRDD(T) created from SparkContext
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;                  
import java.io.*;
import java.util.Map.Entry; 


public class ShortestPath{
    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: BFSShortestPath <inputFile> <sourceVertex> <destinationVertex>");
            System.exit(1);
        }

        // Parse command line arguments
        String inputFile = args[0];
        String sourceVertex = args[1];
        String destinationVertex = args[2];

        SparkConf conf = new SparkConf( ).setAppName( "BFS-based Shortest Path Search" ); 
        JavaSparkContext jsc = new JavaSparkContext( conf );
        JavaRDD<String> lines = jsc.textFile( inputFile );

        // now start a timer
        long startTime = System.currentTimeMillis();

        // Initializing of the network and neighborList 
        JavaPairRDD<String, Data> network = lines.mapToPair( line -> {
            String[] parts = line.split("=");
            String vertexId = parts[0].trim();
            String[] neighborsWithWeights = parts[1].split(";");
            
            List<Tuple2<String, Integer>> neighborList = new ArrayList<>();
            for(int i = 0; i < neighborsWithWeights.length; i++) {
                String[] neighborAndWeights = neighborsWithWeights[i].split(",");
                neighborList.add(new Tuple2<>(neighborAndWeights[0],  Integer.parseInt(neighborAndWeights[1])));
            }
            
            String status = vertexId.equals(sourceVertex) ? "Active" : "Inactive";
            int distance  = vertexId.equals(sourceVertex) ? 0 : Integer.MAX_VALUE;
            int prev  = vertexId.equals(sourceVertex) ? 0 : Integer.MAX_VALUE;
            Data newData = new Data(neighborList, distance, prev, status);
            return new Tuple2<>(vertexId, newData);  
        });
        
        //If any vertex has status as active, this mean this vertex is ready for visit. Execute the loop

        while( network.filter(vertex -> vertex._2.status.equals("Active")).count() > 0 ) {

            // Create flatmappair containing the new distance from the source for each "Active" vertex to each of it's neighbour.
            // To mark the source as visited set status to "Inactive"
            JavaPairRDD<String, Data> propagatedNetwork = network.flatMapToPair( vertex-> {
                List<Tuple2<String, Data>> result = new ArrayList<>();
                if(vertex._2.status.equals("Active")) {
                    vertex._2.status = "Inactive";
                    for(Tuple2<String, Integer> nbr : vertex._2.neighbors){
                        int newDistance = nbr._2 + vertex._2.distance;
                        Data newData = new Data(null, newDistance, Integer.MAX_VALUE, "Inactive");
                        result.add(new Tuple2<>(nbr._1, newData));

                    }
                }
                result.add(new Tuple2<> (vertex._1, vertex._2));
                return result.iterator();
            } );


            // Here we select the shortest distance between two vertex for all JavaPairRDD if their key matches.
            network = propagatedNetwork.reduceByKey( ( k1, k2 ) ->{
                List<Tuple2<String, Integer>> neighborList;
                neighborList = (k1.neighbors.isEmpty()) ? k2.neighbors :  k1.neighbors;
                int shortestDistance = Math.min(k1.distance, k2.distance);
                int prevV = Math.min(k1.prev, k2.prev);
                return  new Data(neighborList, shortestDistance, prevV, "Inactive");  
                    
            } );

            // Mark all the JavaPairRDD as "Active" if distance < prev argument.
            // This essentially mean a path is found with shorter distance from the previous source vertex.
            // Marking it as "Active" esentially makes it the next source vertex and now all its route is to be considered.
            network = network.mapValues( value -> {
                if(value.distance < value.prev){
                    value.status = "Active";
                    value.prev = value.distance;
                }
                return value;
            } );
        }

        // Retrieve destination vertex distance
        //Integer shortestPath = network.lookup(destinationVertex).get(0).distance;
        Data destinationData = network.filter(vertex -> vertex._1().equals(destinationVertex)).first()._2;
        Integer shortestPath = destinationData.distance;

        // Stop timer
        long endTime = System.currentTimeMillis();

        // Print the result
        System.out.println("Shortest path from " + sourceVertex + " to " + destinationVertex + " is: " + shortestPath);
        System.out.println("Time taken: " + (endTime - startTime)/1000 + " seconds");
    }
}