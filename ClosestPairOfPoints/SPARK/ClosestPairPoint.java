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
import java.util.Collections;
import java.util.Comparator;

public class ClosestPairPoint {
    
    // Custom Comparator to sort by Y-coordinates
    static class SortbyYCoordinates implements Comparator<Tuple2<Integer, Integer>> {
        public int compare(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b)
        {
            return a._2 - b._2;
        }
    }

    // Helper function to find distance between two pints
    public static Double distance(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b){
        return (Double) Math.sqrt(Math.pow(a._1 - b._1, 2) + Math.pow(a._2 - b._2, 2));
    }
    
    // Driver Block
    public static void main(String[] args) {

        // Checking Input Parameter
        if (args.length != 1) {
            System.err.println("Usage: closestPairPoint <inputFile>");
            System.exit(1);
        }
        
        // Parse command line argument
        String inputFile = args[0];
       
        // Initiate Spark context & read the file
        SparkConf conf = new SparkConf( ).setAppName( "Closest Pair Points" ); 
        JavaSparkContext spark = new JavaSparkContext( conf );
        JavaRDD<String> pointsFile = spark.textFile( inputFile );

        // Now start a timer
        long startTime = System.currentTimeMillis();

        // Initializing of the points in input space
        JavaRDD<Tuple2<Integer, Integer>> points = pointsFile.map( line -> {
            String[] parts = line.split(",");
            Integer xCoordinate = Integer.parseInt(parts[0].trim());
            Integer yCoordinate = Integer.parseInt(parts[1].trim());
            
            return new Tuple2<>(xCoordinate, yCoordinate);  
        });
        
        // Sort by x cordinates
        JavaRDD<Tuple2<Integer, Integer>> tempSortedRDD = points.sortBy(Tuple2::_1, true, 2);

        // Index all the rows for the RDD
        List<Tuple2<Integer, Integer>> sortedList = tempSortedRDD.collect();
        List<Tuple2<Long,Tuple2<Integer, Integer>>> mPoints = new ArrayList<>();

        for(int i = 0; i < sortedList.size(); i++){
            Tuple2<Integer, Integer> pt = sortedList.get(i);
            mPoints.add(new Tuple2<>(Long.valueOf(i/3), pt));

        }

        JavaPairRDD<Long, Tuple2<Integer, Integer>> SortedRDD = spark.parallelizePairs(mPoints);
      
        // Group by key and store the list of point in each stripe
        JavaPairRDD<Long, Iterable<Tuple2<Integer, Integer>>> stripesList = SortedRDD.groupByKey();

        // Find the local closest pair in each stripe with brute force approach
        JavaPairRDD<Long, ClosestPair> closestPairRDD = stripesList.mapToPair( obj -> {
            
            List<Tuple2<Integer, Integer>> pointList = new ArrayList();
            List<Tuple2<Integer, Integer>> closestPoints  = new ArrayList();
            Double minDis = Double.MAX_VALUE;
            Long stripeNumber = obj._1;
            int size = 0;
            
            // Collect all the points in the stripe
            for(Tuple2<Integer, Integer> pt : obj._2){
                pointList.add(pt);
            }
            size = pointList.size();

            // Edge case: if only one point on the stripe 
            if(size == 1) {
                ClosestPair cp = new ClosestPair(null, minDis, pointList);
                return new Tuple2 <> (stripeNumber, cp);
            }

            // Bruteforce calculation of local minDistance (max(size) = 3)
            for(int i = 0; i < size; i++) {
                for(int j = i + 1; j < size; j++) {
                    Double dis = distance(pointList.get(i), pointList.get(j));
                    if(minDis > dis) {
                        minDis = dis;
                        closestPoints.clear();
                        closestPoints.add(pointList.get(i));
                        closestPoints.add(pointList.get(j));
                    }
                }
            }
            
            ClosestPair cp = new ClosestPair(closestPoints, minDis, pointList);
            return new Tuple2 <> (stripeNumber, cp);
        }).cache();
        
        while( closestPairRDD.count() > 1) {

            // Strip number modified for adjacent stripes
            closestPairRDD = closestPairRDD.mapToPair(stripe -> {
                Long stripeNumber = stripe._1/2;
                return new Tuple2<>(stripeNumber,stripe._2);
            });

            // Merge each stripe by reducing by key
            closestPairRDD = closestPairRDD.reduceByKey( (k1, k2) -> {
                List<Tuple2<Integer, Integer>> ptList = new ArrayList();
                List<Tuple2<Integer, Integer>> clPoints = new ArrayList();
                List<Tuple2<Integer, Integer>> strip  = new ArrayList();
                List<Tuple2<Integer, Integer>> stripCP  = new ArrayList();
                Double stripDis = Double.MAX_VALUE;
                Double minDis = Double.MAX_VALUE;

                // Get the min Distance between stripe
                minDis = Math.min(k1.distance, k2.distance);

                // Insert all list of points from left stripe 
                for(Tuple2<Integer, Integer> pt : k1.pointList) {
                    ptList.add(pt);
                }

                // Insert all list of points from right stripe 
                for(Tuple2<Integer, Integer> pt : k2.pointList) {
                    ptList.add(pt);
                }

                // Getting the closest points across adjacent stripe
                Tuple2<Integer, Integer> a = k1.pointList.get(k1.pointList.size() - 1);
                Tuple2<Integer, Integer> b = k2.pointList.get(k2.pointList.size() - 1);
                int median  = (a._1 + b._1)/2;

                // Identifying the strip boundary
                Double left = median - minDis;
                Double right = median + minDis;

                // Get the points along the boundry that are probable candidates for shortest path
                for(int i = 0; i < ptList.size(); i++) {
                    Tuple2<Integer, Integer> pt = ptList.get(i);
                    if(pt._1 >= left && pt._1  <= right){
                        strip.add(pt);
                    }
                }

                // Sort by y-cordinates within the strip
                Collections.sort(strip, new SortbyYCoordinates());

                int size = strip.size();

                // Finding the closest point in the boundary strip
                // At any point we only need to check for next 7 points
                for(int i = 0; i < size; i++) {
                    for(int j = i + 1; j < Math.min(i + 7,size); j++) {
                        Double dis = distance(strip.get(i), strip.get(j));
                        if(stripDis > dis) {
                            stripDis = dis;
                            stripCP.clear();
                            stripCP.add(strip.get(i));
                            stripCP.add(strip.get(j));
                        }
                    }
                }

                // Getting the shortest distance after strip merging 
                minDis = Math.min(minDis, stripDis);

                // Getting the shortest pair of points
                if(Double.compare(minDis, k1.distance) == 0) {
                    clPoints.add(k1.closestPoints.get(0));
                    clPoints.add(k1.closestPoints.get(1));
                }
                else if(Double.compare(minDis, k2.distance) == 0) {
                    clPoints.add(k2.closestPoints.get(0));
                    clPoints.add(k2.closestPoints.get(1));
                }
                else{
                    clPoints.add(stripCP.get(0));
                    clPoints.add(stripCP.get(1));
                }

                return new ClosestPair(clPoints, minDis, ptList);
                
            }).cache();

        }

        //Get the global closest pair of points
        Tuple2<Long, ClosestPair> poinList = closestPairRDD.collect().get(0);

        Double distance =  poinList._2.distance;
        String closestPair1 = "("+ poinList._2.closestPoints.get(0)._1 +","+ poinList._2.closestPoints.get(0)._2 + ")";
        String closestPair2 = "("+ poinList._2.closestPoints.get(1)._1 +","+ poinList._2.closestPoints.get(1)._2 + ")";

        // Stop timer
        long endTime = System.currentTimeMillis();
       
        System.out.println("\n\nShortest Distance Found = " + distance + " between points " +  closestPair1 +  " and "+ closestPair2 + "\n\n");      
        System.out.println("Time taken: " + (endTime - startTime) + " ms");

        spark.stop();     
    }
}
