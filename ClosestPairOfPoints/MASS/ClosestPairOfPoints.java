package edu.uwb.css534;

import java.util.Scanner;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.lang.Math; 


import edu.uw.bothell.css.dsl.MASS.Agents;
import edu.uw.bothell.css.dsl.MASS.MASS;
import edu.uw.bothell.css.dsl.MASS.Places;
import edu.uw.bothell.css.dsl.MASS.logging.LogLevel;

public class ClosestPairOfPoints { 
    private static final String NODE_FILE = "nodes.xml";

    public static Double getDistance(Point a, Point b) {
        return (Double) Math.sqrt(Math.pow(a.x - b.x, 2) + Math.pow(a.y - b.y, 2));
    }

    //Driver Program
    public static void main(String[] args) throws Exception
    {   
        MASS.setNodeFilePath( NODE_FILE );
        ArrayList<Point> points = new ArrayList<>();
        String fileName = "/home/NETID/pdghosh/mass_quickstart/Quickstart/src/main/java/edu/uwb/css534/coordinates.txt";

        try {
            Scanner scanner = new Scanner(new File(fileName));

            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                String[] split = line.split(",");
                
                points.add(new Point(Integer.parseInt(split[0]), Integer.parseInt(split[1])));
            }
            scanner.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        int noOfPoints = points.size();
        int maxX = Integer.MIN_VALUE;
        int maxY = Integer.MIN_VALUE;
        
        for(int i = 0; i < noOfPoints; i++) {
            Point pt = points.get(i);
            maxX = Math.max(maxX, pt.x);
            maxY = Math.max(maxY, pt.y);
        }

        // Starting Mass
        MASS.init();

        //Start timer To be added
        long startTime = System.currentTimeMillis();

        // Create the Places matrix of the dimension = maxX * maxY
        Places placesCP = new Places(1, PlacesCP.class.getName() ,  null, maxX + 1, maxY + 1);

        // Create Agents = the no of points 
        Agents agentsCP = new Agents(2, AgentsCP.class.getName(), null, placesCP, noOfPoints);

        // //Set start position for each agent
        agentsCP.callAll(AgentsCP.SET_START_POSITION, points);
        agentsCP.manageAll();
 
        Object candidateData[] = null;
        int counter = 0;
        boolean resultFound = false;
        Double minDistance = Double.MAX_VALUE;
        Point closestPt1 = null;
        Point closestPt2 = null;
      
        while( !resultFound ) {

            if(counter % 2 == 0) {
                //Spawn 8 children : Moore Migration
                agentsCP.callAll(AgentsCP.SPAWN,8);
                agentsCP.manageAll();
            }
            else{
                //Spawn 4 children : Von Neumann Migration
                agentsCP.callAll(AgentsCP.SPAWN, 4 );
                agentsCP.manageAll();
            }
        
            //Migrate to new neighbourhood
            agentsCP.callAll(AgentsCP.MIGRATE);
            agentsCP.manageAll();

            //Kill duplicates agents
            agentsCP.callAll(AgentsCP.KILL);
            agentsCP.manageAll();

            //On Arrival Register it Parent Source
            agentsCP.callAll(AgentsCP.ARRIVE);
            agentsCP.manageAll();

            //Collect Collision Details if happened
	        candidateData = ( Object[] )placesCP.callAll(PlacesCP.COLLECT, null );
      
            //Checking for collision and finding the closest pair of points.
            for (Object oArr : candidateData) {
                if(oArr != null) {
                    resultFound = true;
                    List<Point> pointResults = new ArrayList<Point>();
                    pointResults.addAll((ArrayList<Point>) oArr);
                    Point p1 = pointResults.get(0);
                    Point p2 = pointResults.get(1);
                    Double distance = getDistance(p1 ,p2);
                    if(distance < minDistance) {
                        minDistance = distance;
                        closestPt1 = p1;
                        closestPt2 = p2;
                    }
                }
            }
            counter++;
         }

        System.out.println( "Closest pair of points are " + closestPt1 + " and " +  closestPt2 + "and distance = " + minDistance);

        MASS.finish();
        
        // display execution time
		long execTime = new Date().getTime() - startTime;
		System.out.println( "Execution time = " + execTime + " milliseconds" );

    }
}
