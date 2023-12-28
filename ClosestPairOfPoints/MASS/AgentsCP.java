//Agent class defination

package edu.uwb.css534;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import edu.uw.bothell.css.dsl.MASS.Agent;
import edu.uw.bothell.css.dsl.MASS.MASS;

public class AgentsCP extends Agent {
    public static final int SET_START_POSITION = 0;
    public static final int SPAWN = 1;
    public static final int MIGRATE = 2;
    public static final int KILL = 3;
    public static final int ARRIVE = 4;


    private int agentId;
    private Point nextLocation = null; 
    private Point source = null;


    public AgentsCP(Object obj) {
        if (obj != null)
        {
            AgentDetails agentDetail = (AgentDetails) obj;
            this.source = agentDetail.source;
            this.nextLocation = agentDetail.nextLocation;
        }

    }

    public Object callMethod(int method, Object args) {
		switch (method) {
			case SET_START_POSITION:
				return setStartPosition(args);
            case SPAWN:
				return spawnAgents(args);
            case MIGRATE:
				return migrateAgents(args);
            case ARRIVE:
                return onArrival(args);
            case KILL:
				return killAgents(args);
			default:
				return new String("Unknown Method Number: " + method);
		}
	}

    // Initiating first set of agents.
    public Object setStartPosition(Object args) {
        ArrayList<Point> pointsList = (ArrayList<Point>) args;
        this.agentId = getAgentId();
        this.source = pointsList.get(agentId);
        this.migrate(this.source.x, this.source.y);
        return null;
    }

    //Spawn new child agents for migration to neighbourhood.
    public Object spawnAgents(Object args) {
        int nAgent = (Integer) args;
        

        PlacesCP grid = (PlacesCP) getPlace();
        //Get curnt coordinates
        int currX = grid.getIndex()[0];
        int currY = grid.getIndex()[1];

        //Get the boundary 
        int sizeX = grid.getSize()[0];
        int sizeY = grid.getSize()[1];

        int newX = 0;
        int newY = 0;

        //Von Neumann Migration = 4 children spawning
        int[] dr_v = {0, 0, 1, -1};
        int[] dc_v = {1, -1, 0, 0};

        //Moore Migration = 8 children spawning
        int[] dr_m = {0, 0, 1, -1, 1, 1, -1, -1};
        int[] dc_m = {-1, 1, 0, 0, -1, 1, -1, 1};

        //Construct am array with all the next location
        List<AgentDetails> nextAgentDetails = new ArrayList<>();        
        for(int i = 0; i < nAgent; i++) {
            if(nAgent == 4) {
                newX = currX + dr_v[i];
                newY = currY + dc_v[i];
            }
            else{
                newX = currX + dr_m[i];
                newY = currY + dc_m[i];
            }
            
            //Checking for valid neighbourhood
            if(newY < 0 || newX < 0 || newX >= sizeX || newY >= sizeY) {
                continue;
            }
            Point newSource = this.source;
            Point newNextLocation = new Point(newX, newY);
            nextAgentDetails.add(new AgentDetails( newSource ,  newNextLocation));
        }

        //Call spawn library function with the next set of location
        this.spawn(nextAgentDetails.size(), nextAgentDetails.toArray());

        //Kill older agents to save memory footprint.
        this.kill();
        return null;
    }

    // Invoke migrate library function with the next location
    public Object migrateAgents(Object args) {
        this.migrate(this.nextLocation.x, this.nextLocation.y);
        return null;
    }

    // OnArrival to a place register it's source to the place
    public Object onArrival(Object args) {
        PlacesCP place = (PlacesCP) getPlace();
        place.setAgentSourceList(this.source);
        return null;
    }

    //Killing Duplicate agents
    public Object killAgents(Object args) {
        //Get all agents in the current place
        PlacesCP place = (PlacesCP) getPlace();
        Set<Point> prevAgents = place.getAgentSourceList();
 
        //If Collision has already happened
        if (prevAgents.size() > 1)
        {
            this.kill();
            return null;
        }

        //If duplicate agents are found
        if (prevAgents.contains(this.source)){
            this.kill();
        }

        return null;
    }

}