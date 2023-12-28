//Places class defination

package edu.uwb.css534;
import java.util.*;

import edu.uw.bothell.css.dsl.MASS.MASS;
import edu.uw.bothell.css.dsl.MASS.Place;


public class PlacesCP extends Place{
    public static final int COLLECT = 1;
    private Set<Point> agentLst = new HashSet<Point>();
 
    //Constructor
    public PlacesCP(Object obj) { 
    }

    //Set the parent agent for a child agent
    public void setAgentSourceList(Point source) {
        if(this.agentLst.size() < 2) {
            this.agentLst.add(source);
        }
        
        return;
    }

    //Get the list of parent agent who has visited the place
    public Set<Point> getAgentSourceList() {
        return agentLst;
    }
    
    //List of Mass method being invoked
    public Object callMethod(int method, Object args) {
		switch (method) {
            case COLLECT:
                return collectAgents(args);
			default:
				return new String("Unknown Method Number: " + method);
		}
	}

    //Collect point of collision 
    public Object collectAgents(Object args) {
        
        Long size = Long.valueOf(this.getAgentSourceList().size());

        ArrayList<Point> result = new ArrayList<>();

        if(size > 1) {
            for(Point pt : this.getAgentSourceList()) {
               result.add(pt);
            }
            return result;
        }

        return null;
    }
}

