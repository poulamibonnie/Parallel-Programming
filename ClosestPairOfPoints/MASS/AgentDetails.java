package edu.uwb.css534;
import java.io.Serializable;

public class AgentDetails implements Serializable {
   Point source;
   Point nextLocation;
   

   public AgentDetails(Point source, Point nextLocation) {
      this.source = source;
      this.nextLocation = nextLocation;
   }
}