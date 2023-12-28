import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import scala.Tuple2;


public class ClosestPair implements Serializable {
        List<Tuple2<Integer, Integer>> closestPoints;
        Double distance = 0.0;
        List<Tuple2<Integer, Integer>> pointList;

        public ClosestPair(){
            closestPoints = new ArrayList<>();
            distance = 0.0;
            pointList = new ArrayList<>();
        }

        ClosestPair( List<Tuple2<Integer, Integer>> closestPoints, Double distance, List<Tuple2<Integer, Integer>> pointList) {
            if ( pointList != null ) {
                this.pointList = new ArrayList<>( pointList );
            } else {
                this.pointList = new ArrayList<>( );
            }
            
            if ( closestPoints != null ) {
                this.closestPoints = new ArrayList<>( closestPoints );
            } else {
                this.closestPoints = new ArrayList<>( );
            }

            this.distance = distance;
        }

};