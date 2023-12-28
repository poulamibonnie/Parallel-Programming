package edu.uwb.css534;
import java.io.Serializable;

public class Point implements Serializable{
   int x;
   int y;

   public Point(int x, int y) {
      this.x = x;
      this.y = y;
   }

   @Override
   public String toString() {
      return "(" + x + ", " + y + ")";
   }

   @Override
   public boolean equals(Object o) {
       if (this == o) return true;
       if (!(o instanceof Point)) return false;
       Point pt = (Point) o;
       return pt.x == x && pt.y == y;
   }

   @Override
   public int hashCode() {
       
       return this.toString().hashCode();
   }
}