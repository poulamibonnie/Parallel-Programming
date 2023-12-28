#!/bin/sh

/usr/lib/jvm/java-1.8.0/bin/javac -cp jars/spark-core_2.11-2.3.1.jar:jars/spark-sql_2.11-2.3.1.jar:jars/scala-library-2.11.8.jar:google-collections-1.0.jar ClosestPairPoint.java ClosestPair.java
/usr/lib/jvm/java-1.8.0/bin/jar -cvf ClosestPairPoint.jar ClosestPair*.class
