import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class KeywordDocumentCount {

    public static class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        private Text documentCount = new Text();
        private Text keyword = new Text();
    
        JobConf conf;
        public void configure( JobConf job ) {
            this.conf = job;
        }

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String line = value.toString();
            int argc = Integer.parseInt( conf.get( "argc" ) );
            Set<String> keywords = new HashSet<String>(); 
            
            for (int i = 0; i < argc; i++) {
                keywords.add(conf.get("keyword" + i)); // Retrieve keywords using the correct key
            }
            Map<String, Integer> wordCount = new HashMap<String, Integer>();
            FileSplit fileSplit = ( FileSplit )reporter.getInputSplit( );
            String filename = "" + fileSplit.getPath( ).getName( );

            StringTokenizer tokenizer = new StringTokenizer(line);
            
	    //Get the total occurace of each keywords in mapper
            while (tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken();
                if (keywords.contains(word)) {
		    int count = wordCount.getOrDefault(word, 0);
                    wordCount.put(word, count + 1);
                }
            }
            
	    //Creating key value pair
            for (Map.Entry<String, Integer> entry : wordCount.entrySet()) {
		String word = entry.getKey();
                keyword.set(word);
                documentCount.set(filename + " " + wordCount.get(word));
                output.collect(keyword, documentCount);
            }

        }

    }

    public static class ReduceClass extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
            Map<String, Integer> documentCounts = new HashMap<String, Integer>();
            
	    //Agregating the key value pairs
            while (values.hasNext()) {
                String[] parts = values.next().toString().split(" ");
                String fileName = parts[0];
                int count = Integer.parseInt(parts[1]);
                int currCount = documentCounts.getOrDefault(fileName, 0);
		documentCounts.put(fileName, count + currCount);
            }

            StringBuilder result = new StringBuilder();
	    
	    //Flattening of the file
            for (Map.Entry<String, Integer> entry : documentCounts.entrySet()) {
                result.append(entry.getKey()).append(" ").append(entry.getValue()).append(" ");
            } 
            output.collect(key, new Text(result.toString()));
            
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Usage: KeywordDocumentCount <input_directory> <output_directory> <keyword1> <keyword2> ...");
            System.exit(1);
        }
        long begin = System.currentTimeMillis();
        JobConf conf = new JobConf(KeywordDocumentCount.class);
	    conf.setJobName("KeywordDocumentCount");
	
	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(Text.class);
	
	    conf.setMapperClass(MapClass.class);
	    conf.setCombinerClass(ReduceClass.class);
	    conf.setReducerClass(ReduceClass.class);
	
	    conf.setInputFormat(TextInputFormat.class);
	    conf.setOutputFormat(TextOutputFormat.class);
	
	    FileInputFormat.setInputPaths(conf, new Path(args[0]));
	    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        conf.set( "argc", String.valueOf( args.length - 2 ) );
        for ( int i = 0; i < args.length - 2; i++ )
            conf.set( "keyword" + i, args[i + 2] );
        
        JobClient.runJob(conf);
        long end = System.currentTimeMillis();
        long time = (end-begin)/1000;
        System.out.println();
        System.out.println("Elapsed Time: "+time);

    }
}
