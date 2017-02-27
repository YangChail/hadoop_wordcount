package org.conan.myhadoop.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WordCountMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1); 
	@Override
	public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		 String line = value.toString();  
         String arry[]=line.split(" ");
         for(String str:arry){
        	 Text word = new Text(str);  
        	 output.collect(word, one);
         }
  //       StringTokenizer tokenizer = new StringTokenizer(line);  
//         Text word = new Text();  
//         while (tokenizer.hasMoreTokens())  
//         {  
//             word.set(tokenizer.nextToken());  
//             output.collect(word, one);  
//         }  
		
	}

}
