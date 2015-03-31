package com.muru.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LetterFrequencyMapper extends Mapper<Object, Text, Text, LongWritable> {

  @Override
  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
	  
	  String line = (value.toString().toLowerCase()).replaceAll("[^a-z]", "");

	  int a[] = new int[26];
	  int pos=0;
	  
	  for (int i=0;i<line.length();i++){
		
		  pos = (int)line.charAt(i) - (int) 'a';
		  a[pos] = a[pos] + 1;
	  }
	  
	  for(int i=0; i < a.length; i++)
	  {
		  String ch = String.valueOf((char) (i + (int) 'a'));
		  context.write(new Text(ch), new LongWritable(a[i]));
	  }
  }
  
  /*	
  public static void main(String ar[]){
  String line = "abcdhjgfdi eASDaJDbSafd sdceicdflckjw ecindcflkbsbdbfbhlaaaaa".toString().toLowerCase();

	  int a[] = new int[26];
	  int pos=0;
	  
	  for (int i=0;i<line.length();i++){
		  if (line.charAt(i) == ' ') {
				  continue;
		  }
		  pos = (int)line.charAt(i) - (int) 'a';
		  a[pos] = a[pos] + 1;
//		  System.out.println(pos + " "+a[pos]);
	  }
	  
	  for(int i=0; i < a.length; i++)
	  {
		  char ch = (char) (i + (int) 'a');
	  }

	  
  } */
	  
}
