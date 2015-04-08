package com.muru.hadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class VotingSecondMapper extends Mapper<Object, Text, Text, Text> {

  @Override
  protected void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
	  
	  Text voterPersonText = new Text();
	  Text worthText = new Text();
	  
	  StringTokenizer st = new StringTokenizer(value.toString(), "\t",false);    
	  
	  String token="";

	  
	  while (st.hasMoreTokens()){
		  token = st.nextToken().trim().toUpperCase();   //Voter & Person
		  voterPersonText.set(token);
		  token = st.nextToken().trim();   // Worth 
		  worthText.set(token);
		  context.write(voterPersonText, worthText);
	  }
	  
	  
  }
  

}
