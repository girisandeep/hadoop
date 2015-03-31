package com.muru.hadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DNAMirrorSeqMapper extends Mapper<Object, Text, Text, Text> {

  @Override
  protected void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
	  
	  Text userIdText = new Text();
	  Text dnaText = new Text();
	  
	  StringTokenizer st = new StringTokenizer(value.toString(), " \t",false);    
	  
	  String token="";
	  String reverseToken = "";
	  
	  while (st.hasMoreTokens()){
		  token = st.nextToken().trim();   //user id
		  userIdText.set(token);
		  token = st.nextToken().trim().toUpperCase();   //dna 
		  reverseToken = (new StringBuilder(token).reverse()).toString();
		  if (token.compareTo(reverseToken) > 0)
		  {
			  token = reverseToken;
		  }
		  
		  dnaText.set(token);
		  context.write(dnaText, userIdText);
	  }
	  
	  
  }
  

}
