package com.muru.hadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AnagramMapper extends Mapper<Object, Text, Text, Text> {

  @Override
  protected void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
	  
	  Text sortText = new Text();
	  Text outText = new Text();
	  
	  StringTokenizer st = new StringTokenizer(value.toString(), " \n\t\r:.,()[]!?",false);    
	  
	  while (st.hasMoreTokens()){
		  String token = st.nextToken().trim().toLowerCase();
		  sortText.set(alphaSort(token));
		  outText.set(token);
		  context.write(sortText, outText);
	  }
	  
	  
  }
  
  protected String alphaSort(String curWord){
	  char[] charArr = curWord.toCharArray();
	  Arrays.sort(charArr);
	  return new String(charArr);
  }
	  
}
