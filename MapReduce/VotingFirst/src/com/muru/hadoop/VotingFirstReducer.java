package com.muru.hadoop;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class VotingFirstReducer extends Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  
	  int counter = 0;
	  int numInd = 0;
	  int keyFound = 0;
	  ArrayList<String> arrList = new ArrayList<String>();
	  
	  for (Text value: values){
		 String str = value.toString().trim().toUpperCase();
		 
		 if (str.matches("[0-9]*")){
			 numInd = counter;
		 }
		  arrList.add(str);
		  
		  if (str.equals(key.toString().trim().toUpperCase())){
			  keyFound = 1;
		  }
		  
		  counter++;
	  }
			  
	  for(counter=0;counter < arrList.size(); counter++)
	  {
		  if (counter == numInd){			  
			  continue;
		  }
		  context.write(new Text(arrList.get(counter)), new Text(arrList.get(numInd)));
	  }
		  
	  if (keyFound == 0){		  
		  context.write(key, new Text("0"));
	  }
  }
}  

