package com.muru.hadoop;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AnagramReducer extends Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  
	  int i=0;
	  StringBuffer anagramList = new StringBuffer();
	  Set<Text> uniqueVal = new HashSet<Text>();
	  
	  for(Text value : values)
	  {
		 if (uniqueVal.add(value)){
			 
			  if (i++ > 0){
				 anagramList.append(",");
			  }
			  anagramList.append(value.toString());
		 }	  
			  
	  }
	  context.write(new Text(""), new Text(anagramList.toString()));
  }
}
