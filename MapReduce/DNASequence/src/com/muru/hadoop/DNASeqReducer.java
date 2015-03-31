package com.muru.hadoop;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DNASeqReducer extends Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  
	  int i=0;
	  StringBuilder userIdList = new StringBuilder();
	  
	  for(Text value : values)
	  {
			  if (i++ > 0){
				 userIdList.append(",");
			  }
			  userIdList.append(value.toString());
		 }	  
			  
	  context.write(key, new Text(userIdList.toString()));
  }
}  

