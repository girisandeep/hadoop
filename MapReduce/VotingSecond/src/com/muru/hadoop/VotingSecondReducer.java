package com.muru.hadoop;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class VotingSecondReducer extends Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  
	  int sum = 0;
	  
	  for (Text value: values){
		 sum += Integer.valueOf(value.toString()).intValue();
	  }

	  context.write(key, new Text(String.valueOf(sum)));
  }
}  

