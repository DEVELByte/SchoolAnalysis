package com.mapreduce.combiners;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SchoolToStudentCombiner extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		System.out.println("SchoolToStudentCombiner: Started");
		String[] compositKey = key.toString().split("-");
		String Class = compositKey[0];
		String rollnbr = compositKey[1];
		String name = compositKey[2];
		int totMarks = 0;

		for (Text val : values) {
			totMarks += Integer.parseInt((val.toString()));
		}

		Text outKey = new Text(Class);
		Text outValue = new Text(rollnbr + "-" + name + "-" + totMarks);
		System.out.println("SchoolToStudentCombiner: complete" +outKey+ "-->" + outValue);
		context.write(outKey, outValue);
	}
}