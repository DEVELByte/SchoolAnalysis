package com.mapreduce.combiners;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TotalMarksCombiner extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		System.out.println("TotalMarksCombiner: Started -->" + key.toString());
		String[] compositKey = key.toString().split("-");
		int totMarks = 0;

		for (Text val : values) {
			totMarks += Integer.parseInt((val.toString()));
		}

		Text outKey = new Text(compositKey[0]);
		Text outValue = new Text(compositKey[1] + "-" + compositKey[2] + "-"
				+ totMarks);
		System.out.println("TotalMarksCombiner: complete" + outKey + "-->"
				+ outValue);
		context.write(outKey, outValue);
	}
}