package com.mapreduce.reducers;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GenderReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		System.out.println("GenderReducer: Sterted");

		String Class = key.toString();
		String gender = null;
		int boystotmarks = 0;
		int galstotmarks = 0;

		for (Text val : values) {
			String[] row = val.toString().split("-");
			gender = row[0];

			if (gender.equals("M"))
				boystotmarks += Integer.parseInt(row[1]);
			if (gender.equals("F"))
				galstotmarks += Integer.parseInt(row[1]);
		}
		System.out.println("GenderReducer: complete ---> " + boystotmarks + "->"
				+ galstotmarks );
		String outValue;
		if (boystotmarks > galstotmarks) {
			outValue = "Boys are leading in average marks";
		} else if (boystotmarks < galstotmarks) {
			outValue = "Girls are leading in average marks";
		} else {
			outValue = "Both Boys and Girls have equal marks";
		}

		System.out.println("GenderReducer: complete ---> " + Class + "->"
				+ gender + "->" + outValue);
		context.write(new Text(Class), new Text(outValue));
	}
}