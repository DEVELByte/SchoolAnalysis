package com.mapreduce.mappers;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ClassToGenderMapper extends Mapper<Object, Text, Text, Text> {

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		System.out.println("ClassToGenderMapper: Started");
		
		String[] rows = value.toString().split("\\|");

		String gender = rows[4];
		String Class = rows[5];
		String marks = rows[7];
		
		Text outKey = new Text(Class);
		Text outValue = new Text(gender + "-" + marks);
		
		System.out.println("ClassToGenderMapper: complete:" + outKey + " : " + outValue);
		context.write(outKey, outValue);
	}
}