package com.mapreduce.mappers;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ClassToStudentMapper extends Mapper<Object, Text, Text, Text> {
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		System.out.println("ClassToStudentMapper: Started");
		String[] rows = value.toString().split("\\|");

		String rollnbr = rows[0];
		String name = rows[2];
		String Class = rows[5];
		String marks = rows[7];
		
		Text outKey = new Text(Class + "-" + rollnbr + "-" + name );
		Text outValue = new Text(marks);
		
		System.out.println("ClassToStudentMapper: complete:" + outKey + " : " + outValue);
		context.write(outKey, outValue);
	}
}
