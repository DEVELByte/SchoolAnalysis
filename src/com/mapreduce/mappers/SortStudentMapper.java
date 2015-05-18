package com.mapreduce.mappers;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortStudentMapper extends Mapper<Object, Text, Text, Text> {
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		System.out.println("SortStudentMapper: Started");
		String[] rows = value.toString().split("\\|");
		
		String rollnbr = rows[0];
		String school = rows[1];
		String name = rows[2];
		String marks = rows[7];
		
		Text outKey = new Text(school + "-" + rollnbr + "-" + name );
		Text outValue = new Text(marks);
		System.out.println("SortStudentMapper: Completed");
		context.write(outKey, outValue);
	}
}