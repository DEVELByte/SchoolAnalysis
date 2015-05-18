package com.mapreduce.mappers;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SchoolToStudentMapper extends Mapper<Object, Text, Text, Text> {
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] rows = value.toString().split("\\|");

		
		String rollnbr = rows[0];
		String school = rows[1];
		String name = rows[2];
		String Class = rows[5];
		String marks = rows[7];
		
		Text outKey = new Text(school + "\t" + Class + "-" + rollnbr + "-" + name );
		Text outValue = new Text(marks);
		
		context.write(outKey, outValue);
	}
}
