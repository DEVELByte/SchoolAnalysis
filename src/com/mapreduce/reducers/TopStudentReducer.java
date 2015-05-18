package com.mapreduce.reducers;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopStudentReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		System.out.println("ClassToStudentReducer: Started");
		String name = null;
		String rollnbr = null;
		String totMarks = null;
		int count = 0;
		int higestMarks = 0;
		String student = null;

		for (Text val : values) {
			if (count == 0) {
				String[] row = val.toString().split("-");
				rollnbr = row[0];
				name = row[1];
				totMarks = row[2];
				higestMarks = Integer.parseInt(totMarks);
				student = rollnbr + "\t" + name;
			} else {
				String[] row = val.toString().split("-");
				rollnbr = row[0];
				name = row[1];
				totMarks = row[2];
				if (higestMarks < Integer.parseInt(totMarks)) {
					higestMarks = Integer.parseInt(totMarks);
					student = rollnbr + "\t" + name;
				} else if (higestMarks == Integer.parseInt(totMarks)) {
					student = student + "\t" + rollnbr + "\t" + name;
				}
			}
		}

		// System.out.println("SecondGenderReducer: key" + key.toString());
		// System.out.println("SecondGenderReducer: key" + outValue);
		System.out.println("SecondGenderReducer: complete");
		context.write(key, new Text(student + "\t" + higestMarks));
	}
}