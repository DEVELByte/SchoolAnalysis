package com.mapreduce.reducers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.mapreduce.others.ValueComparator;

public class SortStudentsMarksReducer extends
		Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		System.out.println("SortStudentsMarksReducer: Started");
		HashMap<String, Integer> UnsortedStudentDetails = new HashMap<String, Integer>();
		ValueComparator compair = new ValueComparator(UnsortedStudentDetails);
		TreeMap<String, Integer> sortedStudentDetails = new TreeMap<String, Integer>(
				compair);
		
		
		for (Text val : values) {
			String[] row = val.toString().split("-");
			System.out.println("SortStudentsMarksReducer: Started -->" + row[0] + "\t" + row[1] + "\t"+
					Integer.parseInt(row[2]));
			UnsortedStudentDetails.put(row[1], Integer.parseInt(row[2]));
		}
		
		sortedStudentDetails.putAll(UnsortedStudentDetails);
		System.out.println("SortStudentsMarksReducer: complete -->" + sortedStudentDetails);
		
		for(Map.Entry<String, Integer> x : sortedStudentDetails.entrySet()){
			context.write(key, new Text(x.getKey() + "\t" + x.getValue()));
		}
		
		
	}
}
