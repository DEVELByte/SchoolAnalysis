package com.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import com.mapreduce.chains.ClassToStudentChain;
import com.mapreduce.chains.GenderAnalysisChain;
import com.mapreduce.chains.SchoolToStudentChain;
import com.mapreduce.chains.SortStudents;

public class MainDriver {

	public static void main(String[] args) throws Exception {
		
		ToolRunner.run(new Configuration(), new ClassToStudentChain(), args);
		ToolRunner.run(new Configuration(), new SchoolToStudentChain(), args);
		ToolRunner.run(new Configuration(), new SortStudents(), args);
		ToolRunner.run(new Configuration(), new GenderAnalysisChain(), args);
	}
}