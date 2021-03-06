package com.mapreduce.chains;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.mapreduce.MainDriver;
import com.mapreduce.combiners.TotalMarksCombiner;
import com.mapreduce.mappers.SortStudentMapper;
import com.mapreduce.reducers.SortStudentsMarksReducer;

public class SortStudents extends Configured implements Tool {

	//private static final String OUTPUT_PATH = "intermediate_output/D_GenderAnalysis";

	@Override
	public int run(String[] arg0) throws Exception {

		//
		// Job 1
		//

		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		//fs.delete(new Path(OUTPUT_PATH), true);
		fs.delete(new Path(arg0[1] + "/C_SortStudents"), true);
		Job job = new Job(conf, "SortStudents");
		job.setJarByClass(MainDriver.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(SortStudentMapper.class);
		job.setCombinerClass(TotalMarksCombiner.class);
		job.setReducerClass(SortStudentsMarksReducer.class);

		FileInputFormat.setInputPaths(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1] + "/C_SortStudents"));

		return job.waitForCompletion(true) ? 0 : 1;

	}

}
