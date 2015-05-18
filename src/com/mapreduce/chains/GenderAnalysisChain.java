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
import com.mapreduce.mappers.ClassToGenderMapper;
import com.mapreduce.reducers.GenderReducer;

public class GenderAnalysisChain extends Configured implements Tool {

	//private static final String OUTPUT_PATH = "intermediate_output/D_GenderAnalysis";

	@Override
	public int run(String[] arg0) throws Exception {

		//
		// Job 1
		//

		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		//fs.delete(new Path(OUTPUT_PATH), true);
		fs.delete(new Path(arg0[1] + "/D_GenderAnalysis"), true);
		Job job = new Job(conf, "ClassGenderAggregation");
		job.setJarByClass(MainDriver.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(ClassToGenderMapper.class);
		//job.setCombinerClass(GenderReducer.class);
		job.setReducerClass(GenderReducer.class);

		FileInputFormat.setInputPaths(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1] + "/D_GenderAnalysis"));

		//job.waitForCompletion(true);

//		//
//		// Job 2
//		//
//
//		Configuration conf2 = getConf();
//		// FileSystem fs = FileSystem.get(conf);
//		Job job2 = new Job(conf2, "FinalClassGenderAggregation");
//		job2.setJarByClass(MainDriver.class);
//
//		job2.setOutputKeyClass(Text.class);
//		job2.setOutputValueClass(Text.class);
//
//		job2.setMapperClass(SecondClassToGenderMapper.class);
//		//job2.setCombinerClass(SecondGenderReducer.class);
//		job2.setReducerClass(SecondGenderReducer.class);
//
//		FileInputFormat.setInputPaths(job2, new Path(OUTPUT_PATH));
//		FileOutputFormat.setOutputPath(job2, new Path(arg0[1] + "/D_GenderAnalysis"));

		return job.waitForCompletion(true) ? 0 : 1;

	}

}
