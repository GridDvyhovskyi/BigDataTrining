package com.grid;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJobName("searchCount");
        job.setJarByClass(Main.class);

        job.setMapOutputValueClass(RequestCountPair.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(SearchMapper.class);
        job.setCombinerClass(SearchCombine.class);
        job.setReducerClass(SearchReduce.class);

        Path inputFilePath = new Path("/Users/dvyhovskyi/IdeaProjects/SearQueryTask/input/input_data_basic.tsv");
        Path outputFilePath = new Path("/Users/dvyhovskyi/IdeaProjects/SearQueryTask/output");

        FileInputFormat.addInputPath(job, inputFilePath);
        FileOutputFormat.setOutputPath(job, outputFilePath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String [] args) throws Exception {

        int exitCode = ToolRunner.run(new Main(), args);
        System.exit(exitCode);

    }
}