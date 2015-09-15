/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package main;

import inputFormats.CustomInputFormat2;
import mappers.CustomMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import outputFormats.CustomOutputFormat;
import outputValues.ScoreKeeperOutput;
import reducers.CustomReducer;

/**
 *
 * @author kalanihalemano
 */
public class MutationFrequency {


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Mutation");
//        job.setJarByClass(MutationFrequency.class);
        job.setOutputFormatClass(CustomOutputFormat.class);
        job.setInputFormatClass(CustomInputFormat2.class);
        job.setMapperClass(CustomMapper.class);
        job.setReducerClass(CustomReducer.class);
        job.setCombinerClass(CustomReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ScoreKeeperOutput.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}