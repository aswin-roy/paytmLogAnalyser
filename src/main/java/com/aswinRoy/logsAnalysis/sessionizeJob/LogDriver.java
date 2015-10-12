package com.aswinRoy.logsAnalysis.sessionizeJob;

import com.aswinRoy.logsAnalysis.models.LogModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *
 */
public class LogDriver {

    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(LogDriver.class);
        Configuration config = new Configuration();

        Job job = Job.getInstance(config, "LogsAnalyser");

        job.setJobName("LogsAnalyser");
        job.setJarByClass(LogDriver.class);

        job.setMapperClass(LogMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LogModel.class);
        job.setReducerClass(LogReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        try {
            job.waitForCompletion(true);
        } catch (Exception e) {
            logger.info("Exception in job execution.");
        }
    }

}
