package com.aswinRoy.logsAnalysis.sessionizeJob;

import com.aswinRoy.logsAnalysis.models.LogModel;
import com.aswinRoy.logsAnalysis.utils.Utils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.Date;

/**
 *
 */
public class LogMapper extends Mapper<LongWritable, Text, Text, LogModel> {

    public Utils utils;
    private Logger logger = LoggerFactory.getLogger(LogMapper.class);


    @Override
    protected void setup(Context context) throws UnknownHostException {
        utils = new Utils();
    }

    @Override
    public void cleanup(Context context) {

    }

    @Override
    public void map(LongWritable key, Text value, Context context){
        String log = value.toString();

        LogModel logModel = new LogModel();
        logModel.parse(log);

        Date roundedTs = utils.getRoundedTs(logModel.getValue(LogModel.TIMESTAMP));
        logger.info("ts: {}", roundedTs);
        logger.info("key : {}", roundedTs.toString() + "#" + logModel.getValue(LogModel.CL_PORT));
        try {
            context.write(new Text(roundedTs.toString() + "#" +
                    logModel.getValue(LogModel.CL_PORT)), logModel);
                    //Emitting log model for each session per ip
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
