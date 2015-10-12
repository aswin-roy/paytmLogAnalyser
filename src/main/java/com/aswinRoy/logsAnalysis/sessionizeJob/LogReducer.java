package com.aswinRoy.logsAnalysis.sessionizeJob;

import com.aswinRoy.logsAnalysis.models.LogModel;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 *
 */
public class LogReducer extends TableReducer< Text, LogModel, Text> {

    private HashMap<String,HashSet<String>> uniqueURLs = new HashMap<>();
    private HashMap<Long, ArrayList<String>> sessionAndIpMap = new HashMap<>();//Map of each session to its corresponding ip
    //This map sorted based on key will have longest sessions with related ips
    private long totalSessionTime;
    private int numberOfSessions;

    private Logger logger = LoggerFactory.getLogger(LogReducer.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        totalSessionTime = 0;
        numberOfSessions = 0;
    }


    @Override
    public void reduce(Text key, Iterable<LogModel> logModels, Context context) {

        ArrayList<Date> listOfTimestamps = new ArrayList<>();

        String session = key.toString().split("#")[0];//get session timestamp alone separated from the key
        final String clientIp = key.toString().split("#")[1];//get the client ip from the key
        if (!uniqueURLs.containsKey(session)){
            uniqueURLs.put(session, new HashSet<String>());
        }

        for (LogModel logModel :logModels) {
            DateTime dateTime = ISODateTimeFormat.dateTime().parseDateTime(logModel
                    .getValue(LogModel.TIMESTAMP));
            listOfTimestamps.add(dateTime.toDate());

            uniqueURLs.get(session).add(logModel
                    .getValue(LogModel.RQST));//add unique urls per session

        }

        long sessionTimeInSeconds = ((Collections.max(listOfTimestamps)).getTime()-
                (Collections.min(listOfTimestamps)).getTime());
        totalSessionTime += sessionTimeInSeconds;//calculating total sessions duration.
        logger.info("session time  : {}  {}", sessionTimeInSeconds, totalSessionTime);
        numberOfSessions++;//number of sessions
        logger.info("no of sessions : {}", numberOfSessions);

        if (sessionAndIpMap.containsKey(sessionTimeInSeconds)){
            sessionAndIpMap.get(sessionTimeInSeconds)
                    .add(clientIp);//adding client ip to respective session
        }
        else sessionAndIpMap.put(sessionTimeInSeconds, new ArrayList<String>(){
            {    add(clientIp);
            }
        });

        try {
            context.write(null, null);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        logger.info("Average session time : {}", totalSessionTime/numberOfSessions);//average session time

        ArrayList<Long> sessionsList = new ArrayList<>();
        for (Long session:sessionAndIpMap.keySet()) {
            sessionsList.add(session);
        }
        Collections.sort(sessionsList, Collections.reverseOrder());
        logger.info("IP's with largest session time in descending order : ");
        for (Long session:sessionsList) {
            for (String ip:sessionAndIpMap.get(session)){
                logger.info("{}", ip);
            }
        }

    }
}
