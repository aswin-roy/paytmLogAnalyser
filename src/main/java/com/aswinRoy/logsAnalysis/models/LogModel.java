package com.aswinRoy.logsAnalysis.models;

import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 *
 */
public class LogModel implements Writable {

    protected HashMap<String, String> dataMap;
    public static Logger logger = LoggerFactory.getLogger(LogModel.class);

    public static final String TIMESTAMP = "ts";
    public static final String ELB = "elb";
    public static final String CL_PORT = "client:port";
    public static final String BE_PORT = "backend:port";
    public static final String R_PT = "request_processing_time";
    public static final String B_PT = "backend_processing_time";
    public static final String RS_PT = "response_processing_time";
    public static final String ESC = "elb_status_code";
    public static final String BSC = "backend_status_code";
    public static final String RECEIVED_BYTES = "received_bytes";
    public static final String SEND_BYTES = "sent_bytes";
    public static final String RQST = "request";
    public static final String USR_AGENT = "user_agent";
    public static final String SSL_C = "ssl_cipher";
    public static final String SSL_P = "ssl_protocol";


    public LogModel(){
        dataMap = new HashMap<>();
        dataMap.put(TIMESTAMP, "");
        dataMap.put(ELB, "");
        dataMap.put(CL_PORT, "");
        dataMap.put(BE_PORT, "");
        dataMap.put(R_PT, "");
        dataMap.put(B_PT, "");
        dataMap.put(RS_PT, "");
        dataMap.put(ESC, "");
        dataMap.put(BSC, "");
        dataMap.put(RECEIVED_BYTES, "");
        dataMap.put(SEND_BYTES, "");
        dataMap.put(RQST, "");
        dataMap.put(USR_AGENT, "");
        dataMap.put(SSL_C, "");
        dataMap.put(SSL_P, "");
    }

    @Override
    public void write(DataOutput out) throws IOException {
        writeObjectToStream(out);
        logger.info("In write");
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        readFieldsFromStream(in);
        logger.info("In read");
    }

    private void writeObjectToStream(DataOutput out) throws IOException {

        Iterator<String> iterator = dataMap.keySet().iterator();

        while (iterator.hasNext()){
            String qualifier = iterator.next();
            logger.info("emitting value : {}",getValue(qualifier));
            out.writeUTF(getValue(qualifier));
        }
    }

    private void readFieldsFromStream(DataInput in) throws IOException {

        Iterator<String> iterator = dataMap.keySet().iterator();

        while (iterator.hasNext()){
            String qualifier = iterator.next();
            this.setValue(qualifier, in.readUTF());
        }
    }

    public void parse(String eachLog) {
        StringTokenizer stringTokenizer = new StringTokenizer(eachLog);
        dataMap.put(TIMESTAMP, stringTokenizer.nextToken());
        logger.info("set ts to {}", dataMap.get(TIMESTAMP));
        dataMap.put(ELB, stringTokenizer.nextToken());
        dataMap.put(CL_PORT, stringTokenizer.nextToken());
        dataMap.put(BE_PORT, stringTokenizer.nextToken());
        dataMap.put(R_PT,stringTokenizer.nextToken());
        dataMap.put(B_PT, stringTokenizer.nextToken());
        dataMap.put(RS_PT, stringTokenizer.nextToken());
        dataMap.put(ESC, stringTokenizer.nextToken());
        dataMap.put(BSC, stringTokenizer.nextToken());
        dataMap.put(RECEIVED_BYTES, stringTokenizer.nextToken());
        dataMap.put(SEND_BYTES, stringTokenizer.nextToken());
        stringTokenizer.nextToken("\"");
        dataMap.put(RQST, stringTokenizer.nextToken("\""));
        stringTokenizer.nextToken("\"");
        dataMap.put(USR_AGENT, stringTokenizer.nextToken("\""));
        stringTokenizer.nextToken(" ");
        dataMap.put(SSL_C, stringTokenizer.nextToken());
        dataMap.put(SSL_P, stringTokenizer.nextToken());

    }

    public HashMap<String, String> getDataMap() {
        return this.dataMap;
    }

    public void setValue(String qualifier, String value) {
        logger.info("setting value: {}", value);
        dataMap.put(qualifier, value);
    }

    public String getValue(String qualifier) {
        return dataMap.get(qualifier);
    }

}