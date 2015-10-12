package com.aswinRoy.logsAnalysis.utils;

import org.apache.commons.lang.time.DateUtils;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.format.ISODateTimeFormat;

import java.text.ParseException;
import java.util.*;

/**
 *
 */
public class Utils {

    public Date getRoundedTs(String ts){
        DateTime dt = ISODateTimeFormat.dateTime().parseDateTime(ts);
        Date roundedTs = DateUtils.round(dt.toDate(), Calendar.HOUR);

        return roundedTs;
    }
}
