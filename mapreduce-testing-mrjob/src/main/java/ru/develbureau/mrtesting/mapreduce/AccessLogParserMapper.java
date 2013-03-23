package ru.develbureau.mrtesting.mapreduce;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import ru.develbureau.mrtesting.model.Counter;
import ru.develbureau.mrtesting.model.LoggedRequest;
import ru.develbureau.mrtesting.parser.ApacheLogParser;
import ru.develbureau.mrtesting.util.Util;

/**
 * User: sergey.sheypak
 * Date: 23.03.13
 * Time: 16:01
 */
public class AccessLogParserMapper extends Mapper<LongWritable, Text, LoggedRequest, LongWritable>{

    private static final Log LOG = LogFactory.getLog(AccessLogParserMapper.class);

    private final ApacheLogParser parser = new ApacheLogParser();
    private final LongWritable one = new LongWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context){
        try{
            LoggedRequest loggedRequest = parser.parse(value.toString());
            if(loggedRequest.getReplyCode().equals(500)){
                context.getCounter(Counter.SERVER_ERROR_500).increment(1);
            }else{
                loggedRequest.setTimestamp(Util.trimMinutes(loggedRequest.getTimestamp()));
                context.write(loggedRequest,one);
            }
        }
        catch (Exception e){
            LOG.error("Error while parsing line", e);
            context.getCounter(Counter.CORRUPTED_RECORD).increment(1);
        }

    }
}
