package ru.develbureau.mrtesting.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import ru.develbureau.mrtesting.model.LoggedRequest;

import java.io.IOException;

/**
 * User: sergey.sheypak
 * Date: 23.03.13
 * Time: 16:59
 */
public class AccessLogReducer extends Reducer<LoggedRequest, IntWritable, NullWritable, Text>{

    private final Text outputText = new Text();

    @Override
    public void reduce(LoggedRequest loggedRequest, Iterable<IntWritable> hits, Context context) throws IOException, InterruptedException {
        long totalCount = 0;
        for(IntWritable counter : hits){
            totalCount+=counter.get();
        }
        context.write(NullWritable.get(), prettyPrint(loggedRequest, totalCount));
    }

    public Text prettyPrint(LoggedRequest loggedRequest, long totalCount){
        outputText.set(loggedRequest.getTimestamp() +"\t" + loggedRequest.getRequest() + "\t" + loggedRequest.getReplyCode()+ "\t" + totalCount);
        return outputText;
    }
}
