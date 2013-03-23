package ru.develbureau.mrtesting.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import ru.develbureau.mrtesting.model.LoggedRequest;

import java.io.IOException;

/**
 * User: sergey.sheypak
 * Date: 23.03.13
 * Time: 17:12
 */
public class AccessLogCombiner extends Reducer<LoggedRequest, LongWritable, LoggedRequest, LongWritable> {

    private final LongWritable sum = new LongWritable();

    @Override
    public void reduce(LoggedRequest loggedRequest, Iterable<LongWritable> hits, Context context) throws IOException, InterruptedException {
        long totalCount = 0;
        for(LongWritable counter : hits){
            totalCount+=counter.get();
        }
        sum.set(totalCount);
        context.write(loggedRequest, sum);
    }

}

