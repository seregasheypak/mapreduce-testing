package ru.develbureau.mrtesting.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.testng.annotations.Test;
import ru.develbureau.mrtesting.model.LoggedRequest;

import java.util.Arrays;

/**
 * User: sergey.sheypak
 * Date: 23.03.13
 * Time: 17:22
 */
public class AccessLogCombinerTest {


    private final AccessLogCombiner combiner = new AccessLogCombiner();
    private final ReduceDriver<LoggedRequest, LongWritable, LoggedRequest, LongWritable> driver = new ReduceDriver<LoggedRequest, LongWritable, LoggedRequest, LongWritable>();


    @Test
    public void runCombiner(){
        LoggedRequest loggedRequest = new LoggedRequest(123l, "/someResource/", 200);
        driver.setReducer(combiner);
        driver.withInput(loggedRequest, Arrays.asList(new LongWritable(1), new LongWritable(2), new LongWritable(3)))
                .withOutput(loggedRequest, new LongWritable(1+2+3))
                .runTest();
    }
}
