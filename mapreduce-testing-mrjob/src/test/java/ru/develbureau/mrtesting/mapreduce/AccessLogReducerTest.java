package ru.develbureau.mrtesting.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.testng.annotations.Test;
import ru.develbureau.mrtesting.model.LoggedRequest;

import java.util.Arrays;

/**
 * User: sergey.sheypak
 * Date: 23.03.13
 * Time: 17:06
 */
public class AccessLogReducerTest {

    private final AccessLogReducer reducer = new AccessLogReducer();
    private final ReduceDriver<LoggedRequest, IntWritable, NullWritable, Text> driver = new ReduceDriver<LoggedRequest, IntWritable, NullWritable, Text>();


    @Test
    public void runReducer(){
        LoggedRequest loggedRequest = new LoggedRequest(123l, "/someResource/", 200);
        driver.setReducer(reducer);
        driver.withInput(loggedRequest, Arrays.asList(new IntWritable[]{new IntWritable(1),new IntWritable(2),new IntWritable(3)}))
              .withOutput(NullWritable.get(), reducer.prettyPrint(loggedRequest, 6))
              .runTest();
    }
}
