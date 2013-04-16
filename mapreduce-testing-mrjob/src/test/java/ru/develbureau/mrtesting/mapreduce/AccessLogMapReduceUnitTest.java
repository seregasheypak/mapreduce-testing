package ru.develbureau.mrtesting.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.testng.annotations.Test;
import ru.develbureau.mrtesting.model.LoggedRequest;
import ru.develbureau.mrtesting.parser.ApacheLogParser;
import ru.develbureau.mrtesting.parser.ParserException;

/**
 * User: sergey.sheypak
 * Date: 17.04.13
 * Time: 0:08
 */
public class AccessLogMapReduceUnitTest {
    private final ApacheLogParser parser = new ApacheLogParser();

    @Test
    public void runMapReduceWithCombiner() throws ParserException {
        MapReduceDriver<LongWritable, Text, LoggedRequest, LongWritable, NullWritable, Text>  mapReduceDriver = new MapReduceDriver();
        mapReduceDriver.setMapper(new AccessLogParserMapper());
        mapReduceDriver.setCombiner(new AccessLogCombiner());
        AccessLogReducer accessLogReducer = new AccessLogReducer();
        mapReduceDriver.setReducer(accessLogReducer);

        mapReduceDriver.withInput(new LongWritable(), new Text("205.189.154.54 - - [01/Jul/1995:00:00:29 -0400] \"GET /shuttle/countdown/count.gif HTTP/1.0\" 200 40310"))
                       .withInput(new LongWritable(), new Text("205.189.155.66 - - [01/Jul/1995:00:01:15 -0400] \"GET /shuttle/countdown/count.gif HTTP/1.0\" 200 40310"))

                       //.withInput(new LongWritable(), new Text("210.189.154.54 - - [05/Jun/1995:02:01:15 -0400] \"GET /moon/landing HTTP/1.0\" 200 40310"))
                       //.withInput(new LongWritable(), new Text("212.189.154.54 - - [05/Jun/1995:06:01:15 -0400] \"GET /moon/landing HTTP/1.0\" 200 40310"))

                       .withOutput(NullWritable.get(), accessLogReducer.prettyPrint(new LoggedRequest(parser.parseTimestamp("05/Jun/1995:02:01:15 -0400"), "/shuttle/countdown/count.gif", 200), 2))
                       //.withOutput(NullWritable.get(), accessLogReducer.prettyPrint(new LoggedRequest(parser.parseTimestamp("05/Jun/1995:02:01:15 -0400"), "/moon/landing", 200), 2))
                       .runTest();
    }
}
