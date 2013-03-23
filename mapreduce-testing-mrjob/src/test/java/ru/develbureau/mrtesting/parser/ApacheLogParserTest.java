package ru.develbureau.mrtesting.parser;

import org.testng.annotations.Test;
import ru.develbureau.mrtesting.model.LoggedRequest;

import java.util.Calendar;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.equalTo;

/**
 * User: sergey.sheypak
 * Date: 23.03.13
 * Time: 15:32
 */
public class ApacheLogParserTest {

    private static final int JULY_MONTH_NUMBER = 6;
    private final ApacheLogParser parser = new ApacheLogParser();

    @Test
    public void parseCorrectLogLineWithReturnCode200() throws ParserException {
        String inputLine = "news.ti.com - - [28/Jul/1995:12:10:31 -0400] \"GET /shuttle/missions/sts-69/sts-69-patch-small.gif HTTP/1.0\" 200 8083";
        LoggedRequest loggedRequest = parser.parse(inputLine);

        assertThat(loggedRequest.getRequest(), equalTo("/shuttle/missions/sts-69/sts-69-patch-small.gif"));
        assertThat(loggedRequest.getReplyCode(),equalTo(200));
    }

    @Test(expectedExceptions = ParserException.class)
    public void parseBadRecord()throws ParserException {
        String inputLine = "news.ti.com - - [28/Jul/1995:12:10:31 -0400] \"GET /shuttle/missions/sts-69/sts-69-patch-small.gif HTTP/1.0\" ";
        parser.parse(inputLine);
    }

    @Test
    public void parseTimestamp() throws ParserException{
        long timestamp = parser.parseTimestamp("28/Jul/1995:12:10:31");
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(timestamp);

        assertThat(calendar.get(Calendar.YEAR), equalTo(1995));
        assertThat(calendar.get(Calendar.MONTH), equalTo(JULY_MONTH_NUMBER));
        assertThat(calendar.get(Calendar.DAY_OF_MONTH), equalTo(28));
        assertThat(calendar.get(Calendar.HOUR_OF_DAY), equalTo(12));
        assertThat(calendar.get(Calendar.MINUTE), equalTo(10));
        assertThat(calendar.get(Calendar.SECOND), equalTo(31));
    }

    @Test(expectedExceptions = ParserException.class)
    public void parseBadDate() throws ParserException{
        parser.parseTimestamp("28/Ju/1995:12:10:31");
    }
}

