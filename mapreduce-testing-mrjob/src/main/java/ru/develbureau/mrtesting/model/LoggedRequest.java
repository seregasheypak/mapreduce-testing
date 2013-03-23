package ru.develbureau.mrtesting.model;

import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * User: sergey.sheypak
 * Date: 23.03.13
 * Time: 13:25
 * http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html
 * - host making the request. A hostname when possible, otherwise the Internet address if the name could not be looked up.
 * - timestamp in the format "DAY MON DD HH:MM:SS YYYY",
 *      where DAY is the day of the week, MON is the name of the month, DD is the day of the month,
 *      HH:MM:SS is the time of day using a 24-hour clock, and YYYY is the year.
 *      The timezone is -0400.
 * - request given in quotes.
 * - HTTP reply code.
 */
public class LoggedRequest implements WritableComparable<LoggedRequest>{

    private final LongWritable timestamp = new LongWritable();
    private final Text request = new Text();
    private final IntWritable replyCode = new IntWritable();

    public LoggedRequest() {
    }

    public LoggedRequest(Long timestamp, String request, Integer replyCode){
        this.timestamp.set(timestamp);
        this.request.set(request);
        this.replyCode.set(replyCode);
    }


    public Long getTimestamp() {
        return timestamp.get();
    }

    public String getRequest() {
        return request.toString();
    }

    public Integer getReplyCode() {
        return replyCode.get();
    }

    @Override
    public int hashCode(){
        return getTimestamp().hashCode();
    }

    public boolean equals(Object obj){
        if(this == obj){
            return true;
        }
        if(obj instanceof LoggedRequest){
            LoggedRequest other = (LoggedRequest)obj;
            return new EqualsBuilder().append(this.getTimestamp(), other.getTimestamp())
                                      .append(this.getRequest(), other.getRequest())
                                      .append(this.getReplyCode(), other.getReplyCode())
                                      .build();
        }

        return false;
    }

    @Override
    public int compareTo(LoggedRequest other) {
        return new CompareToBuilder().append(this.getTimestamp(), other.getTimestamp())
                                     .append(this.getReplyCode(), other.getReplyCode())
                                     .append(this.getRequest(), other.getRequest())
                                     .build();
    }

    @Override
    public void write(DataOutput out) throws IOException {
       timestamp.write(out);
       request.write(out);
       replyCode.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        timestamp.readFields(in);
        request.readFields(in);
        replyCode.readFields(in);
    }
}
