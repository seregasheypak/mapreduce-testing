package ru.develbureau.mrtesting.sequencefile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * User: sergey.sheypak
 * Date: 24.06.13
 * Time: 22:55
 */
public class SequenceFileSerializationTest {

    @Test(expectedExceptions = NullPointerException.class)
    public void seeThatAnyObjectsDontWorkOutOfTheBox() throws IOException{
        SequenceFile.Writer writer = createWriter(System.getProperty("project.build.directory")+"/"+"IntegerString.seq", String.class, String.class);
        writer.append(new Integer(1), new String("abc"));
        writer.append(new Integer(2), new String("def"));
        writer.append(new Integer(3), new String("ghj"));
        writer.close();
    }

    @Test
    public void seeThatWritablesWork() throws IOException{
        SequenceFile.Writer writer = createWriter(System.getProperty("project.build.directory")+"/"+"LongWritableText.seq", LongWritable.class, Text.class);
        writer.append(new LongWritable(1), new Text("abc"));
        writer.append(new LongWritable(2), new Text("def"));
        writer.append(new LongWritable(3), new Text("ghj"));
        writer.close();

    }

    private SequenceFile.Writer createWriter(String pathToFile, Class<?> keyClass, Class<?> valueClass) throws IOException {
        return SequenceFile.createWriter(new Configuration(),
                SequenceFile.Writer.file(new Path(pathToFile)),
                SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE),
                SequenceFile.Writer.keyClass(keyClass),
                SequenceFile.Writer.valueClass(valueClass)
        );
    }
}
