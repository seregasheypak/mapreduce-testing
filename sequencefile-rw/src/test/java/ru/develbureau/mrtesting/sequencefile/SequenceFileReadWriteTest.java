package ru.develbureau.mrtesting.sequencefile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.ReflectionUtils;
import static org.hamcrest.Matchers.*;

import static org.hamcrest.MatcherAssert.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;



import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * User: sergey.sheypak
 * Date: 28.05.13
 * Time: 19:11
 */
public class SequenceFileReadWriteTest {

    private static final Logger LOG = LoggerFactory.getLogger(SequenceFileReadWriteTest.class);

    private static final String[] sampleLines = {"this is the first line",
                                                 "this is the second line",
                                                 "this is the third line"};

    @Test
    public void writeAndVerfiySequenceFileWithGzipCompression() throws IOException {
        writeSampleLinesToFile();
        String[] linesFromSequenceFile = readLinesFromSequenceFile();

        assertThat(linesFromSequenceFile, equalTo(sampleLines));
    }

    private void writeSampleLinesToFile() throws IOException{
        SequenceFile.Writer writer = createWriterWithCompression();

        LongWritable key = new LongWritable();
        Text value =  new Text();
        for(int seqFileKey = 0; seqFileKey<sampleLines.length; seqFileKey++){
            key.set(seqFileKey);
            value.set(sampleLines[seqFileKey]);
            writer.append(key, value);
        }
        writer.close();

    }

    private String[] readLinesFromSequenceFile() throws IOException{
        LongWritable key = new LongWritable();
        Text value = new Text();
        List<String> values = new LinkedList<String>();
        SequenceFile.Reader reader = createSequenceFileReader();
        while(reader.next(key, value)){
            values.add(value.toString());
        }
        reader.close();
        return values.toArray(new String[values.size()]);

    }

    private SequenceFile.Reader createSequenceFileReader() throws IOException {
        return new SequenceFile.Reader(createLocalConfiguration(), SequenceFile.Reader.file(getPathToTestFile()));
    }

    private SequenceFile.Writer createWriterWithCompression() throws IOException{
        SequenceFile.Writer writer = SequenceFile.createWriter(createLocalConfiguration(),
                                                    SequenceFile.Writer.file(getPathToTestFile()),
                                                    SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK),
                                                    SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK, createGzipCompressionCodec()),
                                                    SequenceFile.Writer.keyClass(LongWritable.class),
                                                    SequenceFile.Writer.valueClass(Text.class)
                                                  );
        return writer;
    }

    private Configuration createLocalConfiguration(){
        return new Configuration();
    }

    private Path getPathToTestFile(){
        LOG.info("Getting path to test file:[{}]", System.getProperty("project.build.directory/testSequenceFile.gz") );
        return new Path(System.getProperty("project.build.directory")+"/testSequenceFile.gz");
    }

    private CompressionCodec createGzipCompressionCodec(){
        return ReflectionUtils.newInstance(GzipCodec.class, createLocalConfiguration());
    }
}
