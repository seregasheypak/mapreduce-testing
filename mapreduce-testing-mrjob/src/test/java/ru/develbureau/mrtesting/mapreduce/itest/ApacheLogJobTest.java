package ru.develbureau.mrtesting.mapreduce.itest;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import ru.develbureau.mrtesting.mapreduce.AccessLogCombiner;
import ru.develbureau.mrtesting.mapreduce.AccessLogParserMapper;
import ru.develbureau.mrtesting.mapreduce.AccessLogReducer;
import ru.develbureau.mrtesting.mapreduce.minimrcluster.AbstractMapReduceJobTest;
import ru.develbureau.mrtesting.model.LoggedRequest;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * User: sergey.sheypak
 * Date: 24.03.13
 * Time: 1:11
 */
public class ApacheLogJobTest extends AbstractMapReduceJobTest {

    private static final Logger LOG = LoggerFactory.getLogger(ApacheLogJobTest.class);

    /** A final name for downloaded input dataset*/
    private static final String INPUT_GZ = "input.gz";

    /**A local catalog for data you want to feed to your mapreduce program*/
    private final String baseDirForTestSourceData = "itest.source.directory";

    /** A remote data you download once to run itest*/
    private final String downloadUrl = "ApacheLogJobTest.downloadUrl";

    /** A temp input path for your mapreduce program */
    private final String pathToDirWithThisTestSourceData =   System.getProperty(baseDirForTestSourceData) +"/"+this.getClass().getSimpleName();

    /**
     * Downloads input dataset and prepares for submission to mr
     * */
    @BeforeMethod(groups = INTEGRATION)
    public void downloadTest() throws IOException {
        File storageDir = new File(pathToDirWithThisTestSourceData);
        if(doesDirExistAndNotEmpty(storageDir)){
            LOG.info("Looks like [{}] has input file. Ready to copy it to mr input dir", storageDir.getAbsolutePath());
        }else{
            LOG.info("Catalog[{}] is empty. Need to download archive:[{}]. Wait a little...", storageDir.getAbsolutePath(), System.getProperty(downloadUrl));
            FileUtils.copyURLToFile(new URL(System.getProperty(downloadUrl)), new File(storageDir, INPUT_GZ));
        }
        File dest = new File(storageDir, INPUT_GZ);
        LOG.info("Copying source file [{}] to mapreduce input folder [{}]", dest.getAbsolutePath(), getPathToInputData());
        FileUtils.copyFileToDirectory(dest, new File(getPathToInputData()));
    }

    /**
     * Configure input dataset and run job submitting it to locally launched JobTracker
     * */
    @Test(groups = INTEGRATION)
    public void runJob() throws IOException, ClassNotFoundException, InterruptedException {
        Job job = new Job(getConfigurationForTest());

        job.setMapperClass(AccessLogParserMapper.class);
        job.setCombinerClass(AccessLogCombiner.class);
        job.setReducerClass(AccessLogReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(LoggedRequest.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        boolean result = submitAndWaitForCompletion(job);
        assertThat(result, equalTo(true));

    }

    @Override
    public String getPathToInputData() {
        return  getPathToRootOutputDirectory()+"/"+this.getClass().getSimpleName()+"/input";
    }

    private boolean doesDirExistAndNotEmpty(File storageDir){
        return storageDir.exists() && storageDir.listFiles().length > 0;
    }
}
