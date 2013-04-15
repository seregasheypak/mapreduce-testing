package ru.develbureau.mrtesting.mapreduce.minimrcluster;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.testng.annotations.AfterGroups;
import org.testng.annotations.BeforeGroups;

import java.io.File;
import java.io.IOException;

/**
 * User: sergey.sheypak
 * Date: 24.03.13
 * Time: 0:57
 */
public abstract class AbstractMapReduceJobTest {

    private static final Log LOG = LogFactory.getLog(AbstractMapReduceJobTest.class);

    public static final String INTEGRATION = "integration";
    public static final String DEFAULT_LOG_CATALOG = "local-mr-logs";
    public static final String MR_DATA_OUTPUT = "mr-data-output";
    public static final String DEFAULT_OUTPUT_FILE_NAME =  "part-r-00000";

    private static final String HADOOP_LOG_DIR = "hadoop.log.dir";

    //Dirty hack. share conf instance among all subclasses
    private static final StaticHolder<MiniMRCluster> MR_CLUSTER_HOLDER = new StaticHolder<MiniMRCluster>();
    private static final StaticHolder<JobConf> MR_CLUSTER_CONF_HOLDER = new StaticHolder<JobConf>();
    private static final int DEFAULT_MAP_MAX_ATTEMPTS = 0;
    private static final int DEFAULT_MAX_REDUCE_ATTEMPTS = 0;
    private static final String PROJECT_BUILD_DIRECTORY = "project.build.directory";

    private static final int NUM_TASK_TRACKERS = 1;
    private static final int NUM_DIR = 1;


    /** Dummy reference holder. */
    private static class StaticHolder<T>{
        private T instance;

        void set(T instance){
            this.instance = instance;
        }

        T get(){
            return this.instance;
        }

    }

    @BeforeGroups(groups = INTEGRATION)
    public final void initMRCluster() throws IOException {
        LOG.info("#initMRCluster -> starting to init MiniMRCluster...");
        System.setProperty(HADOOP_LOG_DIR, getPathToLogCatalog());
        cleanupOutputPath();
        Configuration configuration = new Configuration(true);
        FileSystem localFileSystem = FileSystem.get(configuration);
        MR_CLUSTER_HOLDER.set(new MiniMRCluster(NUM_TASK_TRACKERS, localFileSystem.getUri().toString(), NUM_DIR, null, null, new JobConf(configuration)));
        MR_CLUSTER_CONF_HOLDER.set(MR_CLUSTER_HOLDER.get().createJobConf());
        LOG.info(String.format("#initMRCluster -> Conf[%s] MRCluster has been initialized for test instance[%s]",
                                MR_CLUSTER_CONF_HOLDER, this.getClass().getSimpleName()));
    }

    @AfterGroups(groups = INTEGRATION)
    public final void shutdownMRCluster() {
        LOG.info("#shutdownMRCluster -> starting to shutdown MiniMRCluster...");
        if (MR_CLUSTER_HOLDER.get() != null) {
            Runnable clusterStopper = new Runnable() {
                @Override
                public void run() {
                    MR_CLUSTER_HOLDER.get().shutdown();
                    MR_CLUSTER_HOLDER.set(null);
                }
            };
            new Thread(clusterStopper).start();
        }
        LOG.info(String.format("#shutdownMRCluster -> MRCluster is shut down for test instance[%s]",
                this.getClass().getSimpleName()));
    }

    public Configuration getConfigurationForTest(){
        return MR_CLUSTER_CONF_HOLDER.get();
    }

    /**
     * Pass ready to use job here. Input, output paths will be set here automatically
     * */
    public boolean submitAndWaitForCompletion(Job job) throws InterruptedException, IOException, ClassNotFoundException{
        String pathToInputFile = getPathToInputData();
        checkThatFileExists(pathToInputFile);

        FileInputFormat.setInputPaths(job, pathToInputFile);
        LOG.info(String.format("Path to input[%s]", pathToInputFile));

        FileOutputFormat.setOutputPath(job, createPath(getOutputPath()));
        LOG.info(String.format("Path to output[%s]", getOutputPath()));

        job.getConfiguration().set("mapreduce.map.maxattempts", "0");
        job.getConfiguration().set("mapreduce.reduce.maxattempts", "0");
        job.getConfiguration().set("mapreduce.job.maxtaskfailures.per.tracker", "1");
        LOG.info("Submitting job...");
        job.submit();
        LOG.info("Job has been submitted.");

        String trackingUrl = job.getTrackingURL();
        String jobId = job.getJobID().toString();
        LOG.info("trackingUrl:" +trackingUrl);
        LOG.info("jobId:" +jobId);

        return job.waitForCompletion(true);
    }

    protected int getMapreduceMapMaxAttempts(){
        return DEFAULT_MAP_MAX_ATTEMPTS;
    }

    protected int getMapreduceReduceMaxAttempts(){
        return DEFAULT_MAX_REDUCE_ATTEMPTS;
    }

    /**
     * @return path reducer output
     * default is: @{link AbstractClusterMapReduceTestOld#DEFAULT_OUTPUT_CATALOG}
     * */
    protected String getOutputPath(){
        return  String.format("%s/%s/%s", getPathToRootOutputDirectory(),this.getClass().getSimpleName(), MR_DATA_OUTPUT);
    }

    /**
     * Always provide path to log catalog.
     * */
    protected String getPathToLogCatalog(){
        File logCatalog = new File(String.format("%s/%s", getPathToRootOutputDirectory(), DEFAULT_LOG_CATALOG));
        if(!logCatalog.exists()){
            logCatalog.mkdir();
        }
        LOG.info(String.format("Path to log catalog is: %s", logCatalog.getAbsolutePath()));
        return logCatalog.getAbsolutePath();
    }

    protected String getPathToRootOutputDirectory(){
        return System.getProperty(PROJECT_BUILD_DIRECTORY);
    }

    /**
     * Output path should be empty
     *
     * */
    protected void cleanupOutputPath(){
        File outputCatalog = new File(getOutputPath());
        if(outputCatalog.exists()){
            outputCatalog.delete();
        }
    }

    /**
     * Creates @{link Path} using absolute path to some FS resource
     * @return new Path instance.
     * */
    protected Path createPath(String pathToFSResource){
        return new Path(pathToFSResource);
    }

    public void checkThatFileExists(String absolutePathToFile){
        if(! new File(absolutePathToFile).exists()){
            throw new UnsupportedOperationException(String.format("Path to input file is incorrect. Can't run MR job. Incorrect path is: %s",
                                                                  absolutePathToFile));
        }
    }

    public abstract String getPathToInputData();

}
