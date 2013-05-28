package ru.develburau.mrtesting.movielens.sql.integration.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.service.HiveServer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import ru.develburau.mrtesting.movielens.sql.HiveConnector;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * User: sergey.sheypak
 * Date: 04.05.13
 * Time: 13:42
 */
public class HiveIntegrationBaseTest {

    private static final Logger LOG = LoggerFactory.getLogger(HiveIntegrationBaseTest.class);

    public static final String INTEGRATION = "integration";

    public static final String PROJECT_BUILD_DIRECTORY = System.getProperty("project.build.directory");

    private HiveServer.HiveServerHandler hiveServerHandler;

    private MiniMRCluster miniMRCluster;

    @BeforeSuite(groups = INTEGRATION)
    public void startupEnvironment() throws MetaException, IOException, SQLException, InterruptedException {
        LOG.info("Starting environment for Hive...");
        Thread.sleep(6000l);
        startupJobTracker();
        startupHive();
        LOG.info("Env is ready to use");
    }
    @AfterSuite(groups = INTEGRATION)
    public void cleanupEnvironment(){
        hiveServerHandler.shutdown();
        miniMRCluster.shutdown();
    }

    private void startupJobTracker() throws IOException {
        System.setProperty("hadoop.log.dir", PROJECT_BUILD_DIRECTORY +"/mr-output");

        Configuration configuration = new Configuration(true);
        FileSystem localFileSystem = FileSystem.get(configuration);
        miniMRCluster = new MiniMRCluster(1, localFileSystem.getUri().toString(), 1, null, null, new JobConf(configuration));
    }

    private void startupHive() throws MetaException {
        System.setProperty("hive.metastore.metadb.dir", String.format("%s/%s",PROJECT_BUILD_DIRECTORY,"metadb_dir"));
        System.setProperty("hive.metastore.local", "true");
        System.setProperty("javax.jdo.option.ConnectionURL", String.format("jdbc:derby:;databaseName=%s/metastore_db;create=true", PROJECT_BUILD_DIRECTORY));

        System.setProperty("hive.metastore.warehouse.dir", PROJECT_BUILD_DIRECTORY);
        System.setProperty("mapred.job.tracker", "localhost:"+miniMRCluster.getJobTrackerPort());

        hiveServerHandler = new HiveServer.HiveServerHandler();
    }



    protected Connection getConnection() throws SQLException {
        return HiveConnector.INSTANCE.getConnection();
    }

}
