package ru.develburau.mrtesting.movielens.sql.integration.base;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import ru.develburau.mrtesting.movielens.sql.HiveConnector;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * User: sergey.sheypak
 * Date: 04.05.13
 * Time: 14:12
 */
public class MovieLensBaseTest extends HiveIntegrationBaseTest {

    //where source zip with data should be located
    private final String pathToSourceData = System.getProperty("path.to.source.data");

    //if source data is not present, we need to download it
    private final String urlToSourceData = System.getProperty("url.to.source.data");

    private final String downloadedSourceFileName = System.getProperty("downloaded.source.file.name");
    private final String moviesFileFromZip = System.getProperty("movies.file.from.zip");
    private final String ratingsFileFromZip = System.getProperty("ratings.file.from.zip");

    private static final Logger LOG = LoggerFactory.getLogger(MovieLensBaseTest.class);

    @BeforeClass(groups = INTEGRATION)
    public void prepareMovieLensTables() throws IOException, SQLException {
        createMoviesTable();
        createRatingsTable();
        moveDataToTablesFolders();
    }

    private void moveDataToTablesFolders() throws IOException {
        File storageDir = new File(pathToSourceData);
        if(doesDirExistAndNotEmpty(storageDir)){
            LOG.info("Looks like [{}] has input file. Ready to copy it to hive storage", storageDir.getAbsolutePath());
        }else{
            LOG.info("Catalog[{}] is empty. Need to download archive:[{}]. Wait a little...", storageDir.getAbsolutePath(), urlToSourceData);
            FileUtils.copyURLToFile(new URL(urlToSourceData), new File(storageDir, downloadedSourceFileName));
        }

        File dest = new File(storageDir, downloadedSourceFileName);
        LOG.info("Trying to unzip test data from [{}] zip file", dest);
        ZipFile zipFile = new ZipFile(dest);
        unzipToCatalog(zipFile, moviesFileFromZip, String.format("%s/%s",PROJECT_BUILD_DIRECTORY, "movies"));
        unzipToCatalog(zipFile, ratingsFileFromZip, String.format("%s/%s",PROJECT_BUILD_DIRECTORY, "ratings"));
    }

    private void unzipToCatalog(ZipFile zipFile, String zipEntryName, String destPath) throws IOException {
        ZipEntry zipEntry = zipFile.getEntry(zipEntryName);
        LOG.info("Working with zipEntry [{}]", zipEntry.getName());
        InputStream inputStream = zipFile.getInputStream(zipEntry);
        File hiveStorageDir = new File(destPath);
        FileUtils.forceMkdir(hiveStorageDir);

        FileOutputStream fileOutputStream = new FileOutputStream(new File(destPath, getEntryName(zipEntry.getName())));
        IOUtils.copy(inputStream, fileOutputStream);
        fileOutputStream.close();
        inputStream.close();
    }

    private String getEntryName(String pathName){
        String[] splitted = pathName.split("/");
        return splitted[splitted.length-1];
    }


    private void createMoviesTable() throws IOException, SQLException {
        createTable("movies", "CreateMoviesTable.sql");
    }

    private void createRatingsTable() throws IOException, SQLException {
        createTable("ratings", "CreateRatingsTable.sql");
    }

    private void createTable(String tableName, String tableDDL)throws IOException, SQLException {
        LOG.info("Creating table [{}] using DDL[{}]", tableDDL, tableDDL);
        String createTableDDL = FileUtils.readFileToString(FileUtils.toFile(this.getClass().getClassLoader().getResource(tableDDL)));
        Connection connection = null;
        PreparedStatement preparedStatement;
        try{
            connection = HiveConnector.INSTANCE.getConnection();
            preparedStatement = connection.prepareStatement(String.format("DROP TABLE IF EXISTS %s",tableName));
            preparedStatement.executeUpdate();
            HiveConnector.INSTANCE.close(preparedStatement);

            preparedStatement = connection.prepareStatement(createTableDDL);
            preparedStatement.executeUpdate();
            HiveConnector.INSTANCE.close(preparedStatement);

        }finally {
            HiveConnector.INSTANCE.close(connection);
        }
    }

    private boolean doesDirExistAndNotEmpty(File storageDir){
        return storageDir.exists() && storageDir.listFiles().length > 0;
    }
}
