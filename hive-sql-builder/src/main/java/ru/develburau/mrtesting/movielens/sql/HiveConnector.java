package ru.develburau.mrtesting.movielens.sql;

import org.apache.commons.dbutils.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/** Naive singleton. I don't know for sure how does hive handles multiple threads. */
public enum HiveConnector {

	INSTANCE;
	
    private final Logger LOG = LoggerFactory.getLogger(HiveConnector.class);

	private HiveConnector(){
        String jdbcDriverClassName = System.getProperty("url.rating.hive.jdbc.driver");
		try {
            LOG.debug("Trying to load Hive JDBC driver: {}", jdbcDriverClassName);
			Class.forName(jdbcDriverClassName);
		} catch (ClassNotFoundException e) {
            LOG.error("Driver class is not found", e);
			throw new RuntimeException(String.format("Can't find JDBC driver class [%s]", System.getProperty("url.rating.hive.jdbc.driver")), e);
		}
	}

	public Connection getConnection() throws SQLException{
        String preparedJdbcUrl = System.getProperty("url.rating.hive.jdbc.remote.url");
        String user = System.getProperty("url.rating.hive.jdbc.user");
        LOG.debug("Establishing connection with Hive using JDBC URL[{}] and user[{}]", preparedJdbcUrl, user);
		return DriverManager.getConnection(preparedJdbcUrl, user, "");
	}

	public void close(ResultSet rs, Statement stmt, Connection conn){
		close(rs);
		close(stmt);
		close(conn);
	}

	public void close(Connection conn){
        DbUtils.closeQuietly(conn);
	}

	public void close(ResultSet rs){
        DbUtils.closeQuietly(rs);
    }

	public void close(Statement stmt){
        DbUtils.closeQuietly(stmt);
    }
}
