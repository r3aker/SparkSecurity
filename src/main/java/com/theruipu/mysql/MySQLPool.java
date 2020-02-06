package com.theruipu.mysql;

import java.io.IOException;
import java.util.Properties;

import javax.sql.DataSource;

import com.alibaba.druid.pool.DruidDataSource;
import com.theruipu.utils.Getconfig;

public class MySQLPool {

	private static MySQLPool instance;

	private static String jdbcUrl;
	private static String username;
	private static String password;
	private static String driverClassName;
	private static int initialSize;
	private static int maxActive;
	private static int minIdle;
	private static int maxWait;
	// private static boolean poolPreparedStatements;
	// private static int maxOpenPreparedStatements;
	private static boolean testWhileIdle;

	private static String validationQuery;
	private static boolean removeAbandoned;
	private static int removeAbandonedTimeout;
	private static boolean logAbandoned;

	private DruidDataSource dataSource;
	
	private static 		String Environment = Getconfig.Getproperty("env");


	static {
		Properties prop = new Properties();

		try {
			prop.load(MySQLPool.class.getClassLoader().getResourceAsStream("Mysql.properties"));
			
			
	
			
			jdbcUrl =prop.getProperty("druid.jdbcUrl");
			username = prop.getProperty("druid.username");
			password =prop.getProperty("druid.password");
			
			driverClassName = prop.getProperty("druid.driverClassName");
			initialSize = Integer.parseInt(prop.getProperty("druid.initialSize"));
			maxActive = Integer.parseInt(prop.getProperty("druid.maxActive"));
			minIdle = Integer.parseInt(prop.getProperty("druid.minIdle"));
			maxWait = Integer.parseInt(prop.getProperty("druid.maxWait"));
			//poolPreparedStatements = Boolean.parseBoolean(prop.getProperty("druid.poolPreparedStatements"));
			//maxOpenPreparedStatements = Integer.parseInt(prop.getProperty("druid.maxOpenPreparedStatements"));
			testWhileIdle = Boolean.parseBoolean(prop.getProperty("druid.testWhileIdle"));

			validationQuery = prop.getProperty("druid.validationQuery");
			removeAbandoned = Boolean.parseBoolean(prop.getProperty("druid.removeAbandoned"));
			removeAbandonedTimeout = Integer.parseInt(prop.getProperty("druid.removeAbandonedTimeout"));
			logAbandoned = Boolean.parseBoolean(prop.getProperty("druid.logAbandoned"));
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}

	private MySQLPool() {
	}

	private void init() {
		dataSource = new DruidDataSource();
		dataSource.setUrl(jdbcUrl);
		dataSource.setUsername(username);
		dataSource.setPassword(password);
		dataSource.setDriverClassName(driverClassName);
		dataSource.setInitialSize(initialSize);
		dataSource.setMaxActive(maxActive);
		dataSource.setMinIdle(minIdle);
		dataSource.setMaxWait(maxWait);
		// dataSource.setPoolPreparedStatements(poolPreparedStatements);
		// dataSource.setMaxOpenPreparedStatements(maxOpenPreparedStatements);
		dataSource.setTestWhileIdle(testWhileIdle);

		// 连接池自动回收
		dataSource.setValidationQuery(validationQuery);
		dataSource.setRemoveAbandoned(removeAbandoned);
		dataSource.setRemoveAbandonedTimeoutMillis(removeAbandonedTimeout);
		dataSource.setLogAbandoned(logAbandoned);
	}

	public static MySQLPool getInstance() {
		if (instance == null) {
			synchronized (MySQLPool.class) {
				if (instance == null) {
					instance = new MySQLPool();
				}
			}
		}
		return instance;
	}

	public DataSource getDataSource() {
		if (null == dataSource) {
			init();
		}
		return dataSource;
	}

	public void colse() {
		if (null != dataSource) {
			dataSource.close();
		}
	}
	/*
	 * public void closeConnection(Connection conn) { try { conn.close(); } catch
	 * (SQLException e) { e.printStackTrace(); } }
	 * 
	 * public Connection getConnection() { try { return
	 * getDataSource().getConnection(); } catch (SQLException e) {
	 * e.printStackTrace(); } return null; }
	 */

}
