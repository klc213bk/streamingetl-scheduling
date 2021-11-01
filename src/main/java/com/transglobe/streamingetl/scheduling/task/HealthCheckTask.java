package com.transglobe.streamingetl.scheduling.task;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Calendar;
//import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class HealthCheckTask {
	private static final Logger LOG = LoggerFactory.getLogger(HealthCheckTask.class);

	private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private static final String LOGMINER_HEARTBEAT_TABLE = "LOGMINER_HEARTBEAT";
	private static final String STREAMING_HEALTH_TABLE = "STREAMING_HEALTH";

//	private static final String ZOOKEEPER_SERVER = "Zookeeper";
//	private static final String KAFKA_SERVER = "Kafka";
//	private static final String LOGMINER_SERVER = "Logminer";
//	private static final String HEARTBEAT_CONSUMER_SERVER = "heartbeatConsumer";
//
//	private static final String SERVER_STATUS_RUNNING = "RUNNING";

	@Value("${logminer.db.driver}")
	private String logminerDbDriver;

	@Value("${logminer.db.url}")
	private String logminerDbUrl;

	@Value("${logminer.db.username}")
	private String logminerDbUsername;

	@Value("${logminer.db.password}")
	private String logminerDbPassword;

	@Value("${connect.rest.port}")
	private String connectRestPort;

	@Value("${connect.connector.name}")
	private String connectConnectorName;

	@Value("${connect.connector.config.file}")
	private String connectConnectorConfigFile;

	@Value("${server.port}")
	private String serverPort;

	private boolean initialLogminer = true;

	int i0 = 0;
	int i1 = 0;

	public static void main(String[] args) {

		HealthCheckTask app = new HealthCheckTask();
		try {
			app.getReConfigStr();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	private Properties loadProperty() {
		LOG.info(">>>>  loadProperty, connectConnectorConfigFile={}", connectConnectorConfigFile);
		Properties props = null;

		try {
			props = new Properties();
			FileInputStream fis = new FileInputStream(connectConnectorConfigFile);
			props.load(fis);
		} 
		catch (Exception e){
			e.printStackTrace();
		}
		return props;
	}
	private String getReConfigStr() throws JsonProcessingException {
		Properties prop = loadProperty();
		Map<String, Object> map = new HashMap<>();
		Map<String, String> configmap = new HashMap<>();
		for (Entry<Object, Object> e : prop.entrySet()) {
			String key = (String)e.getKey();
			String value =  (String)e.getValue();
			if ("name".equals(key)) {
				map.put(key, value);
			} else {
				if ("reset.offset".equals(key)) {
					value = "false";
					//value = "true";
				}
				if ("start.scn".equals(key)) {
					value = "";
				}
				configmap.put(key, value);
				//				LOG.info(">>>>>>>>>>>> entry key={}, value={}", (String)e.getKey(), (String)e.getValue());
			}
		}
		map.put("config", configmap);

		ObjectMapper objectMapper = new ObjectMapper();
		String connectStr = objectMapper.writeValueAsString(map);
		LOG.info(">>>>>>>>>>>> connectStr:" + connectStr);

		return connectStr;
	}

	// 每日8時11分00秒
	@Scheduled(cron = "0 11 8 * * ?")
	public void houseKeepingStreamingHealth() throws Exception {
		LOG.info(">>>>>>>>>>>> houseKeepingStreamingHealth");

		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "";
		try {
			Class.forName(logminerDbDriver);
			conn = DriverManager.getConnection(logminerDbUrl, logminerDbUsername, logminerDbPassword);
			conn.setAutoCommit(false);

			sql = "delete " + LOGMINER_HEARTBEAT_TABLE
					+ " where HEARTBEAT_TIME in ( "
					+ "   select HEARTBEAT_TIME from " + LOGMINER_HEARTBEAT_TABLE
					+ "   where extract(day from (CURRENT_TIMESTAMP - HEARTBEAT_TIME)) > ?"
					+ ")";

			pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, 30);
			pstmt.executeUpdate();
			pstmt.close();

			sql = "delete " + STREAMING_HEALTH_TABLE
					+ " where HEARTBEAT_TIME in ( "
					+ "   select HEARTBEAT_TIME from " + STREAMING_HEALTH_TABLE
					+ "   where extract(day from (CURRENT_TIMESTAMP - HEARTBEAT_TIME)) > ?"
					+ ")";

			pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, 30);
			pstmt.executeUpdate();
			pstmt.close();

			conn.commit();

			LOG.info(">>>>>>>>>>>> houseKeepingStreamingHealth Done!!!");

		} catch (Exception e1) {

			LOG.error(">>>>> Error!!!, error msg={}, stacetrace={}", ExceptionUtils.getMessage(e1), ExceptionUtils.getStackTrace(e1));

			throw e1;
		} finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
			if (conn != null) conn.close();
		}


	}

	@Scheduled(fixedRate = 30000, initialDelay=4000)
	public void createNewConnector() throws Exception {
		LOG.info(">>>>>>>>>>>> createNewConnector");

		HttpURLConnection httpConn = null;
		DataOutputStream dataOutStream = null;
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "";
		try {
			Class.forName(logminerDbDriver);
			conn = DriverManager.getConnection(logminerDbUrl, logminerDbUsername, logminerDbPassword);
			conn.setAutoCommit(false);

			sql = "select HEARTBEAT_TIME, HEALTH_STATUS from " + STREAMING_HEALTH_TABLE
					+ " order by HEARTBEAT_TIME desc fetch next 1 row only";
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			int healthStatus = 0;
			Timestamp heartbeatTime = null;
			while (rs.next()) {
				heartbeatTime = rs.getTimestamp("HEARTBEAT_TIME");
				healthStatus = rs.getInt("HEALTH_STATUS");
			}
			rs.close();
			pstmt.close();

			if (healthStatus == -1 ) {

				Map<String, Object> cpuMap = getCpuUtilization();
				Date beginTime = (Date)cpuMap.get("BEGIN_TIME");
				Date endTime = (Date)cpuMap.get("END_TIME");
				BigDecimal cpuUtil = (BigDecimal)cpuMap.get("VALUE");

				long nowmillis = System.currentTimeMillis();
				long lag = nowmillis - heartbeatTime.getTime();
				if (lag > 600*1000) { // 10 mins
					//if (cpuUtil.doubleValue() < 30) {
					// create new connector
					LOG.info(">>>>> lag={}, cpu util < {}, create new connector...", lag, cpuUtil.doubleValue());

					String reConfigStr = getReConfigStr();	
					String urlStr = "http://localhost:" + connectRestPort+"/connectors";

					LOG.info(">>>>> connector urlStr={},reConfigStr={}", urlStr, reConfigStr);

					URL url = new URL(urlStr);
					httpConn = (HttpURLConnection)url.openConnection();
					httpConn.setRequestMethod("POST");
					httpConn.setDoInput(true);
					httpConn.setDoOutput(true);
					httpConn.setRequestProperty("Content-Type", "application/json");
					httpConn.setRequestProperty("Accept", "application/json");
					//					String data="";
					//					byte[] out = data.getBytes(StandardCharsets.UTF_8);

					dataOutStream = new DataOutputStream(httpConn.getOutputStream());
					dataOutStream.writeBytes(reConfigStr);

					dataOutStream.flush();

					int responseCode = httpConn.getResponseCode();
					LOG.info(">>>>> createNewConnector responseCode={}",responseCode);

					String connectorState = getConnectorState();
					String taskState = getConnectorTaskState();
					LOG.info(">>>>> check connectorState={},taskState={}", connectorState, taskState);

					// reset health status
					LOG.info(">>>>> set health status 8");
					sql = "INSERT INTO " + STREAMING_HEALTH_TABLE + " (HEARTBEAT_TIME,LOGMINER_SCN,LOGMINER_RECEIVED,HEALTH_STATUS,CPU_UTIL_BT,CPU_UTIL_ET,CPU_UTIL_VALUE) values (CURRENT_TIMESTAMP,NULL,NULL,?,?,?,?)";
					pstmt = conn.prepareStatement(sql);
					pstmt.setInt(1, 8);
					pstmt.setDate(2, beginTime);
					pstmt.setDate(3, endTime);
					pstmt.setBigDecimal(4, cpuUtil);
					pstmt.executeUpdate();
					conn.commit();
					pstmt.close();
				} else {
					// record and keep waiting
					LOG.info(">>>>> lag={}, cpu util >= {}, keep waiting...", lag, cpuUtil.doubleValue());

					sql = "INSERT INTO " + STREAMING_HEALTH_TABLE + " (HEARTBEAT_TIME,LOGMINER_SCN,LOGMINER_RECEIVED,HEALTH_STATUS,CPU_UTIL_BT,CPU_UTIL_ET,CPU_UTIL_VALUE) values (CURRENT_TIMESTAMP,NULL,NULL,?,?,?,?)";
					pstmt = conn.prepareStatement(sql);
					pstmt.setInt(1, -1);
					pstmt.setDate(2, beginTime);
					pstmt.setDate(3, endTime);
					pstmt.setBigDecimal(4, cpuUtil);
					pstmt.executeUpdate();

					conn.commit();
					pstmt.close();
				}

			} else {
				LOG.info(">>>>> healthStatus = {}, do nothing.", healthStatus);
			}

		}  catch (Exception e1) {

			LOG.error(">>>>> ErrorMessage={}, stacktrace={}", ExceptionUtils.getMessage(e1), ExceptionUtils.getStackTrace(e1));

			throw e1;
		} finally {
			if (dataOutStream != null) {
				try {
					dataOutStream.flush();
					dataOutStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (httpConn != null )httpConn.disconnect();

			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
			if (conn != null) conn.close();
		}

	}
	@Scheduled(fixedRate = 30000, initialDelay=3000)
	public void stopIgnite() throws Exception {
		LOG.info(">>>>>>>>>>>> stopIgnite");

		HttpURLConnection httpConn = null;
		DataOutputStream dataOutStream = null;
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "";
		try {
			Class.forName(logminerDbDriver);
			conn = DriverManager.getConnection(logminerDbUrl, logminerDbUsername, logminerDbPassword);
			conn.setAutoCommit(false);

			sql = "select HEALTH_STATUS from " + STREAMING_HEALTH_TABLE
					+ " order by HEARTBEAT_TIME desc fetch next 1 row only";
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			int healthStatus = 0;
			while (rs.next()) {
				healthStatus = rs.getInt("HEALTH_STATUS");
			}
			rs.close();
			pstmt.close();

			if (healthStatus == -9 ) {

				LOG.info(">>>>> healthStatus={},stop ignite1&ignite1", healthStatus);

				String urlStr1 = "http://localhost:" + connectRestPort+"/partycontact/stopIgnite1";
				String urlStr2 = "http://localhost:" + connectRestPort+"/partycontact/stopIgnite2";

				LOG.info(">>>>> stop ignite1 urlStr={}", urlStr1);
				LOG.info(">>>>> stop ignite2 urlStr={}", urlStr2);
				
				URL url = new URL(urlStr1);
				httpConn = (HttpURLConnection)url.openConnection();
				httpConn.setRequestMethod("POST");
				httpConn.setRequestProperty("Content-Type", "application/json");
				httpConn.setRequestProperty("Accept", "application/json");
				//					String data="";
				//					byte[] out = data.getBytes(StandardCharsets.UTF_8);

//				dataOutStream = new DataOutputStream(httpConn.getOutputStream());
//				
//				dataOutStream.flush();

				int responseCode = httpConn.getResponseCode();
				LOG.info(">>>>> stop ignite1 responseCode={}",responseCode);

				///////////////////////////////////////
				url = new URL(urlStr2);
				httpConn = (HttpURLConnection)url.openConnection();
				httpConn.setRequestMethod("POST");
				httpConn.setRequestProperty("Content-Type", "application/json");
				httpConn.setRequestProperty("Accept", "application/json");
				//					String data="";
				//					byte[] out = data.getBytes(StandardCharsets.UTF_8);

//				dataOutStream = new DataOutputStream(httpConn.getOutputStream());
//				
//				dataOutStream.flush();

				responseCode = httpConn.getResponseCode();
				LOG.info(">>>>> stop ignite2 responseCode={}",responseCode);


			} else {
				LOG.info(">>>>> healthStatus = {}, do nothing.", healthStatus);
			}

		}  catch (Exception e1) {

			LOG.error(">>>>> ErrorMessage={}, stacktrace={}", ExceptionUtils.getMessage(e1), ExceptionUtils.getStackTrace(e1));

			throw e1;
		} finally {
			if (dataOutStream != null) {
				try {
					dataOutStream.flush();
					dataOutStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (httpConn != null )httpConn.disconnect();

			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
			if (conn != null) conn.close();
		}

	}
	@Scheduled(fixedRate = 65000, initialDelay=2000)
	public void checkHealthStatusAndDeleteConnector() throws Exception {
		LOG.info(">>>>>>>>>>>> checkHealthStatusAndDeleteConnector");

		//		LOG.info(">>>>> driver={}, url={}, username={}, pswd={}", 
		//				logminerDbDriver,logminerDbUrl, logminerDbUsername, logminerDbPassword);

		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "";
		try {
			Class.forName(logminerDbDriver);
			conn = DriverManager.getConnection(logminerDbUrl, logminerDbUsername, logminerDbPassword);
			conn.setAutoCommit(false);

			sql = "select LOGMINER_RECEIVED,HEALTH_STATUS from " + STREAMING_HEALTH_TABLE
					+ " order by HEARTBEAT_TIME desc fetch next 1 row only";
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			int healthStatus = 0;
			Timestamp logminerReceived = null;
			while (rs.next()) {
				logminerReceived = rs.getTimestamp("LOGMINER_RECEIVED");
				healthStatus = rs.getInt("HEALTH_STATUS");
			}
			rs.close();
			pstmt.close();

			if (healthStatus == 1 && logminerReceived == null) {
				sql = "select count(*) as CNT from " + STREAMING_HEALTH_TABLE + "\n"  
						+ " WHERE LOGMINER_RECEIVED IS NULL and HEALTH_STATUS = 1 AND heartbeat_time >\n" 
						+ " (select heartbeat_time from STREAMING_HEALTH\n" 
						+ " where (health_status = 0 or health_status = 8) \n"
						+ " order by heartbeat_time desc fetch next 1 row only)"
						+ " order by HEARTBEAT_TIME desc";
				pstmt = conn.prepareStatement(sql);
				rs = pstmt.executeQuery();
				int abNormalCount = 0;
				while (rs.next()) {
					abNormalCount = rs.getInt("CNT");
				}
				rs.close();
				pstmt.close();

				if (abNormalCount > 20) {

					// pause connector
					pauseConnector();
					LOG.info(">>>>> call pauseConnector() Done!!!");

					//wait for 30 seconds
					LOG.info(">>>>> wait for 30 seconds");
					Thread.sleep(30000);

					// delete connector
					deleteConnector();
					LOG.info(">>>>> call deleteConnector() Done!!!");

					Map<String, Object> cpuMap = getCpuUtilization();
					Date beginTime = (Date)cpuMap.get("BEGIN_TIME");
					Date endTime = (Date)cpuMap.get("END_TIME");
					BigDecimal cpuUtil = (BigDecimal)cpuMap.get("VALUE");

					sql = "INSERT INTO " + STREAMING_HEALTH_TABLE + " (HEARTBEAT_TIME,LOGMINER_SCN,LOGMINER_RECEIVED,HEALTH_STATUS,CPU_UTIL_BT,CPU_UTIL_ET,CPU_UTIL_VALUE) values (CURRENT_TIMESTAMP,NULL,NULL,?,?,?,?)";
					pstmt = conn.prepareStatement(sql);
					pstmt.setInt(1, -9); // -1: restartable, -9: not restartable 
					pstmt.setDate(2, beginTime);
					pstmt.setDate(3, endTime);
					pstmt.setBigDecimal(4, cpuUtil);
					pstmt.executeUpdate();

					conn.commit();
					pstmt.close();

					LOG.info(">>>>> insert STREAMING_HEALTH , status -1, Done !!!");

				} else {
					LOG.info(">>>>>>>>>>>> NO UPDATE healthStatus, abNormalCount={}", abNormalCount);
				}
			} else {
				LOG.info(">>>>> healthStatus = {} , do nothing", healthStatus); 
			}

		} catch (Exception e1) {

			LOG.error(">>>>> Error!!!, error msg={}, stacetrace={}", ExceptionUtils.getMessage(e1), ExceptionUtils.getStackTrace(e1));

			throw e1;
		} finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
			if (conn != null) conn.close();
		}
	}

	@Scheduled(fixedRate = 60000, initialDelay=1000)
	public void sendLogminerHeartbeat() throws Exception {
		Calendar calendar = Calendar.getInstance();

		LOG.info(">>>>>>>>>>>> sendLogminerHeartbeat1, The time is now {}", dateFormat.format(calendar.getTime()));

		//		LOG.info(">>>>> driver={}, url={}, username={}, pswd={}", 
		//				logminerDbDriver,logminerDbUrl, logminerDbUsername, logminerDbPassword);

		Connection conn = null;
		PreparedStatement pstmt1 = null;
		PreparedStatement pstmt2 = null;
		ResultSet rs = null;
		String sql = "";
		try {
			Class.forName(logminerDbDriver);
			conn = DriverManager.getConnection(logminerDbUrl, logminerDbUsername, logminerDbPassword);
			conn.setAutoCommit(false);

			if (initialLogminer) {

				boolean serverRequired = true;
				//				String zookeeperStatus = getServerStatus(ZOOKEEPER_SERVER);
				//				String kafkaStatus = getServerStatus(KAFKA_SERVER);
				//				String logminerStatus = getServerStatus(LOGMINER_SERVER);
				//				if (SERVER_STATUS_RUNNING.equals(zookeeperStatus)
				//						&& SERVER_STATUS_RUNNING.equals(kafkaStatus)
				//						&& SERVER_STATUS_RUNNING.equals(logminerStatus)) {
				//					serverRequired = true;
				//				} 

				if (serverRequired) {
					sql = "INSERT INTO " + STREAMING_HEALTH_TABLE  + " (HEARTBEAT_TIME,LOGMINER_SCN,LOGMINER_RECEIVED,HEALTH_STATUS,CPU_UTIL_BT,CPU_UTIL_ET,CPU_UTIL_VALUE) values (CURRENT_TIMESTAMP,NULL,NULL,0,NULL,NULL,NULL)";
					pstmt1 = conn.prepareStatement(sql);
					pstmt1.executeUpdate();

					conn.commit();
					pstmt1.close();

					initialLogminer = false;

					//					LOG.info(">>> Start sending heartbeat, zookeeper status={}, kafka status={},logminer status={}", zookeeperStatus, kafkaStatus, logminerStatus);
				} else {
					//					LOG.info(">>> NO heartbeat sent, zookeeper status={}, kafka status={},logminer status={}", zookeeperStatus, kafkaStatus, logminerStatus);
					return;
				}
			}
			Thread.sleep(1000);

			sql = "select HEALTH_STATUS from " + STREAMING_HEALTH_TABLE
					+ " order by HEARTBEAT_TIME desc fetch next 1 row only";
			pstmt1 = conn.prepareStatement(sql);
			rs = pstmt1.executeQuery();
			int healthStatus = 0;
			while (rs.next()) {
				healthStatus = rs.getInt("HEALTH_STATUS");
			}
			rs.close();
			pstmt1.close();

			if (healthStatus == 0 || healthStatus == 1 || healthStatus == 8) {

				Map<String, Object> cpuMap = getCpuUtilization();
				Date beginTime = (Date)cpuMap.get("BEGIN_TIME");
				Date endTime = (Date)cpuMap.get("END_TIME");
				BigDecimal cpuUtil = (BigDecimal)cpuMap.get("VALUE");

				long currTimeMillis = System.currentTimeMillis();
				Timestamp heartbeatTimestamp = new Timestamp(currTimeMillis);
				conn.setAutoCommit(false);
				sql = "insert into " + LOGMINER_HEARTBEAT_TABLE + " (HEARTBEAT_TIME) values(?)";
				pstmt1 = conn.prepareStatement(sql);
				pstmt1.setTimestamp(1, heartbeatTimestamp);
				pstmt1.executeUpdate();

				sql = "insert into  " + STREAMING_HEALTH_TABLE + " (HEARTBEAT_TIME,LOGMINER_SCN,LOGMINER_RECEIVED,HEALTH_STATUS,CPU_UTIL_BT,CPU_UTIL_ET,CPU_UTIL_VALUE) values(?,NULL,NULL,?,?,?,?)";
				pstmt2 = conn.prepareStatement(sql);
				pstmt2.setTimestamp(1, heartbeatTimestamp);
				pstmt2.setInt(2, 1);
				pstmt2.setDate(3, beginTime);
				pstmt2.setDate(4, endTime);
				pstmt2.setBigDecimal(5, cpuUtil);
				pstmt2.executeUpdate();

				conn.commit();
				pstmt1.close();
				pstmt2.close();

				LOG.info(">>>>> insert HEARTBEAT_TIME={}", heartbeatTimestamp);

				//				Timestamp heartBeatTime = null;
				//				Timestamp logminerReceived = null;
				//				int queryCount1 = 0;
				//				boolean logminerFailed = false;
				//
				//				while (logminerReceived == null) {
				//					if (queryCount1 < 10) {
				//						Thread.sleep(500);
				//						queryCount1++;
				//					} else {
				//						logminerFailed = true;
				//						break;
				//					}
				//
				//					sql = "select HEARTBEAT_TIME,LOGMINER_RECEIVED from " + STREAMING_HEALTH_TABLE
				//							+ " where HEARTBEAT_TIME=?";
				//					pstmt1 = conn.prepareStatement(sql);
				//					pstmt1.setTimestamp(1, new Timestamp(currTimeMillis));
				//					rs = pstmt1.executeQuery();
				//
				//					while (rs.next()) {
				//						heartBeatTime = rs.getTimestamp("HEARTBEAT_TIME");
				//						logminerReceived = rs.getTimestamp("LOGMINER_RECEIVED");
				//					}
				//					rs.close();
				//					pstmt1.close();
				//
				//					LOG.info(">>>>> logminerReceived={},queryCount1={}",logminerReceived,queryCount1); 
				//
				//				}
				//				if (logminerFailed) {
				//					long nowmillis = System.currentTimeMillis();
				//					LOG.info("XXXXX Logminer sync NOT OK. No response from logminer, Logminer sync Error!!!, currTimeMillis={}, nowmillis={},queryCount1={}",currTimeMillis, nowmillis, queryCount1);
				//
				//				} else {
				//					LOG.info(">>>>> Logminer sync OK.  HeartBeatTime.Time={}, logminerReceived.Time={},queryCount1={}", heartBeatTime, logminerReceived,queryCount1);
				//				}
			} else {
				LOG.info(">>>>> healthStatus={}, do nothing", healthStatus);
			}
		} catch (Exception e1) {

			LOG.error(">>>>> Error!!!, error msg={}, stacetrace={}", ExceptionUtils.getMessage(e1), ExceptionUtils.getStackTrace(e1));

			throw e1;
		} finally {
			if (rs != null) rs.close();
			if (pstmt1 != null) pstmt1.close();
			if (pstmt2 != null) pstmt2.close();
			if (conn != null) conn.close();
		}
	}
	private Map<String, Object> getCpuUtilization() throws Exception {
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "";
		Map<String, Object> map = null;
		try {
			Class.forName(logminerDbDriver);
			conn = DriverManager.getConnection(logminerDbUrl, logminerDbUsername, logminerDbPassword);
			conn.setAutoCommit(false);

			// metric:2057 Host CPU Utilization (%)
			sql = "select BEGIN_TIME,END_TIME,VALUE from V$SYSMETRIC_HISTORY"
					+ " where METRIC_ID = 2057 order by BEGIN_TIME desc fetch next 1 row only";
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			Date beginTime = null;
			Date endTime = null;
			BigDecimal value = null;
			while (rs.next()) {
				beginTime = rs.getDate("BEGIN_TIME");
				endTime = rs.getDate("END_TIME");
				value = rs.getBigDecimal("VALUE");
			}
			rs.close();
			pstmt.close();

			if (beginTime != null) {
				map = new HashMap<>();
				map.put("BEGIN_TIME", beginTime);
				map.put("END_TIME", endTime);
				map.put("VALUE", value);
			}

		} catch (Exception e1) {

			LOG.error(">>>>> Error!!!, error msg={}, stacetrace={}", ExceptionUtils.getMessage(e1), ExceptionUtils.getStackTrace(e1));

			throw e1;
		} finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
			if (conn != null) conn.close();
		}
		return map;
	}
	private void pauseConnector() throws Exception{
		String urlStr = "http://localhost:" + connectRestPort+"/connectors/"+connectConnectorName+"/pause";
		LOG.info(">>>>> connector urlStr:" + urlStr);
		HttpURLConnection putConn = null;
		//		DataOutputStream dataOutStream = null;
		try {
			URL url = new URL(urlStr);
			putConn = (HttpURLConnection)url.openConnection();
			putConn.setRequestMethod("PUT");

			int responseCode = putConn.getResponseCode();
			LOG.info(">>>>> pause responseCode={}",responseCode);

		} finally {

			if (putConn != null )putConn.disconnect();
		}
	}
	private void deleteConnector() throws Exception{
		String urlStr = "http://localhost:" + connectRestPort+"/connectors/"+connectConnectorName+"/";
		//		LOG.info(">>>>> connector urlStr:" + urlStr);
		HttpURLConnection httpConn = null;
		try {
			URL url = new URL(urlStr);
			httpConn = (HttpURLConnection)url.openConnection();
			httpConn.setRequestMethod("DELETE");
			int responseCode = httpConn.getResponseCode();
			LOG.info(">>>>> DELETE responseCode={}",responseCode);

		} finally {
			if (httpConn != null )httpConn.disconnect();
		}
	}
	private void resumeConnector() throws Exception{
		String urlStr = "http://localhost:" + connectRestPort+"/connectors/"+connectConnectorName+"/resume";
		//		LOG.info(">>>>> connector urlStr:" + urlStr);
		HttpURLConnection putConn = null;
		try {
			URL url = new URL(urlStr);
			putConn = (HttpURLConnection)url.openConnection();
			putConn.setRequestMethod("PUT");
			int responseCode = putConn.getResponseCode();
			LOG.info(">>>>> resume responseCode={}",responseCode);

		} finally {
			if (putConn != null )putConn.disconnect();
		}
	}
	private void createConnector() throws Exception{
		String urlStr = "http://localhost:" + connectRestPort+"/connectors";
		//		LOG.info(">>>>> connector urlStr:" + urlStr);
		HttpURLConnection httpConn = null;
		try {
			URL url = new URL(urlStr);
			httpConn = (HttpURLConnection)url.openConnection();
			httpConn.setRequestMethod("POST");
			int responseCode = httpConn.getResponseCode();
			LOG.info(">>>>> restart task responseCode={}",responseCode);

		} finally {
			if (httpConn != null ) httpConn.disconnect();
		}
	}
	private void restartConnectorTask() throws Exception{
		String urlStr = "http://localhost:" + connectRestPort+"/connectors/"+connectConnectorName+"/tasks/0/restart";
		//		LOG.info(">>>>> connector urlStr:" + urlStr);
		HttpURLConnection httpConn = null;
		try {
			URL url = new URL(urlStr);
			httpConn = (HttpURLConnection)url.openConnection();
			httpConn.setRequestMethod("POST");
			int responseCode = httpConn.getResponseCode();
			LOG.info(">>>>> restart task responseCode={}",responseCode);

		} finally {
			if (httpConn != null ) httpConn.disconnect();
		}
	}
	private String getConnectorState() throws Exception{
		String urlStr = "http://localhost:" + connectRestPort+"/connectors/"+connectConnectorName+"/status";
		//		LOG.info(">>>>> connector urlStr:" + urlStr);
		HttpURLConnection httpCon = null;
		try {
			URL url = new URL(urlStr);
			httpCon = (HttpURLConnection)url.openConnection();
			httpCon.setRequestMethod("GET");
			int responseCode = httpCon.getResponseCode();
			String readLine = null;
			if (httpCon.HTTP_OK == responseCode) {
				BufferedReader in = new BufferedReader(new InputStreamReader(httpCon.getInputStream()));
				StringBuffer response = new StringBuffer();
				while ((readLine = in.readLine()) != null) {
					response.append(readLine);
				}
				in.close();
				//			LOG.info(">>>>> CONNECT REST response", response.toString());

				ObjectMapper objectMapper = new ObjectMapper();
				JsonNode jsonNode = objectMapper.readTree(response.toString());
				JsonNode connectorNode = jsonNode.get("connector");
				JsonNode stateNode = connectorNode.get("state");
				String state = stateNode.asText();
				//			LOG.info(">>>>> sconnector tate={}", state);
				return state;
			} else {
				LOG.error("XXXXX CONNECTOR GET STATE NOT WORKED");
				throw new Exception("CONNECTOR GET STATE NOT WORKED");
			}
		} finally {
			if (httpCon != null ) httpCon.disconnect();
		}
	}
	private String getConnectorTaskState() throws Exception{
		String urlStr = "http://localhost:" + connectRestPort+"/connectors/"+connectConnectorName+"/tasks/0/status";
		//		LOG.info(">>>>> connector urlStr:" + urlStr);
		HttpURLConnection httpCon = null;
		try {
			URL url = new URL(urlStr);
			httpCon = (HttpURLConnection)url.openConnection();
			httpCon.setRequestMethod("GET");
			int responseCode = httpCon.getResponseCode();
			String readLine = null;
			if (httpCon.HTTP_OK == responseCode) {
				BufferedReader in = new BufferedReader(new InputStreamReader(httpCon.getInputStream()));
				StringBuffer response = new StringBuffer();
				while ((readLine = in.readLine()) != null) {
					response.append(readLine);
				}
				in.close();
				//			LOG.info(">>>>> CONNECT REST response", response.toString());

				ObjectMapper objectMapper = new ObjectMapper();
				JsonNode jsonNode = objectMapper.readTree(response.toString());
				JsonNode stateNode = jsonNode.get("state");
				String state = stateNode.asText();
				//			LOG.info(">>>>> sconnector tate={}", state);
				return state;
			} else {
				LOG.error("XXXXX CONNECTOR TASK GET STATE NOT WORKED");
				throw new Exception("CONNECTOR TASK  GET STATE NOT WORKED");
			}
		} finally {
			if (httpCon != null ) httpCon.disconnect();
		}
	}
	//	@Scheduled(fixedRate = 60000, initialDelay=1000)
	public void sendLogminerHeartbeat2() throws Exception {
		Calendar calendar = Calendar.getInstance();

		LOG.info(">>>>>>>>>>>> sendLogminerHeartbeat1, The time is now {}", dateFormat.format(calendar.getTime()));

		//		LOG.info(">>>>> driver={}, url={}, username={}, pswd={}", 
		//				logminerDbDriver,logminerDbUrl, logminerDbUsername, logminerDbPassword);

		Connection conn = null;
		PreparedStatement pstmt1 = null;
		PreparedStatement pstmt2 = null;
		ResultSet rs = null;
		String sql = "";
		try {
			Class.forName(logminerDbDriver);
			conn = DriverManager.getConnection(logminerDbUrl, logminerDbUsername, logminerDbPassword);
			conn.setAutoCommit(false);

			if (initialLogminer) {
				sql = "INSERT INTO " + STREAMING_HEALTH_TABLE  + " (HEARTBEAT_TIME,LOGMINER_SCN,LOGMINER_RECEIVED,HEALTH_STATUS,CPU_UTIL_BT,CPU_UTIL_ET,CPU_UTIL_VALUE) values (CURRENT_TIMESTAMP,NULL,NULL,0,NULL,NULL,NULL)";
				pstmt1 = conn.prepareStatement(sql);
				pstmt1.executeUpdate();

				conn.commit();
				pstmt1.close();

				initialLogminer = false;
			}
			Thread.sleep(1000);

			sql = "select HEALTH_STATUS from " + STREAMING_HEALTH_TABLE
					+ " order by HEARTBEAT_TIME desc fetch next 1 row only";
			pstmt1 = conn.prepareStatement(sql);
			rs = pstmt1.executeQuery();
			int healthStatus = 0;
			while (rs.next()) {
				healthStatus = rs.getInt("HEALTH_STATUS");
			}
			rs.close();
			pstmt1.close();

			i0++;
			if (healthStatus == 0 || healthStatus == 1 || healthStatus == 8) {

				Map<String, Object> cpuMap = getCpuUtilization();
				Date beginTime = (Date)cpuMap.get("BEGIN_TIME");
				Date endTime = (Date)cpuMap.get("END_TIME");
				BigDecimal cpuUtil = (BigDecimal)cpuMap.get("VALUE");

				long currTimeMillis = System.currentTimeMillis();
				Timestamp heartbeatTimestamp = new Timestamp(currTimeMillis);
				conn.setAutoCommit(false);
				sql = "insert into " + LOGMINER_HEARTBEAT_TABLE + " (HEARTBEAT_TIME) values(?)";
				pstmt1 = conn.prepareStatement(sql);
				pstmt1.setTimestamp(1, heartbeatTimestamp);
				pstmt1.executeUpdate();

				sql = "insert into  " + STREAMING_HEALTH_TABLE + " (HEARTBEAT_TIME,LOGMINER_SCN,LOGMINER_RECEIVED,HEALTH_STATUS,CPU_UTIL_BT,CPU_UTIL_ET,CPU_UTIL_VALUE) values(?,NULL,NULL,?,?,?,?)";
				pstmt2 = conn.prepareStatement(sql);
				pstmt2.setTimestamp(1, heartbeatTimestamp);
				pstmt2.setInt(2, 1);
				pstmt2.setDate(3, beginTime);
				pstmt2.setDate(4, endTime);
				pstmt2.setBigDecimal(5, cpuUtil);
				pstmt2.executeUpdate();

				conn.commit();
				pstmt1.close();
				pstmt2.close();

				LOG.info(">>>>> insert HEARTBEAT_TIME={}", heartbeatTimestamp);

				Timestamp heartBeatTime = null;
				Timestamp logminerReceived = null;
				int queryCount1 = 0;
				boolean logminerFailed = false;

				while (logminerReceived == null) {
					if (queryCount1 < 10) {
						Thread.sleep(500);
						queryCount1++;
					} else {
						logminerFailed = true;
						break;
					}

					sql = "select HEARTBEAT_TIME,LOGMINER_RECEIVED from " + STREAMING_HEALTH_TABLE
							+ " where HEARTBEAT_TIME=?";
					pstmt1 = conn.prepareStatement(sql);
					pstmt1.setTimestamp(1, new Timestamp(currTimeMillis));
					rs = pstmt1.executeQuery();

					while (rs.next()) {
						heartBeatTime = rs.getTimestamp("HEARTBEAT_TIME");
						logminerReceived = rs.getTimestamp("LOGMINER_RECEIVED");
					}
					rs.close();
					pstmt1.close();

					LOG.info(">>>>> logminerReceived={},queryCount1={}",logminerReceived,queryCount1); 

				}

				if ((i0 > 3 && i1 == 0) || (i0 > 10 && i1 == 1)) {
					LOG.info(">>>>> for testing set received null");
					sql = "update " + STREAMING_HEALTH_TABLE
							+ " set LOGMINER_RECEIVED =NULL where HEARTBEAT_TIME=?";
					pstmt1 = conn.prepareStatement(sql);
					pstmt1.setTimestamp(1, new Timestamp(currTimeMillis));
					pstmt1.executeUpdate();

					pstmt1.close();
				}

				if (logminerFailed) {
					long nowmillis = System.currentTimeMillis();
					LOG.info("XXXXX Logminer sync NOT OK. No response from logminer, Logminer sync Error!!!, currTimeMillis={}, nowmillis={},queryCount1={}",currTimeMillis, nowmillis, queryCount1);

				} else {
					LOG.info(">>>>> Logminer sync OK.  HeartBeatTime.Time={}, logminerReceived.Time={},queryCount1={}", heartBeatTime, logminerReceived,queryCount1);
				}
			} else {
				LOG.info(">>>>> healthStatus={}, do nothing", healthStatus);
			}
		} catch (Exception e1) {

			LOG.error(">>>>> Error!!!, error msg={}, stacetrace={}", ExceptionUtils.getMessage(e1), ExceptionUtils.getStackTrace(e1));

			throw e1;
		} finally {
			if (rs != null) rs.close();
			if (pstmt1 != null) pstmt1.close();
			if (pstmt2 != null) pstmt2.close();
			if (conn != null) conn.close();
		}
	}
	//	@Scheduled(fixedRate = 65000, initialDelay=2000)
	public void checkHealthStatusAndDeleteConnector2() throws Exception {
		LOG.info(">>>>>>>>>>>> checkHealthStatusAndDeleteConnector2");

		//		LOG.info(">>>>> driver={}, url={}, username={}, pswd={}", 
		//				logminerDbDriver,logminerDbUrl, logminerDbUsername, logminerDbPassword);

		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "";
		try {
			Class.forName(logminerDbDriver);
			conn = DriverManager.getConnection(logminerDbUrl, logminerDbUsername, logminerDbPassword);
			conn.setAutoCommit(false);

			sql = "select LOGMINER_RECEIVED,HEALTH_STATUS from " + STREAMING_HEALTH_TABLE
					+ " order by HEARTBEAT_TIME desc fetch next 1 row only";
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			int healthStatus = 0;
			Timestamp logminerReceived = null;
			while (rs.next()) {
				logminerReceived = rs.getTimestamp("LOGMINER_RECEIVED");
				healthStatus = rs.getInt("HEALTH_STATUS");
			}
			rs.close();
			pstmt.close();

			if (healthStatus == 1 && logminerReceived == null) {
				sql = "select count(*) as CNT from " + STREAMING_HEALTH_TABLE + "\n"  
						+ " WHERE LOGMINER_RECEIVED IS NULL and HEALTH_STATUS = 1 AND heartbeat_time >\n" 
						+ " (select heartbeat_time from STREAMING_HEALTH\n" 
						+ " where (health_status = 0 or health_status = 8) \n"
						+ " order by heartbeat_time desc fetch next 1 row only)"
						+ " order by HEARTBEAT_TIME desc";
				pstmt = conn.prepareStatement(sql);
				rs = pstmt.executeQuery();
				int abNormalCount = 0;
				while (rs.next()) {
					abNormalCount = rs.getInt("CNT");
				}
				rs.close();
				pstmt.close();

				if (abNormalCount > 2) {

					// pause connector
					pauseConnector();
					LOG.info(">>>>> call pauseConnector() Done!!!");

					//wait for 30 seconds
					LOG.info(">>>>> wait for 30 seconds");
					Thread.sleep(30000);

					// delete connector
					deleteConnector();
					LOG.info(">>>>> call deleteConnector() Done!!!");

					Map<String, Object> cpuMap = getCpuUtilization();
					Date beginTime = (Date)cpuMap.get("BEGIN_TIME");
					Date endTime = (Date)cpuMap.get("END_TIME");
					BigDecimal cpuUtil = (BigDecimal)cpuMap.get("VALUE");

					sql = "INSERT INTO " + STREAMING_HEALTH_TABLE + " (HEARTBEAT_TIME,LOGMINER_SCN,LOGMINER_RECEIVED,HEALTH_STATUS,CPU_UTIL_BT,CPU_UTIL_ET,CPU_UTIL_VALUE) values (CURRENT_TIMESTAMP,NULL,NULL,?,?,?,?)";
					pstmt = conn.prepareStatement(sql);
					pstmt.setInt(1, -1);
					pstmt.setDate(2, beginTime);
					pstmt.setDate(3, endTime);
					pstmt.setBigDecimal(4, cpuUtil);
					pstmt.executeUpdate();

					conn.commit();
					pstmt.close();

					LOG.info(">>>>> insert STREAMING_HEALTH , status -1, Done !!!");

				} else {
					LOG.info(">>>>>>>>>>>> NO UPDATE healthStatus, abNormalCount={}", abNormalCount);
				}
			} else {
				LOG.info(">>>>> healthStatus = {} , do nothing", healthStatus); 
			}

		} catch (Exception e1) {

			LOG.error(">>>>> Error!!!, error msg={}, stacetrace={}", ExceptionUtils.getMessage(e1), ExceptionUtils.getStackTrace(e1));

			throw e1;
		} finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
			if (conn != null) conn.close();
		}
	}
	//	@Scheduled(fixedRate = 30000, initialDelay=4000)
	public void createNewConnector2() throws Exception {
		LOG.info(">>>>>>>>>>>> createNewConnector");

		HttpURLConnection httpConn = null;
		DataOutputStream dataOutStream = null;
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "";
		try {
			Class.forName(logminerDbDriver);
			conn = DriverManager.getConnection(logminerDbUrl, logminerDbUsername, logminerDbPassword);
			conn.setAutoCommit(false);

			sql = "select HEARTBEAT_TIME, HEALTH_STATUS from " + STREAMING_HEALTH_TABLE
					+ " order by HEARTBEAT_TIME desc fetch next 1 row only";
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			int healthStatus = 0;
			while (rs.next()) {
				healthStatus = rs.getInt("HEALTH_STATUS");
			}
			rs.close();
			pstmt.close();

			if (healthStatus == -1 ) {

				Map<String, Object> cpuMap = getCpuUtilization();
				Date beginTime = (Date)cpuMap.get("BEGIN_TIME");
				Date endTime = (Date)cpuMap.get("END_TIME");
				BigDecimal cpuUtil = (BigDecimal)cpuMap.get("VALUE");

				//	if (cpuUtil.doubleValue() < 30) {
				// create new connector
				LOG.info(">>>>> cpu util < {}, create new connector...", cpuUtil.doubleValue());

				String reConfigStr = getReConfigStr();	
				String urlStr = "http://localhost:" + connectRestPort+"/connectors";

				LOG.info(">>>>> connector urlStr={},reConfigStr={}", urlStr, reConfigStr);

				URL url = new URL(urlStr);
				httpConn = (HttpURLConnection)url.openConnection();
				httpConn.setRequestMethod("POST");
				httpConn.setDoInput(true);
				httpConn.setDoOutput(true);
				httpConn.setRequestProperty("Content-Type", "application/json");
				httpConn.setRequestProperty("Accept", "application/json");
				//					String data="";
				//					byte[] out = data.getBytes(StandardCharsets.UTF_8);

				dataOutStream = new DataOutputStream(httpConn.getOutputStream());
				dataOutStream.writeBytes(reConfigStr);

				dataOutStream.flush();

				int responseCode = httpConn.getResponseCode();
				LOG.info(">>>>> createNewConnector responseCode={}",responseCode);

				String connectorState = getConnectorState();
				String taskState = getConnectorTaskState();
				LOG.info(">>>>> check connectorState={},taskState={}", connectorState, taskState);

				// reset health status
				LOG.info(">>>>> set health status 8");
				sql = "INSERT INTO " + STREAMING_HEALTH_TABLE + " (HEARTBEAT_TIME,LOGMINER_SCN,LOGMINER_RECEIVED,HEALTH_STATUS,CPU_UTIL_BT,CPU_UTIL_ET,CPU_UTIL_VALUE) values (CURRENT_TIMESTAMP,NULL,NULL,?,?,?,?)";
				pstmt = conn.prepareStatement(sql);
				pstmt.setInt(1, 8);
				pstmt.setDate(2, beginTime);
				pstmt.setDate(3, endTime);
				pstmt.setBigDecimal(4, cpuUtil);
				pstmt.executeUpdate();
				conn.commit();
				pstmt.close();

				if ( i1== 0) {
					i1 = 1;
				} else if (i1 == 1) {
					i1 = 2;
				}
				//				} else {
				//					// record and keep waiting
				//					LOG.info(">>>>> cpu util >= {}, keep waiting...", cpuUtil.doubleValue());
				//
				//					sql = "INSERT INTO " + STREAMING_HEALTH_TABLE + " (HEARTBEAT_TIME,LOGMINER_SCN,LOGMINER_RECEIVED,HEALTH_STATUS,CPU_UTIL_BT,CPU_UTIL_ET,CPU_UTIL_VALUE) values (CURRENT_TIMESTAMP,NULL,NULL,?,?,?,?)";
				//					pstmt = conn.prepareStatement(sql);
				//					pstmt.setInt(1, -1);
				//					pstmt.setDate(2, beginTime);
				//					pstmt.setDate(3, endTime);
				//					pstmt.setBigDecimal(4, cpuUtil);
				//					pstmt.executeUpdate();
				//
				//					conn.commit();
				//					pstmt.close();
				//				}skipRecord=false; // do not skip record

			} else {
				LOG.info(">>>>> healthStatus = {}, do nothing.", healthStatus);
			}

		}  catch (Exception e1) {

			LOG.error(">>>>> ErrorMessage={}, stacktrace={}", ExceptionUtils.getMessage(e1), ExceptionUtils.getStackTrace(e1));

			throw e1;
		} finally {
			if (dataOutStream != null) {
				try {
					dataOutStream.flush();
					dataOutStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (httpConn != null )httpConn.disconnect();

			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
			if (conn != null) conn.close();
		}

	}
//	private String getServerStatus(String servername) throws Exception {
//		String urlStr = "";
//		if (ZOOKEEPER_SERVER.equals(servername)) {
//			urlStr = "http://localhost:" + serverPort+"/streamingetl/zookeeperStatus";
//		} else if (KAFKA_SERVER.equals(servername)) {
//			urlStr = "http://localhost:" + serverPort+"/streamingetl/kafkaStatus";
//		} else if (LOGMINER_SERVER.equals(servername)) {
//			urlStr = "http://localhost:" + serverPort+"/streamingetl/logminerStatus";
//		} else if (HEARTBEAT_CONSUMER_SERVER.equals(servername)) {
//			urlStr = "http://localhost:" + serverPort+"/streamingetl/heartbeatConsumerStatus";
//		}
//		LOG.info(">>>>> connector urlStr:" + urlStr);
//		HttpURLConnection httpCon = null;
//		//		DataOutputStream dataOutStream = null;
//		String status = null;
//		try {
//			URL url = new URL(urlStr);
//			httpCon = (HttpURLConnection)url.openConnection();
//			httpCon.setRequestMethod("GET");
//			int responseCode = httpCon.getResponseCode();
//			String readLine = null;
//
//			if (httpCon.HTTP_OK == responseCode) {
//				BufferedReader in = new BufferedReader(new InputStreamReader(httpCon.getInputStream()));
//				StringBuffer response = new StringBuffer();
//				while ((readLine = in.readLine()) != null) {
//					response.append(readLine);
//				}
//				in.close();
//				//			LOG.info(">>>>> CONNECT REST response", response.toString());
//				status = response.toString();
//			} else {
//				LOG.error(">>> getServerStatus Error!!!");
//				throw new Exception("getServerStatus Error!!!");
//			}
//
//		} finally {
//			if (httpCon != null )httpCon.disconnect();
//		}
//		return status;
//	}
}
