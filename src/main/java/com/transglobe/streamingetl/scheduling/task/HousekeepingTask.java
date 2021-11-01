package com.transglobe.streamingetl.scheduling.task;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class HousekeepingTask {
	private static final Logger LOG = LoggerFactory.getLogger(HousekeepingTask.class);

	private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	@Value("${kafka.logs.dir}")
	private String kafkaLogsDir;
	
	@Value("${streamingetl.logs.dir}")
	private String streamingetlLogsDir;
	
	@Value("${partycontact.logs.dir}")
	private String partyContactLogsDir;

	@Scheduled(fixedRate = 60000)
	public void reportCurrentTime() {
		Calendar calendar = Calendar.getInstance();

		LOG.info(" The time is now {}", dateFormat.format(calendar.getTime()));


	}
	// 每日12時11分00秒
	@Scheduled(cron = "0 11 12 * * ?")
//	@Scheduled(fixedRate = 40000)
	public void deleteKafkaLogs() {

		LOG.info(">>>deleteKafkaLogs -  {}", dateFormat.format(new Date()));

		LOG.info("Housekeeping connect log");
		deleteLogFiles(kafkaLogsDir, "connect");

		LOG.info("Housekeeping controlelr log");
		deleteLogFiles(kafkaLogsDir, "controller");

		LOG.info("Housekeeping log-cleaner log");
		deleteLogFiles(kafkaLogsDir, "log-cleaner");

		LOG.info("Housekeeping server log");
		deleteLogFiles(kafkaLogsDir, "server");

		LOG.info("Housekeeping state-change log");
		deleteLogFiles(kafkaLogsDir, "state-change");

		LOG.info("Housekeeping streamingetl-common log");
		deleteLogFiles(streamingetlLogsDir, "streamingetl-common");
		
		LOG.info("Housekeeping streamingetl-rest log");
		deleteLogFiles(streamingetlLogsDir, "streamingetl-rest");
		
		LOG.info("Housekeeping pcr420669-consumer log");
		deleteLogFiles(partyContactLogsDir, "pcr420669-consumer");
		
		LOG.info("Housekeeping pcr420669-load log");
		deleteLogFiles(partyContactLogsDir, "pcr420669-load");
	}
	
	private void deleteLogFiles(String logDir, String logFile) {
		Calendar calendar = Calendar.getInstance();

		calendar.add(Calendar.MONTH, -1); // prev month

		int thisYear = calendar.get(Calendar.YEAR);
		int prevMonth = calendar.get(Calendar.MONTH) + 1; 
		int prevDay = calendar.get(Calendar.DAY_OF_MONTH);

		//		LOG.info(" one month ago {}, year={},month={},day={}", 
		//				dateFormat.format(calendar.getTime())
		//				, thisYear, prevMonth, prevDay);

		//		LOG.info("kafkaLogsDir:{}", kafkaLogsDir);

		File dir = new File(logDir);

		Integer yrStart = null;
		Integer yrEnd = null;
		Integer moStart = null;
		Integer moEnd = null;
		Integer dayStart = null;
		Integer dayEnd = null;
		String filePattern = null;
		if (StringUtils.equals("connect", logFile)) {
			filePattern = "^connect.log..*";
			yrStart = 12;
			yrEnd = 16;
			moStart = 17;
			moEnd = 19;
			dayStart = 20;
			dayEnd = 22;
		} else if (StringUtils.equals("controller", logFile)) {
			filePattern = "^controller.log..*";
			yrStart = 15;
			yrEnd = 19;
			moStart = 20;
			moEnd = 22;
			dayStart = 23;
			dayEnd = 25;
		}  else if (StringUtils.equals("log-cleaner", logFile)) {
			filePattern = "^log-cleaner.log..*";
			yrStart = 16;
			yrEnd = 20;
			moStart = 21;
			moEnd = 23;
			dayStart = 24;
			dayEnd = 26;
		} else if (StringUtils.equals("server", logFile)) {
			filePattern = "^server.log..*";
			yrStart = 11;
			yrEnd = 15;
			moStart = 16;
			moEnd = 18;
			dayStart = 19;
			dayEnd = 21;
		} else if (StringUtils.equals("state-change", logFile)) {
			filePattern = "^state-change.log..*";
			yrStart = 17;
			yrEnd = 21;
			moStart = 22;
			moEnd = 24;
			dayStart = 25;
			dayEnd = 27;
		}  else if (StringUtils.equals("streamingetl-common", logFile)) {
			filePattern = "^streamingetl-common.log..*";
			yrStart = 24;
			yrEnd = 28;
			moStart = 29;
			moEnd = 31;
			dayStart = 32;
			dayEnd = 34;
		} else if (StringUtils.equals("streamingetl-rest", logFile)) {
			filePattern = "^streamingetl-rest.log..*";
			yrStart = 22;
			yrEnd = 26;
			moStart = 27;
			moEnd = 29;
			dayStart = 30;
			dayEnd = 32;
		} else if (StringUtils.equals("pcr420669-consumer", logFile)) {
			filePattern = "^pcr420669-consumer.log..*";
			yrStart = 23;
			yrEnd = 27;
			moStart = 28;
			moEnd = 30;
			dayStart = 31;
			dayEnd = 33;
		} else if (StringUtils.equals("pcr420669-load", logFile)) {
			filePattern = "^pcr420669-load.log..*";
			yrStart = 19;
			yrEnd = 23;
			moStart = 24;
			moEnd = 26;
			dayStart = 27;
			dayEnd = 29;
		}

		FileFilter fileFilter = new RegexFileFilter(filePattern);
		File[] files = dir.listFiles(fileFilter);

		for (File file : files) {
			try {
				String yr = StringUtils.substring(file.getName(), yrStart, yrEnd);
				String mo = StringUtils.substring(file.getName(), moStart, moEnd);
				String day = StringUtils.substring(file.getName(), dayStart, dayEnd);

				//				LOG.info(">>>yr:{},{},{}, prev year:{},{},{}", yr,mo,day,thisYear,prevMonth, prevDay);
				if (Integer.valueOf(yr).intValue() > thisYear) {
					// ignore
				} else if (Integer.valueOf(yr).intValue() < thisYear) {
					Files.delete(Paths.get(file.getAbsolutePath()));
					//					LOG.info(">>> dlete file:{},yr:{},mo:{},d:{}",file.getName(),yr,mo,day);
				} else  {
					if (Integer.valueOf(mo).intValue() > prevMonth) {
						// ignore
					} else if (Integer.valueOf(mo).intValue() < prevMonth) {
						Files.delete(Paths.get(file.getAbsolutePath()));
						//						LOG.info(">>> dlete file:{},yr:{},mo:{},d:{}",file.getName(),yr,mo,day);
					} else {
						if (Integer.valueOf(day).intValue() >= prevDay) {
							// ignore
						} else {
							Files.delete(Paths.get(file.getAbsolutePath()));
							//							LOG.info(">>> dlete file:{},yr:{},mo:{},d:{}",file.getName(),yr,mo,day);
						}
					}
				}
			} catch (IOException e) {
				LOG.info("message={},trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			}
		}
	}


}
