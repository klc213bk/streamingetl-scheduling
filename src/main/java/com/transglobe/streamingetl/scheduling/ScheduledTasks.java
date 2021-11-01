/*
 * Copyright 2012-2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *	  https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.transglobe.streamingetl.scheduling;

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

public class ScheduledTasks {

	private static final Logger log = LoggerFactory.getLogger(ScheduledTasks.class);

	private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	@Value("${app.message}")
	private String welcomeMessage;

	@Value("${kafka.logs.dir}")
	private String kafkaLogsDir;

	@Value("${kafka.controller.log.file}")
	private String kafkaControllerLogFile;

	@Value("${kafka.log-cleaner.log.file}")
	private String kafkaLogcleanerLogFile;

	@Value("${kafka.server.log.file}")
	private String kafkaServerLogFile;

	@Value("${kafka.state-change.log.file}")
	private String kafkaStatechangeLogFile;

	@Value("${kafka.connect.log.file}")
	private String kafkaConnectLogFile;

	@Value("${streamingetl.logs.dir}")
	private String streamingetlLogsDir;

	@Value("${streamingetl.common.log.archive}")
	private String streamingetlCommonLogArchive;

	@Value("${streamingetl.scheduling.log.archive}")
	private String streamingetlSchedulingLogArchive;

	@Value("${streamingetl.partycontact.logs.dir}")
	private String streamingetlPartyContactLogsDir;

	@Value("${streamingetl.partycontact.consumer.archive}")
	private String streamingetlPartycontactConsumerArchive;

	@Value("${streamingetl.partycontact.load.archive}")
	private String streamingetlPartycontactLoadArchive;

//	@Scheduled(fixedRate = 5000)
	public void reportCurrentTime() {
		log.info("{},  The time is now {}", welcomeMessage, dateFormat.format(new Date()));

		
	}
	// 每月1日23時00分00秒
//	@Scheduled(cron = "00 00 23 01 * ?")
//	@Scheduled(fixedRate = 5000)
	public void scheduleTaskUsingCronExpression() {

		long now = System.currentTimeMillis() / 1000;
		log.info("{}, schedule tasks using cron jobs -  {}", welcomeMessage, dateFormat.format(new Date()));
		
		log.info("kafkaLogsDir ={} ", kafkaLogsDir);
		log.info("kafkaControllerLogFile ={} ", kafkaControllerLogFile);
		log.info("kafkaLogcleanerLogFile ={} ", kafkaLogcleanerLogFile);
		log.info("kafkaServerLogFile ={} ", kafkaServerLogFile);
		log.info("kafkaStatechangeLogFile ={} ", kafkaStatechangeLogFile);
		log.info("kafkaConnectLogFile ={} ", kafkaConnectLogFile);
		
		log.info("streamingetlLogsDir ={} ", streamingetlLogsDir);
		log.info("streamingetlCommonLogArchive ={} ", streamingetlCommonLogArchive);
		log.info("streamingetlSchedulingLogArchive ={} ", streamingetlSchedulingLogArchive);
		
		log.info("streamingetlPartyContactLogsDir ={} ", streamingetlPartyContactLogsDir);
		log.info("streamingetlPartycontactConsumerArchive ={} ", streamingetlPartycontactConsumerArchive);
		log.info("streamingetlPartycontactLoadArchive ={} ", streamingetlPartycontactLoadArchive);

		// controller.log.2021-08-09-01
		Calendar calendar = Calendar.getInstance();
		int year = calendar.get(Calendar.YEAR);
		int month = calendar.get(Calendar.MONTH) + 1;
		int day = calendar.get(Calendar.DAY_OF_MONTH);
		int hour = calendar.get(Calendar.HOUR_OF_DAY);

		log.info("year={},month={},day={},hour={}", year,month,day,hour);

		// delete last month controller log, schedule run on Aug. 1 will delete files in Jun. 
		deleteKafkaControllerLogFile(year, month - 2);
		
		// delete last month cleaner log
		deleteKafkaLogCleanerLogFile(year, month - 2);
		
		// delete last month server log
		deleteKafkaServerLogFile(year, month - 2);
		
		// delete last month state change log
		deleteKafkaStatechangeLogFile(year, month - 2);
		
		deleteKafkaConnectLogFile(year, month - 2);
		
		// delete streamingetl common log archive
		deleteStreamingetlCommonLogArchive(year, month - 2);
		
		// delete streamingetl scheduling log archive
		deleteStreamingetlSchedulingLogArchive(year, month - 2);
		
		// delete streamingetlPartycontactConsumerArchive
		deleteStreamingetlPartycontactConsumerArchive(year, month - 2);
		
		// delete streamingetlPartycontactLoadArchive
		deleteStreamingetlPartycontactLoadArchive(year, month - 2);

	}
	private void deleteKafkaControllerLogFile(int year, int deletemonth) {
		log.info("Start deleteKafkaControllerLogFile... ");
		log.info("kafkaLogsDir={},kafkaControllerLogFile ={} ", kafkaLogsDir, kafkaControllerLogFile);

		String monthStr = StringUtils.leftPad(String.valueOf(deletemonth), 2, '0'); 
		String filePattern = "^" + kafkaControllerLogFile+"."+year+"-"+monthStr+"-"+".*";
			
		deleteFilesWithPattern(kafkaLogsDir, filePattern);
	}
	private void deleteKafkaLogCleanerLogFile(int year, int deletemonth) {
		log.info("Start deleteKafkaLogCleanerLogFile... ");
		log.info("kafkaLogsDir={},kafkaLogcleanerLogFile ={} ", kafkaLogsDir, kafkaLogcleanerLogFile);

		String monthStr = StringUtils.leftPad(String.valueOf(deletemonth), 2, '0'); 
		String filePattern = "^" + kafkaLogcleanerLogFile+"."+year+"-"+monthStr+"-"+".*";
			
		deleteFilesWithPattern(kafkaLogsDir, filePattern);
	}
	private void deleteKafkaServerLogFile(int year, int deletemonth) {
		log.info("Start deleteKafkaServerLogFile... ");
		log.info("kafkaLogsDir={},kafkaServerLogFile ={} ", kafkaLogsDir, kafkaServerLogFile);

		String monthStr = StringUtils.leftPad(String.valueOf(deletemonth), 2, '0'); 
		String filePattern = "^" + kafkaServerLogFile+"."+year+"-"+monthStr+"-"+".*";
			
		deleteFilesWithPattern(kafkaLogsDir, filePattern);
	}
	private void deleteKafkaStatechangeLogFile(int year, int deletemonth) {
		log.info("Start deleteKafkaStatechangeLogFile... ");
		log.info("kafkaLogsDir={},kafkaStatechangeLogFile ={} ", kafkaLogsDir, kafkaStatechangeLogFile);

		String monthStr = StringUtils.leftPad(String.valueOf(deletemonth), 2, '0'); 
		String filePattern = "^" + kafkaStatechangeLogFile+"."+year+"-"+monthStr+"-"+".*";
			
		deleteFilesWithPattern(kafkaLogsDir, filePattern);
	}
	
	private void deleteKafkaConnectLogFile(int year, int deletemonth) {
		log.info("Start deleteKafkaConnectLogFile... ");
		log.info("kafkaLogsDir={},kafkaConnectLogFile ={} ", kafkaLogsDir, kafkaConnectLogFile);

		String monthStr = StringUtils.leftPad(String.valueOf(deletemonth), 2, '0'); 
		String filePattern = "^" + kafkaConnectLogFile+"."+year+"-"+monthStr+"-"+".*";
			
		deleteFilesWithPattern(kafkaLogsDir, filePattern);
	}
	
	private void deleteStreamingetlCommonLogArchive(int year, int deletemonth) {
		log.info("Start deleteStreamingetlCommonLogArchive... ");
		log.info("streamingetlLogsDir={},streamingetlCommonLogArchive ={} ", streamingetlLogsDir, streamingetlCommonLogArchive);

		String monthStr = StringUtils.leftPad(String.valueOf(deletemonth), 2, '0'); 
		String filePattern = "^" + streamingetlCommonLogArchive+"."+year+"-"+monthStr+"-"+".*" + ".gz$";
			
		deleteFilesWithPattern(streamingetlLogsDir, filePattern);
	}
	
	private void deleteStreamingetlSchedulingLogArchive(int year, int deletemonth) {
		log.info("Start deleteStreamingetlSchedulingLogArchive... ");
		log.info("streamingetlLogsDir={},streamingetlSchedulingLogArchive ={} ", streamingetlLogsDir, streamingetlSchedulingLogArchive);

		String monthStr = StringUtils.leftPad(String.valueOf(deletemonth), 2, '0'); 
		String filePattern = "^" + streamingetlSchedulingLogArchive+"."+year+"-"+monthStr+"-"+".*" + ".gz$";
			
		deleteFilesWithPattern(streamingetlLogsDir, filePattern);
	}
	private void deleteStreamingetlPartycontactConsumerArchive(int year, int deletemonth) {
		log.info("Start deleteStreamingetlPartycontactConsumerArchive... ");
		log.info("streamingetlPartyContactLogsDir={},streamingetlPartycontactConsumerArchive ={} ", streamingetlPartyContactLogsDir, streamingetlPartycontactConsumerArchive);

		String monthStr = StringUtils.leftPad(String.valueOf(deletemonth), 2, '0'); 
		String filePattern = "^" + streamingetlPartycontactConsumerArchive+"."+year+"-"+monthStr+"-"+".*" + ".gz$";
			
		deleteFilesWithPattern(streamingetlPartyContactLogsDir, filePattern);
	}
	private void deleteStreamingetlPartycontactLoadArchive(int year, int deletemonth) {
		log.info("Start streamingetlPartycontactLoadArchive... ");
		log.info("streamingetlPartyContactLogsDir={},streamingetlPartycontactLoadArchive ={} ", streamingetlPartyContactLogsDir, streamingetlPartycontactLoadArchive);

		String monthStr = StringUtils.leftPad(String.valueOf(deletemonth), 2, '0'); 
		String filePattern = "^" + streamingetlPartycontactLoadArchive+"."+year+"-"+monthStr+"-"+".*" + ".gz$";
			
		deleteFilesWithPattern(streamingetlPartyContactLogsDir, filePattern);
	}
	private void deleteFilesWithPattern(String dirName, String filePattern) {

		File dir = new File(dirName);

		FileFilter fileFilter = new RegexFileFilter(filePattern);
		File[] files = dir.listFiles(fileFilter);
		for (File file : files) {
			try {
				Files.delete(Paths.get(file.getAbsolutePath()));
			} catch (IOException e) {
				log.info("message={},trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			}
		}
	}
	private void listFilesWithPattern(String dirName, String filePattern) {
		log.info("dirName= {}, filePattern={}", dirName, filePattern);
		
		File dir = new File(dirName);

		FileFilter fileFilter = new RegexFileFilter(filePattern);
		File[] files = dir.listFiles(fileFilter);
		for (File file : files) {
			log.info("file= {}", file.getAbsolutePath());
		}
	}
}
