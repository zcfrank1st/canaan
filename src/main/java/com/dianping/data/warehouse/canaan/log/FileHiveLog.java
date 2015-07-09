/**
 * Project: test2
 * 
 * File Created at 2012-9-26
 * $Id$
 * 
 * Copyright 2010 dianping.com.
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * Dianping Company. ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with dianping.com.
 */
package com.dianping.data.warehouse.canaan.log;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.dianping.data.warehouse.canaan.common.Constants;
import com.dianping.data.warehouse.canaan.common.HiveCMDInfo;
import com.dianping.data.warehouse.canaan.common.HiveJobInfo;
import org.apache.log4j.Logger;

/**
 * TODO Comment of FileLog
 * 
 * @author yifan.cao
 * 
 */
public class FileHiveLog implements HiveLog {
	// private final static SimpleDateFormat sdf = Constants.SHORT_DF;
	public static Logger logger= null;
	private final static SimpleDateFormat ldf = Constants.LONG_DF;
	private final static HiveLogConf conf = HiveLogConf.getConf();
	private String logPath;
	private File file;
	private OutputStream outputStream;
	private OutputStreamWriter outputStreamWriter;
	private BufferedWriter bufferedWriter;

	protected void finalize() throws IOException {
		bufferedWriter.close();
		outputStreamWriter.close();
	}

	public FileHiveLog() throws IOException {
		logger = Logger.getLogger(FileHiveLog.class);
		logger.info("FileHiveLog init ,to get hive log conf");
		HiveLogConf conf = HiveLogConf.getConf();
		if (conf.getLogPath() != null) {
			logger.info("hive log path is valid,the path is " + conf.getLogPath());
		}
		this.logPath = conf.getLogPath();
		this.file = new File(logPath);

		// check the file existence or create the file
		if(file.createNewFile()) {
			logger.info("create hive log file sucess");
		} else {
			logger.info("create hive log file failed");
		};
		logger.info("hello");

        file.setWritable(true,false);

		// outputstream initialization
		this.outputStream = new FileOutputStream(this.file, true);
		this.outputStreamWriter = new OutputStreamWriter(this.outputStream);
		this.bufferedWriter = new BufferedWriter(this.outputStreamWriter);
	}

	public void write(String s) {
		if ((file.canWrite()) && (bufferedWriter != null) && (s != null)) {
			try {
				bufferedWriter.write(s);
				bufferedWriter.newLine();
				bufferedWriter.flush();
				// bufferedWriter.close();
				// outputStreamWriter.close();
			} catch (IOException e) {
				System.err
						.println("Following information cannot be written into file "
								+ this.logPath);
				System.err.println(s);
				e.printStackTrace();
			}
		} else {
			System.err
					.println("Following information cannot be written into file "
							+ this.logPath);
			System.err.println(s);
		}
	}

	@Override
	public void destroy(String s) {
		this.write("Task: " + conf.getTaskname() + (conf.getTaskid()==-999?"":"("+conf.getTaskid()+")"));
		this.write("Task Finish Time: " + ldf.format(new Date()));
		this.writeEmptyLine();
	}

	/**
	 * override
	 * 
	 * @see com.dianping.data.warehouse.canaan.log.HiveLog#init(java.lang.String)
	 */
	@Override
	public void init(String s) {
		this.write("Task: " + conf.getTaskname() + (conf.getTaskid()==-999?"":"("+conf.getTaskid()+")"));
		this.write("Task Start Time: " + ldf.format(new Date()));
		this.writeEmptyLine();
	}

	/*
	 * Write an empty line
	 */
	public void writeEmptyLine() {
		this.write("");
	}

	/*
	 * Write a delimited line
	 */
	public void writeDelimitedLine() {
		this.write("===============================================");
	}

	@Override
	public void log(HiveCMDInfo sqlInfo, boolean isUpdate) {
		String logContent = "";
		if (sqlInfo.getIsSQL() == 1) {
			// log for sql
			if (!isUpdate) {
				// log for start
				this.writeDelimitedLine();
				logContent = "SQL Start Time: "
						+ ldf.format(new Date(sqlInfo.getStartTime()))
						+ "\nSQL: " + sqlInfo.getCMD(); 
//						+ "\nSource Tables: "
//						+ sqlInfo.getSourceTableList().toString()
//						+ "\nTarget Tables: "
//						+ sqlInfo.getTargetTableList().toString();
				this.write(logContent);
				// this.write("SQL Start Time: " + ldf.format(new
				// Date(sqlInfo.getStartTime())));
				// this.write("SQL: " + sqlInfo.getSQL());
				// this.write("Job Number: " + sqlInfo.getJobNumber());
				// this.write("Source Tables: " +
				// sqlInfo.getSourceTableList().toString());
				// this.write("Target Tables: " +
				// sqlInfo.getTargetTableList().toString());
			} else {
				// log for end or update
				logContent = "SQL Finish Time: "
						+ ldf.format(new Date(sqlInfo.getFinishTime()))
						+ "\nEnd Status: " + sqlInfo.getStatus()
						+ "\nEnd Info: "
						+ ((sqlInfo.getStatus() == 0) ? "Success\n"
								: (sqlInfo.getExitinfo()))
						+ "Job Number: " + sqlInfo.getJobNumber()
						+ "\nMapper Number: " + sqlInfo.getMapperNumber()
						+ "\nReducer Number: " + sqlInfo.getReducerNumber()
						+ "\nTotal MapReduce CPU Time Spent: "
						+ sqlInfo.getCPUTime() + " ms" + "\nHDFS Read: "
						+ sqlInfo.getHDFSRead() + "\nHDFS Write: "
						+ sqlInfo.getHDFSWrite() + "\nTotal Time Taken: "
						+ (sqlInfo.getFinishTime() - sqlInfo.getStartTime())
						+ " ms";
				this.write(logContent);
				// this.write("SQL Finish Time: " + ldf.format(new
				// Date(sqlInfo.getFinishTime())));
				// this.write("End Status: " + sqlInfo.getStatus());
				// this.write("Mapper Number: " + sqlInfo.getMapperNumber());
				// this.write("Reducer Number: " + sqlInfo.getReducerNumber());
				// this.write("Total MapReduce CPU Time Spent: " +
				// sqlInfo.getCPUTime() + " ms");
				// this.write("HDFS Read: " + sqlInfo.getHDFSRead());
				// this.write("HDFS Write: " + sqlInfo.getHDFSWrite());
				// this.write("Total Time Taken: " + (sqlInfo.getFinishTime() -
				// sqlInfo.getStartTime()) + " ms");
				this.writeDelimitedLine();
			}
			this.writeEmptyLine();
		} else {
			// log for hive cmd
			if (!isUpdate) {
				this.writeDelimitedLine();
				logContent = "CMD Start Time: "
						+ ldf.format(new Date(sqlInfo.getStartTime()))
						+ "\nCMD: " + sqlInfo.getCMD();
				this.write(logContent);
				// this.write("SQL Start Time: " + ldf.format(new
				// Date(sqlInfo.getStartTime())));
				// this.write("SQL: " + sqlInfo.getSQL());
				// this.write("Job Number: " + sqlInfo.getJobNumber());
				// this.write("Source Tables: " +
				// sqlInfo.getSourceTableList().toString());
				// this.write("Target Tables: " +
				// sqlInfo.getTargetTableList().toString());
			} else {
				logContent = "CMD Finish Time: "
						+ ldf.format(new Date(sqlInfo.getFinishTime()))
						+ "\nEnd Status: " + sqlInfo.getStatus();
				this.write(logContent);
				// this.write("SQL Finish Time: " + ldf.format(new
				// Date(sqlInfo.getFinishTime())));
				// this.write("End Status: " + sqlInfo.getStatus());
				// this.write("Mapper Number: " + sqlInfo.getMapperNumber());
				// this.write("Reducer Number: " + sqlInfo.getReducerNumber());
				// this.write("Total MapReduce CPU Time Spent: " +
				// sqlInfo.getCPUTime() + " ms");
				// this.write("HDFS Read: " + sqlInfo.getHDFSRead());
				// this.write("HDFS Write: " + sqlInfo.getHDFSWrite());
				// this.write("Total Time Taken: " + (sqlInfo.getFinishTime() -
				// sqlInfo.getStartTime()) + " ms");
				this.writeDelimitedLine();
			}
			this.writeEmptyLine();
		}
	}

	@Override
	public void log(HiveJobInfo content, int stage) throws Exception {
		
	}
}