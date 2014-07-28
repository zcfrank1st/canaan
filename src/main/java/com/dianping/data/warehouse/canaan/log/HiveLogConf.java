/**
 * Project: canaan
 * 
 * File Created at 2012-10-10
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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.dianping.data.warehouse.canaan.common.Constants;
import com.dianping.data.warehouse.canaan.conf.CanaanConf;

/**
 * TODO Comment of HiveLogConf
 * @author yifan.cao
 *
 */
public class HiveLogConf {
	private static HiveLogConf conf = new HiveLogConf();
	
	// Common HiveLogConf
	private final static SimpleDateFormat ddf = Constants.DAY_DF;
	private String caldate;
	private String taskname;
	private int taskid;
	private String currentkillcmd;
	
	// HiveLogConf for file
	private String logDir;
	private String logFile;
	
	
	// HiveLogConf for mysql
	private Map<String,String> mysqlConnectParams = new HashMap<String, String>();
	private int currentlogid;

	private int status = Constants.RET_SUCCESS;
	

	public static HiveLogConf getConf() {
		return conf;
	}
	
	private HiveLogConf()
	{
		caldate = "1949-10-01";
		taskid = -999;
		taskname = "dummy";
	}
	
	public String getLogPath()
	{
		String today = ddf.format(new Date());
		return ((getLogDir().trim().endsWith("/") || getLogDir().trim().endsWith("\\")) ? getLogDir() + getLogFile() : getLogDir()
				+ "/" + getLogFile())
				+ '.' + today;
	}

	/**
	 * @param logDir the logDir to set
	 */
	public void setLogDir(String logDir) {
		this.logDir = logDir;
	}

	/**
	 * @return the logDir
	 */
	public String getLogDir() {
		return logDir;
	}

	/**
	 * @param logFile the logFile to set
	 */
	public void setLogFile(String logFile) {
		this.logFile = logFile;
	}

	/**
	 * @return the logFile
	 */
	public String getLogFile() {
		return logFile;
	}

	public Map<String,String> getMysqlConnectParams() {
		return mysqlConnectParams;
	}

	public void setMysqlConnectParams(Map<String,String> mysqlConnectParams) {
		this.mysqlConnectParams = mysqlConnectParams;
	}
	
	public String getMysqlConnectParam(String key) {
		return mysqlConnectParams.get(key);
	}
	
	public void setMysqlConnectParam(String key,String value) {
			this.mysqlConnectParams.put(key, value);
	}

	public String getTaskname() {
		return taskname;
	}

	public void setTaskname(String taskname) {
		this.taskname = taskname;
	}

	public int getTaskid() {
		return taskid;
	}
	
	public void setTaskid(String taskid) {
		int temp = -999;
		try 
		{temp = Integer.parseInt(taskid);
		}
		catch (NumberFormatException e)
		{
			this.taskid =  -999;
		}
		this.taskid = temp;
	}
	

	public void setTaskid(int taskid) {
		this.taskid = taskid;
	}

	public String getCaldate() {
		return caldate;
	}

	public void setCaldate(String caldate) {
		this.caldate = caldate;
	}

	public long getCurrentlogid() {
		return currentlogid;
	}

	public void setCurrentlogid(int currentlogid) {
		this.currentlogid = currentlogid;
	}

	public String getCurrentkillcmd() {
		return currentkillcmd;
	}

	public void setCurrentkillcmd(String currentkillcmd) {
		this.currentkillcmd = currentkillcmd;
	}

	public void setStatus(int status) {
		this.status = status;
	}
	
	public int getStatus() {
		return this.status;
	}
}
