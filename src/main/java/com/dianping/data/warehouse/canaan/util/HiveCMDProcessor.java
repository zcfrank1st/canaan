/**
 * Project: canaan
 * 
 * File Created at 2012-9-24
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
package com.dianping.data.warehouse.canaan.util;

import java.io.IOException;
import java.sql.SQLException;

import com.dianping.data.warehouse.canaan.common.HiveCMDInfo;
import com.dianping.data.warehouse.canaan.driver.HiveDriver;
import com.dianping.data.warehouse.canaan.exception.HiveClientNotFoundException;
import com.dianping.data.warehouse.canaan.log.FileHiveLog;
import com.dianping.data.warehouse.canaan.log.MySQLHiveLog;

/**
 * TODO Comment of Executor
 * 
 * @author yifan.cao
 * 
 */
@Deprecated
public class HiveCMDProcessor {
	private FileHiveLog fileHiveLog;
	private MySQLHiveLog mysqlHiveLog;
	private HiveDriver driver;

	public HiveCMDInfo process(HiveCMDInfo hci) throws InterruptedException, IOException, ClassNotFoundException, SQLException {
		hci.setStartTime(System.currentTimeMillis());
		mysqlHiveLog.init("");
		fileHiveLog.log(hci, false);
		mysqlHiveLog.log(hci, false);
		//HiveCMDInfo retInfo = driver.execute(null);
		hci.setFinishTime(System.currentTimeMillis());
		hci.setJobTotalInfo();
		mysqlHiveLog.log(hci, true);
		fileHiveLog.log(hci, true);
		mysqlHiveLog.destroy("");
		return null;//retInfo;
	}

	
	public HiveCMDInfo process(String cmd) throws InterruptedException, IOException, ClassNotFoundException, SQLException {
		HiveCMDInfo hci = new HiveCMDInfo();
		hci.setCMD(cmd);
		return process(hci);
	}
	
	
	public HiveCMDProcessor() throws IOException, ClassNotFoundException, SQLException, HiveClientNotFoundException {
		this.fileHiveLog = new FileHiveLog();
		this.mysqlHiveLog = new MySQLHiveLog();
		
		this.driver = new HiveDriver();
	}
	
	/**
	 * @param cliPath
	 * @throws IOException 
	 * @throws HiveClientNotFoundException 
	 */
	public HiveCMDProcessor(String cliPath) throws IOException, HiveClientNotFoundException {
		this.fileHiveLog = new FileHiveLog();
		this.driver = new HiveDriver();
		this.setHiveClientPath(cliPath);
	}


	public void setHiveClientPath(String cliPath) throws HiveClientNotFoundException
	{
		driver.setHiveClientPath(cliPath);
	}
}