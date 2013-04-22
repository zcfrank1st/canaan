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
package com.dianping.data.warehouse.canaan.driver;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;

import com.dianping.data.warehouse.canaan.common.Constants;
import junit.framework.TestCase;

import com.dianping.data.warehouse.canaan.common.HiveCMDInfo;
import com.dianping.data.warehouse.canaan.common.HiveJobInfo;
import com.dianping.data.warehouse.canaan.conf.CanaanConf;
import com.dianping.data.warehouse.canaan.exception.HiveClientNotFoundException;

/**
 * TODO Comment of CMDDriver
 * 
 * @author yifan.cao
 * 
 */
public class TestHiveDriver extends TestCase {
	public static HiveCMDInfo c0 = new HiveCMDInfo();


	public static HiveDriver driver;

	public TestHiveDriver() {
		c0.setCMD("show tables;");
	}

	public void testExecute() throws InterruptedException, IOException, HiveClientNotFoundException, ClassNotFoundException, SQLException {
		// driver.execute(c0);
		// driver.execute(c1);
		// driver.execute(c2);
		// driver.execute(c3);
		//CanaanConf canaanConf = CanaanConf.getConf();
		//canaanConf.loadHiveLogConf();
		//HiveLogConf conf = canaanConf.getHiveLogConf();
		CanaanConf.getConf().loadHiveLogConf();
		driver = new HiveDriver();
		
		ArrayList<HiveCMDInfo> list = new ArrayList<HiveCMDInfo>();
		list.add(c0);
		driver.execute(list);
		
		for (HiveJobInfo hiveJI : c0.getJobList()) {
			System.out.println("JOB_ID: " + hiveJI.getJobID());
			System.out.println("JOB_TRACKING_URL: "
					+ hiveJI.getJobTrackingURL());
			System.out.println("JOB_KILL_CMD: " + hiveJI.getJobKillCMD());
			System.out.println("JOB_START_TIME: "
					+ Constants.LONG_DF.format(new Date(hiveJI
							.getJobStartTime())));
			System.out.println("JOB_CPU_TIME: " + hiveJI.getCPUTime());
			System.out.println("JOB_FINISH_TIME: "
					+ Constants.LONG_DF.format(new Date(hiveJI
							.getJobFinishTime())));
			System.out
					.println("JOB_MAPPER_NUMBER: " + hiveJI.getMapperNumber());
			System.out.println("JOB_REDUCER_NUMBER: "
					+ hiveJI.getReducerNumber());
			System.out.println("JOB_HDFS_READ: " + hiveJI.getHDFSRead());
			System.out.println("JOB_HDFS_WRITE: " + hiveJI.getHDFSWrite());
			System.out.println("JOB_EXIT_CODE: " + hiveJI.getStatus());
		}
		// System.out.println(c3.getCPUTime());
	}
}
