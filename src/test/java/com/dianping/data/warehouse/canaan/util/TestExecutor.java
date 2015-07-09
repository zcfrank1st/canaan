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

import java.util.ArrayList;
import java.util.List;

import com.dianping.data.warehouse.canaan.common.Constants;
import junit.framework.TestCase;

import com.dianping.data.warehouse.canaan.conf.CanaanConf;
import com.dianping.data.warehouse.canaan.dolite.DOLite;
import com.dianping.data.warehouse.canaan.dolite.DOLiteImpl;
import com.dianping.data.warehouse.canaan.log.HiveLogConf;

/**
 * TODO Comment of Executor
 * 
 * @author yifan.cao
 * 
 */
public class TestExecutor extends TestCase {
	static List<String> stringList = new ArrayList<String>();
	static DOLite doLite = new DOLiteImpl("test_c", stringList);

	public void testExecute() throws Exception {
		// stringList.add("create database bi;");
		stringList.add("show tables;");
		String logDir = CanaanConf.getConf().getCanaanVariables(Constants.BATCH_COMMON_VARS.BATCH_LOG_DIR.toString());
		System.out.println(logDir);
		HiveLogConf.getConf().setLogDir(System.getProperty("user.home"));
		HiveLogConf.getConf().setLogFile("dummy");
		
		DWCalculator calculator = new DWCalculator();
		calculator.initHive(
				CanaanConf.getConf().getCanaanVariables(
						Constants.BATCH_COMMON_VARS.BATCH_HIVE_CLIENT
								.toString()),
				CanaanConf.getConf().getCanaanVariables(
						Constants.BATCH_COMMON_VARS.BATCH_HIVE_INIT_DIR
								.toString()),
				Constants.BATCH_COMMON_VARS.CANAAN_HOME.toString() +"=" + CanaanConf.getConf().getCanaanVariables(Constants.BATCH_COMMON_VARS.CANAAN_HOME.toString())
        ,CanaanConf.getConf().getHiveTmpPath());
		Executor executor = new Executor(calculator);
		executor.setDOLite(doLite);
		executor.execute();
	}
}
