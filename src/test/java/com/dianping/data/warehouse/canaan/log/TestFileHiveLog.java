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

import java.io.IOException;

import org.apache.hadoop.hive.conf.HiveConf;

import com.dianping.data.warehouse.canaan.common.HiveCMDInfo;

import junit.framework.TestCase;

/**
 * TODO Comment of TestFileLog
 * @author yifan.cao
 *
 */
public class TestFileHiveLog extends TestCase{
		public FileHiveLog log;
		
		public TestFileHiveLog()
		{
			super();
			HiveLogConf.getConf().setLogDir(System.getProperty("user.home"));
			HiveLogConf.getConf().setLogFile("dummy");
			
			try {
				log = new FileHiveLog();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
//		public void testInit()
//		{
//			log.init("TEST START");
//		}
		
		public void testLog()
		{
			log.init("TEST START");
			HiveCMDInfo sql = new HiveCMDInfo();
			sql.setCMD("create table if not exists test(name " +
				"String,age int)\n" +
				"row format delimited\n" +
				"fields terminated by \'\\001\'\n" +
				"collection items terminated by \'\\002\'\n" +
				"map keys terminated by \'\\003\'");
			long t = System.currentTimeMillis();
			sql.setStartTime(t);
			log.log(sql,false);		
			long c = t;
			while ((c = System.currentTimeMillis())< t + 1000)
			{
			}
			sql.setFinishTime(c);
			log.log(sql,true);
			log.destroy("TEST FINISH");
		}
}