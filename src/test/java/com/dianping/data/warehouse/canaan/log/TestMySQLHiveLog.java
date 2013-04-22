package com.dianping.data.warehouse.canaan.log;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Date;

import com.dianping.data.warehouse.canaan.common.HiveCMDInfo;
import com.dianping.data.warehouse.canaan.conf.CanaanConf;

import junit.framework.TestCase;

public class TestMySQLHiveLog extends TestCase {
	public void testLog() throws ClassNotFoundException, SQLException, FileNotFoundException, IOException
	{
		CanaanConf canaanConf = CanaanConf.getConf();
		canaanConf.loadHiveLogConf();
		
		MySQLHiveLog log = new MySQLHiveLog();
		HiveCMDInfo hiveCI = new HiveCMDInfo("test");
		log.init("");
		
		hiveCI.setStartTime(System.currentTimeMillis());
		log.log(hiveCI, false);
		//System.out.println(new Date(hiveCI.getStartTime()));
		
		System.out.println("AUTO_INCREASED KEY: " + log.getLogid());
		
		hiveCI.setFinishTime(System.currentTimeMillis());
		log.log(hiveCI, true);
				
		log.destroy("");
	}
}
