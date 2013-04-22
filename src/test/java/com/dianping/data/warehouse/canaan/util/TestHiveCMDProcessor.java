package com.dianping.data.warehouse.canaan.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;

import junit.framework.TestCase;

import com.dianping.data.warehouse.canaan.conf.CanaanConf;

public class TestHiveCMDProcessor extends TestCase {
	public void testProcess() throws FileNotFoundException, IOException, ClassNotFoundException, SQLException, InterruptedException
	{
		CanaanConf canaanConf = CanaanConf.getConf();
		canaanConf.loadHiveLogConf();
		String cmd = "select count(1) from bi.test_a;";
		//HiveCMDProcessor processor = new HiveCMDProcessor();
		//processor.process(cmd);
	}
}