/**
 * Project: canaan
 * 
 * File Created at Oct 10, 2012
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
package com.dianping.data.warehouse.canaan.conf;

import java.io.IOException;

import com.dianping.data.warehouse.canaan.common.Constants;
import junit.framework.TestCase;

/**
 * TODO Comment of TestCanaanConf
 * 
 * @author stephensonz
 * 
 */
public class TestCanaanConf extends TestCase {
	public void testDefaultConf() throws Exception {

		CanaanConf cc = CanaanConf.getConf();
		System.out.println(Constants.DEFAULT_CONF_NAME + ":");
		System.out.println(cc.printDefaultConfVariables());

		for (Constants.BATCH_COMMON_VARS s : Constants.BATCH_COMMON_VARS.values()) {
			System.out.println(s.toString() + ":\t" + cc.getCanaanVariables(s.toString()));
		}

//		assertEquals(cc.getCanaanVariables("login_name"), "dwdev");
//		assertEquals(cc.getCanaanVariables("login_pwd"), "password");
//		assertEquals(cc.getCanaanVariables("BATCH_CACULATOR"), "com.dianping.data.warehouse.canaan.util.DWCalculator");
//		assertEquals(cc.getCanaanVariables("BATCH_SERVER"), "HIVE_TEST");
//		assertEquals(cc.getCanaanVariables("BATCH_USER"), "DWDEV");
//		assertEquals(cc.getCanaanVariables("BATCH_DOL"), "dwdev.tg.dpmid_tg_test");
	}

	public void testGetCalVariables() throws Exception {

		CanaanConf cc = CanaanConf.getConf();

		for (Constants.BATCH_CAL_VARS k : Constants.BATCH_CAL_VARS.values()) {
			String v = cc.getCalVariables(k.toString());
			System.out.println(k + ": " + v);
		}

	}

	public void testGetExtParam() throws Exception {	
		CanaanConf cc = CanaanConf.getConf();
		cc.getExtParamKeys();
    }

    public void testGetTmpPath() throws IOException {
        CanaanConf cc = CanaanConf.getConf();
        System.out.println(cc.getHiveTmpPath());
    }
}
