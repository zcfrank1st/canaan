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

import com.dianping.data.warehouse.canaan.common.Constants;
import com.dianping.data.warehouse.canaan.exception.CanaanPrintHelpException;

import junit.framework.TestCase;

/**
 * TODO Comment of Manager
 * @author yifan.cao
 *
 */
public class TestManager extends TestCase{
	public void testMain()
	{
		String[] args = //"-h".split(" ");
				"-dol dummy".split(" ");
        Manager m = new Manager(args);
        int exitCode = Constants.RET_FAILED;
        try {
            exitCode = m.run();
        } catch (Exception e) {
            exitCode = m.handle(e);
        } finally {
            m.deleteTmpFile();
        }
		assertEquals(0,exitCode);
		//Manager.main("-c com.dianping.data.warehouse.canaan.util.DWCalculator -dol cyf_test.dol -d20120902 -s5 -u dwdev -Eparam1=val1 -Eparam2=val2".split(" "));
	}
}
