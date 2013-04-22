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
package com.dianping.data.warehouse.canaan.util;

import junit.framework.TestCase;

/**
 * TODO Comment of TestDateUtils
 * @author stephensonz
 *
 */
public class TestDateUtils  extends TestCase {
	String ln = "============================================================================================================";
	public void testformatDateString() throws Exception {
		System.out.println(ln);
		assertEquals(DateUtils.getFormatDateString("20121201"),"2012-12-01");
		assertEquals(DateUtils.getFormatDateString("2012/12/01"),"2012-12-01");
		assertEquals(DateUtils.getFormatDateString("2012-12-01"),"2012-12-01");
		assertEquals(DateUtils.getFormatDateString("2012M12-01"),"2012-12-01");
		assertEquals(DateUtils.getFormatDateString("2012-12.01"),"2012-12-01");
		try {
			DateUtils.getFormatDateString("201212");
			assertEquals(true,false);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			assertEquals(true,true);
		}
		System.out.println(ln);
    }
}
