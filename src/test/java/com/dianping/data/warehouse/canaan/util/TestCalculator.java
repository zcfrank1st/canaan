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

import java.util.HashMap;
import java.util.Map;

/**
 * TODO Comment of Calculator
 * @author yifan.cao
 *
 */
public class TestCalculator {
	public static void main(String[] args){
		Map<String,Integer> m = new HashMap<String,Integer>();
		m.put("22", 2);
		System.out.println(m.size());
		m.remove("22");
		System.out.println(m.size());
	}
}
