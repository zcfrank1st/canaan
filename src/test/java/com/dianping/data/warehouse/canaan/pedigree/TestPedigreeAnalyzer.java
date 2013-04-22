package com.dianping.data.warehouse.canaan.pedigree;
/**
 * Project: canaan
 * 
 * File Created at Nov 19, 2012
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
import com.dianping.data.warehouse.canaan.pedigree.AnalyzeResult;

import com.dianping.data.warehouse.canaan.pedigree.PedigreeAnalyzer;

import junit.framework.TestCase;

/**
 * TODO Comment of TestPedigreeAnalyzer
 * 
 * @author stephensonz
 * 
 */
public class TestPedigreeAnalyzer extends TestCase {

	public void testPrintTree() throws Exception {

		PedigreeAnalyzer p = PedigreeAnalyzer.getAnalyzer();

		AnalyzeResult pr = p
				.analyze("insert into table bi.tablexx select * from table_a a join table_b b on (a.id=b.id) join table_c c on (b.name=c.name)");

		System.out.println(pr.toString());
		
	}
};
