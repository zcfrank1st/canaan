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
package com.dianping.data.warehouse.canaan.parser;


import com.dianping.data.warehouse.canaan.conf.CanaanConf;
import com.dianping.data.warehouse.canaan.dolite.DOLite;

import junit.framework.TestCase;

/**
 * TODO Comment of SQLParser
 * @author leo.chen
 *
 */
public class TestSQLParser extends TestCase{
	public void testParse() throws Exception{
		CanaanConf conf = CanaanConf.getConf();
//		
//		DOLParser dolParser = new DOLParser(conf);
//		DOLite hqlite=dolParser.getDOLite();
//		assertNotNull(hqlite);
//		SQLParser sqlParser=new SQLParser(hqlite);
		SQLParser sqlParser=new SQLParser("insert into table table_c select * from table_b");
		sqlParser.parse();
		System.out.println(sqlParser.toString());
		sqlParser.persist();

		
	}

}
