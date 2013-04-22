/**
 * 
 */
package com.dianping.data.warehouse.canaan.parser;


import com.dianping.data.warehouse.canaan.conf.CanaanConf;
import com.dianping.data.warehouse.canaan.dolite.DOLite;

import junit.framework.TestCase;

/**
 * @author leo.chen
 * 
 */
public class TestDOLParser extends TestCase {
	public void testGetDOLite() throws Exception {
		CanaanConf conf = CanaanConf.getConf();
		
		DOLParser dolParser = new DOLParser(conf);
//		System.out.println(conf.getCanaanVariables(Constants.BATCH_COMMON_VARS.BATCH_CAL_DT.toString()));
		DOLite hqlite=dolParser.getDOLite();
		assertNotNull(hqlite);
		for(int i=1;i<=hqlite.size();i++){
			System.out.println("line"+i+":"+hqlite.get(i));
		}
		System.out.println(hqlite.getTaskId());
	}
}
