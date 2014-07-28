/**
 * Project: test
 *
 * File Created at 2012-9-27
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.dianping.data.warehouse.canaan.common.HiveCMDInfo;
import com.dianping.data.warehouse.canaan.dolite.DOLite;

/**
 *
 * @author yifan.cao
 *
 */
public class DWCalculator extends Calculator {

    public DWCalculator() throws IOException {
    }

    /**
     * Default calculator for Dianping data warehouser developer
     *
     * @see com.dianping.data.warehouse.canaan.util.Calculator#calculate()
     */
    @Override
    public int calculate(DOLite doLite) throws Exception {  	
        Logger log = Logger.getLogger(DWCalculator.class);
        String sql;
        log.info("DWCaculator is running");
        ArrayList<HiveCMDInfo> list = new ArrayList<HiveCMDInfo>();
        for (Iterator<String> l = doLite.iterator(); l.hasNext();) {
            sql = l.next().trim();
            if((!sql.endsWith(";"))&&(sql.length()!=0))
                sql += ";";
            list.add(new HiveCMDInfo(sql));
        }       
        return super.driver.execute(list);
    }
}