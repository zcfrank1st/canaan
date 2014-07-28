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

import java.io.IOException;
import java.util.ArrayList;

import com.dianping.data.warehouse.canaan.common.HiveCMDInfo;
import com.dianping.data.warehouse.canaan.dolite.DOLite;
import com.dianping.data.warehouse.canaan.driver.Driver;
import com.dianping.data.warehouse.canaan.driver.HiveDriver;
import com.dianping.data.warehouse.canaan.exception.HiveClientNotFoundException;
import com.dianping.data.warehouse.canaan.exception.HiveInitFileNotFoundException;

/**
 * TODO Comment of Calculator
 * @author yifan.cao
 *
 */
public abstract class Calculator {
    protected Driver driver;

    public abstract int calculate(DOLite dolite)
            throws Exception;

    /**
     * @param cliPath
     * @param initPath
     * @param hiveconf
     * @param tmppath
     * @throws IOException
     * @throws HiveInitFileNotFoundException
     */
    public void initHive(String cliPath,String initPath,String hiveconf,String tmppath) throws HiveClientNotFoundException, IOException, HiveInitFileNotFoundException
    {
        this.driver = new HiveDriver(cliPath,initPath,hiveconf,tmppath);
    }
}