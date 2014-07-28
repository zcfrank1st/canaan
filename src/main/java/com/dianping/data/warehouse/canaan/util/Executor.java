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
import java.sql.SQLException;
import java.util.ArrayList;

import com.dianping.data.warehouse.canaan.common.HiveCMDInfo;
import com.dianping.data.warehouse.canaan.dolite.DOLite;
import com.dianping.data.warehouse.canaan.exception.HiveClientNotFoundException;
import com.dianping.data.warehouse.canaan.log.FileHiveLog;

/**
 * TODO Comment of SQLExecutor
 *
 * @author yifan.cao
 *
 */
public class Executor {
    private Calculator calculator;
    private static FileHiveLog fileHiveLog;
    private DOLite doLite = null;

    public Executor() throws IOException, ClassNotFoundException, SQLException, HiveClientNotFoundException {
        fileHiveLog = new FileHiveLog();
        // this.setCalculator("com.dianping.data.warehouse.canaan.util.DWCalculator");
    }

    public Executor(String classname) throws IOException,
            ClassNotFoundException, InstantiationException,
            IllegalAccessException {
        fileHiveLog = new FileHiveLog();
        this.setCalculator(classname);
    }

    public Executor(Calculator calculator) throws IOException {
        fileHiveLog = new FileHiveLog();
        this.setCalculator(calculator);
    }

    public int execute() throws Exception {
        fileHiveLog.init("");
        int retCode = calculator.calculate(doLite);
        fileHiveLog.destroy("");
        return retCode;
    }

    public Calculator getCalculator() {
        return calculator;
    }

    public void setCalculator(String calculatorClassName)
            throws ClassNotFoundException, InstantiationException,
            IllegalAccessException {
        Class<?> CalculatorClass = Class.forName(calculatorClassName);
        this.calculator = (Calculator) CalculatorClass.newInstance();
    }

    public void setCalculator(Calculator calculator) {
        this.calculator = calculator;
    }

    public DOLite getDOLite() {
        return doLite;
    }

    public void setDOLite(DOLite doLite) {
        this.doLite = doLite;
    }
}
