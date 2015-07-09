package com.dianping.data.warehouse.canaan.util;

import java.util.ArrayList;
import java.util.List;

import com.dianping.data.warehouse.canaan.common.Constants;
import com.dianping.data.warehouse.canaan.conf.CanaanConf;
import com.dianping.data.warehouse.canaan.dolite.DOLite;
import com.dianping.data.warehouse.canaan.dolite.DOLiteImpl;
import com.dianping.data.warehouse.canaan.log.HiveLogConf;

import junit.framework.TestCase;

public class TestDWCalculator extends TestCase {
    static List<String> stringList = new ArrayList<String>();
    static DOLite doLite = new DOLiteImpl("test_b", stringList);

    public void testCalculate() throws Exception {

        // stringList.add("create database bi;");
        stringList.add("show tables;");
        HiveLogConf.getConf().setLogDir(System.getProperty("user.home"));
        HiveLogConf.getConf().setLogFile("dummy");
        DWCalculator calculator = new DWCalculator();
        calculator.initHive(
                CanaanConf.getConf().getCanaanVariables(
                        Constants.BATCH_COMMON_VARS.BATCH_HIVE_CLIENT
                                .toString()),
                CanaanConf.getConf().getCanaanVariables(
                        Constants.BATCH_COMMON_VARS.BATCH_HIVE_INIT_DIR
                                .toString()),
                Constants.BATCH_COMMON_VARS.CANAAN_HOME.toString() + "=" + CanaanConf.getConf().getCanaanVariables(Constants.BATCH_COMMON_VARS.CANAAN_HOME.toString())
                , CanaanConf.getConf().getHiveTmpPath());
        calculator.calculate(doLite);
    }
}
