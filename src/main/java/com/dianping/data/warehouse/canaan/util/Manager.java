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

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.dianping.data.warehouse.canaan.common.Constants;
import com.dianping.data.warehouse.canaan.common.HiveCMDInfo;
import com.dianping.data.warehouse.canaan.exception.ExceptionHandler;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.log4j.Logger;

import com.dianping.data.warehouse.canaan.conf.CanaanConf;
import com.dianping.data.warehouse.canaan.dolite.DOLite;
import com.dianping.data.warehouse.canaan.parser.DOLParser;
import com.dianping.data.warehouse.canaan.parser.OptionParser;
import com.dianping.data.warehouse.canaan.parser.SQLParser;

/**
 * Main Body Of Canaan
 *
 * @author yifan.cao
 */
public class Manager {
    public static Logger logger = null;
    private CanaanConf conf;
    private String[] args;
    private ExceptionHandler eh = null;

    public Manager(String[] args) {
        this.args = args.clone();
    }

    private void init() throws IOException {
        conf = CanaanConf.getConf();
        eh = new ExceptionHandler();
    }

    public int handle(Exception e) {
        return this.eh.handle(e);
    }

    private Properties parseOption() throws Exception {
        Map<String, String> map = new HashMap<String, String>();
        OptionParser.process(this.args, map);
        // load variables
        conf.loadOptionVariables(map);

        // load necessary variables from canaanconf
        conf.loadHiveLogConf();

        // set trace path
        String traceHome = conf.getCanaanVariables(Constants.BATCH_COMMON_VARS.CANAAN_HOME
                .toString()) + File.separator + "trace";
        String traceFileName = conf
                .getCanaanVariables(Constants.BATCH_COMMON_VARS.BATCH_DOL
                        .toString());
        System.setProperty("trace.home", traceHome);
        System.setProperty("trace.filename", traceFileName);

        logger = Logger.getLogger(Manager.class);
        eh.initLogger();
        logger.info("Task Begins");
        File logFile = new File(traceHome + File.separator + traceFileName + ".log");
        logFile.setWritable(true, false);
        return conf.getCanaanProperties();
    }

    private void parseDOL(boolean isPrint) throws Exception {
        logger.info("DOLParser Begins");

        DOLParser dolParser = new DOLParser(conf);
        DOLite dolite = dolParser.getDOLite();
        for (int i = 1; i <= dolite.size(); i++) {
            logger.info("line" + i + ":" + dolite.get(i).trim());
            if (isPrint)
                System.out.println(dolite.get(i).trim() + ";");
        }
        conf.setDOLite(dolite);
        logger.info("DOLParser Ends");
    }

    private void parseSQL() throws ParseException, ClassNotFoundException, IOException, SQLException {
        logger.info("SQLParser Begins");
        SQLParser sqlParser = new SQLParser(conf.getDOLite());
        //得到上下游关系和hive sql，并且持久化到AnalyzeResult
        sqlParser.parse();
        //将上下游关系插入数据库
        sqlParser.persist();
        logger.info(sqlParser.toString());
        logger.info("SQLParser Ends");
    }

    private int execute() throws Exception {
        logger.info("Executor Begins");
        String classType = conf
                .getCanaanVariables(Constants.BATCH_COMMON_VARS.BATCH_CACULATOR
                        .toString());
        Class<?> CalculatorClass = Class.forName(classType);
        Calculator calculator = (Calculator) CalculatorClass.newInstance();
        calculator
                .initHive(
                        conf.getCanaanVariables(Constants.BATCH_COMMON_VARS.BATCH_HIVE_CLIENT
                                .toString()),
                        conf.getCanaanVariables(Constants.BATCH_COMMON_VARS.BATCH_HIVE_INIT_DIR
                                .toString()),
                        Constants.BATCH_COMMON_VARS.CANAAN_HOME.toString() + "=" + conf.getCanaanVariables(Constants.BATCH_COMMON_VARS.CANAAN_HOME.toString())
                        , conf.getHiveTmpPath()
                );
        Executor executor = new Executor(calculator);
        executor.setDOLite(conf.getDOLite());
        int retCode = executor.execute();
        logger.info("Executor Ends");
        return retCode;
    }

    private void destory() {
        logger.info("Task Ends");
    }

    public int run() throws Exception {
        init();
        Properties p = parseOption();
        boolean parseOnly = p.getProperty(Constants.BATCH_COMMON_VARS.BATCH_PARSE_ONLY.toString()).equals("T");
        parseDOL(parseOnly);
        parseSQL();
        int status = 0;
        if (!parseOnly)
            status = execute();
        destory();
        return status;
    }

    public void deleteTmpFile() {
        if (conf.getHiveTmpPath() != null) {
            File file = new File(conf.getHiveTmpPath());
            if (file.exists()) {
                file.delete();
            }
        }
    }

    public static void main(String[] args) {
        Manager m = new Manager(args);
        int exitCode = Constants.RET_FAILED;
        try {
            exitCode = m.run();
        } catch (Exception e) {
            exitCode = m.handle(e);
        } finally {
            m.deleteTmpFile();
        }
        System.exit(exitCode);
        System.currentTimeMillis();
    }
}
