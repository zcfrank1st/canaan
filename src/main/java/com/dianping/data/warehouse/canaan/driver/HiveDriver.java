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
package com.dianping.data.warehouse.canaan.driver;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;

import com.dianping.data.warehouse.canaan.common.Constants;
import com.dianping.data.warehouse.canaan.common.HiveCMDInfo;
import com.dianping.data.warehouse.canaan.exception.HiveClientNotFoundException;
import com.dianping.data.warehouse.canaan.exception.HiveInitFileNotFoundException;
import com.dianping.data.warehouse.canaan.util.ExceptionAlertDO;
import com.dianping.data.warehouse.canaan.util.ExceptionAnalyze;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

/**
 * Hive Driver is used for executing Hive CMD During the execution, it will
 * produce log information
 *
 * @author yifan.cao
 */
public class HiveDriver implements Driver {
    private String[] CMD_HEAD = {"/usr/bin/env", "bash",
            "/home/cyf/hive-0.8.1/bin/hive", "-i",
            "/home/cyf/canaan/init/hive-init.sql", "-f", "", "-hiveconf", "CANAAN_HOME=/home/cyf/canaan"};
    private Logger logger = Logger.getLogger(HiveDriver.class);
    private String fileEncoding = "utf-8";


    public HiveDriver() throws HiveClientNotFoundException {
    }

    public HiveDriver(String cliPath, String initPath, String hiveconf, String tmpPath)
            throws HiveClientNotFoundException, HiveInitFileNotFoundException {
        this.setHiveClientPath(cliPath);
        this.setHiveInitPath(initPath);
        this.setHiveConf(hiveconf);
        this.setTmpPath(tmpPath);
    }

    public int execute(ArrayList<HiveCMDInfo> list)
            throws IOException, InterruptedException, ClassNotFoundException,
            SQLException {
        return execute(list, false);
    }

    public int execute(ArrayList<HiveCMDInfo> list,
                       boolean returnResultFlag) throws IOException, InterruptedException,
            ClassNotFoundException, SQLException {
        ExceptionAnalyze exceptionAnalyze = new ExceptionAnalyze();

        StringBuffer sb = new StringBuffer();
        // set StringBuffer empty
        sb.setLength(0);
        for (HiveCMDInfo hiveCI : list) {
            String tmp = hiveCI.getCMD();
            if (tmp.length() != 0) {
                sb.append(hiveCI.getCMD() + "\n");
                if (!hiveCI.getCMD().endsWith(";"))
                    sb.append(";\n");
            }
        }
        if (sb.length() == 0)
            return Constants.RET_FAILED;
        FileUtils.writeStringToFile(new File(CMD_HEAD[6]), sb.toString(), this.fileEncoding);
        Runtime rt = Runtime.getRuntime();
        String[] cmd = CMD_HEAD.clone();


        Process proc = rt.exec(cmd);

        InputStream stdin = proc.getInputStream();
        InputStream stderr = proc.getErrorStream();
        StreamPrinter spin = new StreamPrinter(stdin);
        HiveCMDInfoTracker jt = new HiveCMDInfoTracker(stderr, list);
        spin.start();

        jt.start();
        int retCode = proc.waitFor();
        logger.info("SQL retCode:" + retCode);
        // hiveCI = parseHiveCMDInfo(hiveCI,stderrList);
        if (retCode != 0) {
            ExceptionAlertDO alert = exceptionAnalyze.analyze(stderr);
            if (alert != null)
                return alert.getId();
            return 16;
        } else
            return Constants.RET_SUCCESS;
    }

    public void setHiveClientPath(String cliPath)
            throws HiveClientNotFoundException {
        String cmd_trimmed = cliPath.trim();
        if (cmd_trimmed.toLowerCase().endsWith("hive")) {
            this.CMD_HEAD[2] = cliPath;
        } else
            this.CMD_HEAD[2] = cliPath
                    + (cliPath.endsWith(File.separator) ? "hive"
                    : File.separator + "hive");
        if (!new File(this.CMD_HEAD[2]).exists())
            throw new HiveClientNotFoundException();
    }

    public void setHiveInitPath(String initPath)
            throws HiveInitFileNotFoundException {
        String cmd_trimmed = initPath.trim();
        if (cmd_trimmed.toLowerCase().endsWith("hive-init.sql")) {
            this.CMD_HEAD[4] = initPath;
        } else
            this.CMD_HEAD[4] = initPath
                    + (initPath.endsWith(File.separator) ? "hive-init.sql"
                    : File.separator + "hive-init.sql");
        if (!new File(this.CMD_HEAD[4]).exists())
            throw new HiveInitFileNotFoundException();
    }

    public void setHiveConf(String hiveconf)
            throws HiveInitFileNotFoundException {
        String cmd_trimmed = hiveconf.trim();
        this.CMD_HEAD[8] = cmd_trimmed;
    }

    public void setTmpPath(String s) {
        this.CMD_HEAD[6] = s.trim();
    }
}
