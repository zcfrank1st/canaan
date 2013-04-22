package com.dianping.data.warehouse.canaan.driver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Date;
import java.util.List;

import com.dianping.data.warehouse.canaan.common.Constants;
import org.apache.log4j.Logger;

import com.dianping.data.warehouse.canaan.common.HiveCMDInfo;
import com.dianping.data.warehouse.canaan.log.HiveLogConf;

public class HiveCMDInfoTracker extends Thread {
    private InputStream is;
    // private String notice;
    private Logger log;
    private OutputStream os;

    private HiveCMDInfoParser hiveCIParser;

    public HiveCMDInfoTracker(InputStream is, List<HiveCMDInfo> list) throws ClassNotFoundException, IOException, SQLException {
        this(is, list, null);
    }

    public HiveCMDInfoTracker(InputStream is, List<HiveCMDInfo> list, OutputStream os) throws ClassNotFoundException, IOException, SQLException {
        this.is = is;
        this.os = os;
        this.log = Logger.getLogger(HiveCMDInfoTracker.class);
        // this.conf = HiveLogConf.getConf();
        this.hiveCIParser = new HiveCMDInfoParser(list);
    }

    public void run() {
        try {
            PrintWriter pw = null;
            if (os != null)
                pw = new PrintWriter(os);
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            String line;
            hiveCIParser.init();
            while ((line = br.readLine()) != null ) {
                String time = Constants.LONG_DF.format(new Date());
                if (pw != null)
                    pw.println("[" + time + "]" + line);
                if (!line.trim().equals(""))
                    System.err.println("[" + time + "]" + line);
                hiveCIParser.parse(time, line);
            }
            if (pw != null)
                pw.flush();
            int exitCode = hiveCIParser.destroy();
            HiveLogConf.getConf().setStatus(exitCode);
        } catch (IOException ioe) {
            log.error(ioe);
            ioe.printStackTrace();
        } catch (ParseException e) {
            log.error(e);
            e.printStackTrace();
        } catch (SQLException e) {
            log.error(e);
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            log.error(e);
            e.printStackTrace();
        }
    }
}