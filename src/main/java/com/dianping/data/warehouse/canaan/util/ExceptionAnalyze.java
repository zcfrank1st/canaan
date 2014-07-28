package com.dianping.data.warehouse.canaan.util;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by Sunny on 14-7-18.
 */
public class ExceptionAnalyze {

    private static Map<String, ExceptionAlertDO> alertMap = new HashMap<String, ExceptionAlertDO>();

    public ExceptionAnalyze() {
        DBConnector connector = new DBConnector();
        List<ExceptionAlertDO> exceptionAlertDOs = connector.getExceptionAlertsByProduct("canaan");
        for (ExceptionAlertDO exceptionAlertDO : exceptionAlertDOs) {
            String reason = exceptionAlertDO.getDescription();
            alertMap.put(reason, exceptionAlertDO);
        }
    }

    public ExceptionAlertDO analyze(InputStream is) {
        try {
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            String line;
            while ((line = br.readLine()) != null) {
                ExceptionAlertDO alert = analyzeLine(line);
                if (alert != null)
                    return alert;
            }
            return null;
        } catch (IOException ioe) {
            ioe.printStackTrace();
            return null;
        }
    }

    private ExceptionAlertDO analyzeLine(String line) {
        Iterator iter = alertMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            String key = (String) entry.getKey();
            if (line.toLowerCase().contains(key.toLowerCase()))
                return (ExceptionAlertDO) entry.getValue();
        }
        return null;
    }

    public static void main(String args[]) {
        File file = new File("/Users/Sunny/Desktop/test.txt");
        try {
            FileInputStream in = new FileInputStream(file);
            ExceptionAlertDO alertDO = new ExceptionAnalyze().analyze(in);

            System.out.println(alertDO.getId());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
