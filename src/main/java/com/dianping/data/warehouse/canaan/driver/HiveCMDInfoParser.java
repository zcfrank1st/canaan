package com.dianping.data.warehouse.canaan.driver;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.dianping.data.warehouse.canaan.common.Constants;
import com.dianping.data.warehouse.canaan.common.HiveCMDInfo;
import com.dianping.data.warehouse.canaan.common.HiveJobInfo;
import com.dianping.data.warehouse.canaan.log.FileHiveLog;
import com.dianping.data.warehouse.canaan.log.MySQLHiveLog;


/*
 * @author cyf
 *
 */
public class HiveCMDInfoParser {
    private static enum HIVECMD_INFO_PREFIX_KEY {
        HIVECMD_JOB_CNT, HIVECMD_CPU_TIME, HIVECMD_END_SUCCESS, HIVECMD_END
    }

    private static final HashMap<String, Pattern> HIVECMD_INFO_PREFIX = new HashMap<String, Pattern>();

    static {
        HIVECMD_INFO_PREFIX.put(
                HIVECMD_INFO_PREFIX_KEY.HIVECMD_JOB_CNT.toString(),
                Pattern.compile("Total MapReduce jobs = (\\d+)"));
        HIVECMD_INFO_PREFIX.put(
                HIVECMD_INFO_PREFIX_KEY.HIVECMD_CPU_TIME.toString(),
                Pattern.compile("Total MapReduce CPU Time Spent: (.+)"));
        HIVECMD_INFO_PREFIX.put(
                HIVECMD_INFO_PREFIX_KEY.HIVECMD_END_SUCCESS.toString(),
                Pattern.compile("OK$"));
        HIVECMD_INFO_PREFIX.put(
                HIVECMD_INFO_PREFIX_KEY.HIVECMD_END.toString(),
                Pattern.compile("Time taken: .*"));
    }

    private static enum JOB_INFO_PREFIX_KEY {
        JOB_START, JOB_KILL_CMD, JOB_MAPR_INFO, JOB_CPU_TIME, JOB_END, JOB_HDFS_INFO, JOB_KILL
    }

    private static final HashMap<String, Pattern> JOB_INFO_PREFIX = new HashMap<String, Pattern>();

    static {
        JOB_INFO_PREFIX.put(JOB_INFO_PREFIX_KEY.JOB_START.toString(),
                Pattern.compile("Starting Job = ([-?\\w]*), Tracking URL = (.*)"));
        JOB_INFO_PREFIX.put(JOB_INFO_PREFIX_KEY.JOB_KILL_CMD.toString(),
                Pattern.compile("Kill Command = (.*)"));
        JOB_INFO_PREFIX.put(JOB_INFO_PREFIX_KEY.JOB_KILL.toString(),
                Pattern.compile("-kill ([-?\\w]*)"));
        JOB_INFO_PREFIX
                .put(JOB_INFO_PREFIX_KEY.JOB_MAPR_INFO.toString(),
                        Pattern.compile("Hadoop job information for Stage-\\d+: number of mappers: (\\d+); number of reducers: (\\d+)"));
        JOB_INFO_PREFIX.put(JOB_INFO_PREFIX_KEY.JOB_END.toString(),
                Pattern.compile("Ended Job = ([-?\\w]*)"));
        JOB_INFO_PREFIX
                .put(JOB_INFO_PREFIX_KEY.JOB_HDFS_INFO.toString(),
                        Pattern.compile("Job \\d+:.* HDFS Read: (\\d+) HDFS Write: (\\d+) ([A-Z]+)"));
        JOB_INFO_PREFIX.put(JOB_INFO_PREFIX_KEY.JOB_CPU_TIME.toString(),
                Pattern.compile("MapReduce Total cumulative CPU time: (.+)"));
        // JOB_INFO_PREFIX.put(JOB_INFO_PREFIX_KEY.JOB_TRACKING_URL.toString(),
        // "Tracking URL = ");
        // JOB_INFO_PREFIX.put(JOB_INFO_PREFIX_KEY.JOB_KILL_CMD.toString(),
        // "Kill Command = ");
        // JOB_INFO_PREFIX.put(JOB_INFO_PREFIX_KEY.JOB_CPU_TIME.toString(),
        // "MapReduce Total cumulative CPU time: ");
        // JOB_INFO_PREFIX.put(JOB_INFO_PREFIX_KEY.JOB_SEQ.toString(),
        // "Launching Job ");
    }

    private static final int msArrayLength = 5;
    private static final int[] msArray = {1, 1000, 60000, 60000 * 60,
            60000 * 60 * 24};

    // counter
    private int cmdCounter;
    private int jobCounter;
    private int mrjobCounter;
    // private int seq;
    // private int mrjobCounter;
    // private int okcnt;
    private List<HiveCMDInfo> list;
    private HiveCMDInfo hiveCI;

    private MySQLHiveLog mysqlLog;
    private FileHiveLog fileLog;

    private boolean failFlag;
    private StringBuffer failInfo;
    private HiveJobInfo hiveJI;

    private void initCounter() {
        cmdCounter = 0;
        jobCounter = 0;
        mrjobCounter = 0;
    }

    public HiveCMDInfo getNextHiveCMDInfo() throws SQLException, ClassNotFoundException {
        while (this.cmdCounter < list.size()) {
            HiveCMDInfo tmp_hiveCI = list.get(this.cmdCounter);
            // list starts with 0 but sequence starts with 1
            System.err.println("hive (bi)>" + tmp_hiveCI.getCMD());
            tmp_hiveCI.setSequence(cmdCounter + 1);
            tmp_hiveCI.setStatus(2);
            tmp_hiveCI.setStartTime(System.currentTimeMillis());
            fileLog.log(tmp_hiveCI, false);
            mysqlLog.log(tmp_hiveCI, false);
            cmdCounter++;
            if (tmp_hiveCI.getIsSQL() == 0) {
                tmp_hiveCI.setStatus(0);
                tmp_hiveCI.setFinishTime(System.currentTimeMillis());
                fileLog.log(tmp_hiveCI, true);
                mysqlLog.log(tmp_hiveCI, true);
            } else {
                mrjobCounter = 0;
                jobCounter = 0;
                this.hiveJI = new HiveJobInfo(0);
                tmp_hiveCI.addJob(this.hiveJI);
                return tmp_hiveCI;
            }
        }
        return null;
    }

    public HiveCMDInfoParser(List<HiveCMDInfo> list)
            throws ClassNotFoundException, IOException, SQLException {
        this.list = list;
        this.mysqlLog = new MySQLHiveLog();
        this.mysqlLog.init("");
        this.fileLog = new FileHiveLog();
    }

    public int destroy() throws SQLException, ClassNotFoundException {
        if (failFlag) {
            hiveCI.setExitinfo(this.failInfo.toString());
            hiveCI.setStatus(1);
            hiveCI.setFinishTime(System.currentTimeMillis());
            fileLog.log(this.hiveCI, true);
            mysqlLog.log(this.hiveCI, true);
            if (hiveJI != null) {
                hiveJI.setStatus(1);
                hiveJI.setRetInfo(this.failInfo.toString().trim());
                mysqlLog.log(hiveJI, MySQLHiveLog.JOB_STAGE.JOB_END.ordinal());
            }
        }
        this.mysqlLog.destroy("");
        if (failFlag)
            return 1;
        else
            return 0;
    }

    private Matcher getMatcher(Pattern pattern, String value) {
        return pattern.matcher(value);
    }

    private long getTimeMillis(String time) throws ParseException {
        return Constants.LONG_DF.parse(time).getTime();
    }

    private long getCPUTimeMillis(String time) {
        String[] timeArray = time.split(" ");
        long millis = 0;
        int j = 0;
        for (int i = timeArray.length - 2; i >= Math.max(0, timeArray.length
                - msArrayLength * 2); ) {
            millis += Integer.parseInt(timeArray[i]) * msArray[j];
            j++;
            i -= 2;
        }
        return millis;
    }

    public void init() throws IOException, ClassNotFoundException, SQLException {
        initCounter();
        this.failFlag = false;
        this.failInfo = new StringBuffer();
        this.failInfo.setLength(0);
        this.hiveCI = getNextHiveCMDInfo();
    }

    public void parse(String time, String line) throws SQLException, ParseException, ClassNotFoundException {
        if (line.startsWith("FAILED:")) {
            this.failFlag = true;
        }
        Matcher m;
        if (failFlag)
        {
            if (line.startsWith("ATTEMPT:"))
                failFlag = false;
            else
                failInfo.append(line).append("\n");
        }
        else if ((getMatcher(
                HIVECMD_INFO_PREFIX.get(HIVECMD_INFO_PREFIX_KEY.HIVECMD_END_SUCCESS
                        .toString()), line)).find()) {
            hiveCI.setStatus(0);
            hiveCI.setFinishTime(System.currentTimeMillis());
        } else if ((getMatcher(
                HIVECMD_INFO_PREFIX.get(HIVECMD_INFO_PREFIX_KEY.HIVECMD_END
                        .toString()), line)).find()) {
            if (hiveCI == null)
                return;
            fileLog.log(this.hiveCI, true);
            mysqlLog.log(this.hiveCI, true);
            hiveCI = getNextHiveCMDInfo();
        } else if ((m = getMatcher(
                HIVECMD_INFO_PREFIX.get(HIVECMD_INFO_PREFIX_KEY.HIVECMD_JOB_CNT
                        .toString()), line)).find()) {
            hiveCI.setJobNumber(Integer.parseInt(m.group(1)));
        } else if (((m = getMatcher(
                JOB_INFO_PREFIX.get(JOB_INFO_PREFIX_KEY.JOB_START.toString()),
                line)).find())) {
            jobCounter++;
            hiveJI = new HiveJobInfo(jobCounter);
            hiveJI.setJobID(m.group(1).trim());
            hiveJI.setJobTrackingURL(m.group(2));
            hiveJI.setJobStartTime(getTimeMillis(time));
            hiveJI.setStatus(2);
            hiveCI.addMRJob(hiveJI);
        } else if (((m = getMatcher(
                JOB_INFO_PREFIX
                        .get(JOB_INFO_PREFIX_KEY.JOB_KILL_CMD.toString()),
                line)).find())) {
            Matcher tmpMatcher = getMatcher(JOB_INFO_PREFIX.get(JOB_INFO_PREFIX_KEY.JOB_KILL.toString()),line);
            // get the job id and insert a row into mysql
            if (tmpMatcher.find()) {
                String jobId = tmpMatcher.group(1);
                hiveCI.getJob(jobId).setJobKillCMD(m.group(1));
                mysqlLog.log(hiveCI.getJob(jobId), MySQLHiveLog.JOB_STAGE.MR_JOB_START.ordinal());
            }
        } else if (((m = getMatcher(
                JOB_INFO_PREFIX.get(JOB_INFO_PREFIX_KEY.JOB_MAPR_INFO
                        .toString()), line)).find())) {
            hiveCI.getJob(jobCounter).setMapperNumber(
                    Integer.parseInt(m.group(1)));
            hiveCI.getJob(jobCounter).setReducerNumber(
                    Integer.parseInt(m.group(2)));
            mysqlLog.log(hiveJI, MySQLHiveLog.JOB_STAGE.MR_JOB_MR_INFO.ordinal());
        } else if (((m = getMatcher(
                JOB_INFO_PREFIX.get(JOB_INFO_PREFIX_KEY.JOB_END.toString()),
                line)).find())) {
            String jobID = m.group(1).trim();
            if (hiveCI.getJob(jobID) != null) {
                hiveCI.getJob(jobID).setJobFinishTime(getTimeMillis(time));
                hiveCI.getJob(jobID).setStatus(0);
                mysqlLog.log(hiveCI.getJob(jobID), MySQLHiveLog.JOB_STAGE.MR_JOB_END.ordinal());
            } else {
                jobCounter++;
                hiveJI = new HiveJobInfo(jobCounter);
                hiveJI.setJobID(m.group(1).trim());
                hiveJI.setJobStartTime(getTimeMillis(time));
                hiveJI.setJobFinishTime(getTimeMillis(time));
                hiveJI.setIsMapReduce(0);
                hiveCI.addJob(hiveJI);
                mysqlLog.log(hiveJI, MySQLHiveLog.JOB_STAGE.NONMR_JOB_START.ordinal());
            }
        } else if (((m = getMatcher(
                JOB_INFO_PREFIX
                        .get(JOB_INFO_PREFIX_KEY.JOB_CPU_TIME.toString()),
                line)).find())) {
            hiveJI.setCPUTime(this.getCPUTimeMillis(m.group(1)));
        } else if (((m = getMatcher(
                JOB_INFO_PREFIX.get(JOB_INFO_PREFIX_KEY.JOB_HDFS_INFO
                        .toString()), line)).find())) {
            // Grep MapReduceJob HDFS information
            mrjobCounter++;
            hiveCI.getMRJob(mrjobCounter).setHDFSRead(
                    Long.parseLong(m.group(1)));
            hiveCI.getMRJob(mrjobCounter).setHDFSWrite(
                    Long.parseLong(m.group(2)));
            mysqlLog.log(hiveCI.getMRJob(mrjobCounter), MySQLHiveLog.JOB_STAGE.MR_JOB_HDFS_INFO.ordinal());
        }
    }
}
