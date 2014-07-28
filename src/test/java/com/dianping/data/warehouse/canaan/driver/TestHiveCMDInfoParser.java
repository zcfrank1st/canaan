package com.dianping.data.warehouse.canaan.driver;

import java.util.ArrayList;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.dianping.data.warehouse.canaan.common.Constants;
import com.dianping.data.warehouse.canaan.connector.MySQLConnector;
import junit.framework.TestCase;

import com.dianping.data.warehouse.canaan.common.HiveCMDInfo;
import com.dianping.data.warehouse.canaan.common.HiveJobInfo;
import com.dianping.data.warehouse.canaan.conf.CanaanConf;

public class TestHiveCMDInfoParser extends TestCase {
	static ArrayList<HiveCMDInfo> list = new ArrayList<HiveCMDInfo>();
	static HiveCMDInfo hiveCI = new HiveCMDInfo("select count(1) from test_a;");
	public void testParse() throws Throwable
	{
		CanaanConf canaanConf = CanaanConf.getConf();
		canaanConf.loadHiveLogConf();
		list.add(hiveCI);
		HiveCMDInfoParser jt = new HiveCMDInfoParser(list);
		MySQLConnector conn = new MySQLConnector(canaanConf.getHiveLogConf().getMysqlConnectParams());
		conn.connect();
		conn.execute("delete from cal_hive_job_log where job_id = 'job_201210231934_0001' or job_id = 'job_201210231934_0002'");
		conn.close();
		jt.init();
		//jt.parse("2012-10-23 19:40:43.870","OK");
		jt.parse("2012-10-23 19:40:43.870","Starting Job = job_201210231934_0001, Tracking URL = http://localhost:50030/jobdetails.jsp?jobid=job_201210231934_0001");
		jt.parse("2012-10-23 19:40:43.870","Kill Command = /usr/local/hadoop/hadoop-release/libexec/../bin/hadoop job  -Dmapred.job.tracker=10.1.1.172:8021 -kill job_201210231934_0002");
		jt.parse("2012-10-23 19:40:43.870","Hadoop job information for Stage-5312: number of mappers: 111; number of reducers: 11");
		jt.parse("2012-10-23 19:40:43.870","MapReduce Total cumulative CPU time: 1 minutes 1 seconds 1 msec");
		jt.parse("2012-10-23 19:41:43.870","Ended Job = job_201210231934_0001");
		jt.parse("2012-10-23 19:40:43.870","Starting Job = job_201210231934_0002, Tracking URL = http://localhost:50030/jobdetails.jsp?jobid=job_201210231934_0002");
		jt.parse("2012-10-23 19:40:43.870","Kill Command = /usr/local/hadoop/hadoop-release/libexec/../bin/hadoop job  -Dmapred.job.tracker=10.1.1.172:8021 -kill job_201210231934_0002");
		jt.parse("2012-10-23 19:40:43.870","Hadoop job information for Stage-5312: number of mappers: 222; number of reducers: 22");
		jt.parse("2012-10-23 19:40:43.870","MapReduce Total cumulative CPU time: 2 minutes 2 seconds 2 msec");
		jt.parse("2012-10-23 19:41:43.870","Ended Job = job_201210231934_0002");
		jt.parse("2012-10-23 19:41:43.870","Job 0: Map: 111   Accumulative CPU: 111.1 sec   HDFS Read: 11111 HDFS Write: 11111 SUCESS");
		jt.parse("2012-10-23 19:41:43.870","Job 1: Map: 222   Accumulative CPU: 222.2 sec   HDFS Read: 22222 HDFS Write: 22222 SUCESS");
		jt.parse("2012-10-23 19:41:43.870","OK");
//		jt.parse("2012-10-23 19:42:43.870","Failed:line1");
//		jt.parse("2012-10-23 19:42:43.870","line2");
//		jt.parse("2012-10-23 19:42:43.870","line3");
		jt.destroy();
		HiveJobInfo hiveJI = hiveCI.getJob(1);
		HiveJobInfo hiveJI2 = hiveCI.getJob(2);
		System.out.println("---------------JOB1--------------");
		System.out.println("JOB_ID: "+hiveJI.getJobID());
		System.out.println("JOB_TRACKING_URL: "+hiveJI.getJobTrackingURL());
		System.out.println("JOB_KILL_CMD: "+hiveJI.getJobKillCMD());
		System.out.println("JOB_START_TIME: " + Constants.LONG_DF.format(new Date(hiveJI.getJobStartTime())));
		System.out.println("JOB_CPU_TIME: " + hiveJI.getCPUTime());
		System.out.println("JOB_FINISH_TIME: " + Constants.LONG_DF.format(new Date(hiveJI.getJobFinishTime())));
		System.out.println("JOB_MAPPER_NUMBER: " + hiveJI.getMapperNumber());
		System.out.println("JOB_REDUCER_NUMBER: " + hiveJI.getReducerNumber());	
		System.out.println("JOB_HDFS_READ: " + hiveJI.getHDFSRead());
		System.out.println("JOB_HDFS_WRITE: " + hiveJI.getHDFSWrite());		
		
		System.out.println("---------------JOB2--------------");
		System.out.println("JOB_ID: "+hiveJI2.getJobID());
		System.out.println("JOB_TRACKING_URL: "+hiveJI2.getJobTrackingURL());
		System.out.println("JOB_KILL_CMD: "+hiveJI2.getJobKillCMD());
		System.out.println("JOB_START_TIME: " + Constants.LONG_DF.format(new Date(hiveJI2.getJobStartTime())));
		System.out.println("JOB_CPU_TIME: " + hiveJI2.getCPUTime());
		System.out.println("JOB_FINISH_TIME: " + Constants.LONG_DF.format(new Date(hiveJI2.getJobFinishTime())));
		System.out.println("JOB_MAPPER_NUMBER: " + hiveJI2.getMapperNumber());
		System.out.println("JOB_REDUCER_NUMBER: " + hiveJI2.getReducerNumber());	
		System.out.println("JOB_HDFS_READ: " + hiveJI2.getHDFSRead());
		System.out.println("JOB_HDFS_WRITE: " + hiveJI2.getHDFSWrite());		
		System.out.println("JOB_EXIT_CODE: " + hiveJI2.getStatus());
		
		System.out.println("---------------CMDINFO--------------");
		System.out.println("JOB_END_STATUS: "+ hiveCI.getStatus());
		
		System.out.println("---------------FAIL--------------");
		System.out.println("JOB_END_STATUS: "+ hiveCI.getExitinfo());	
	}
    public void testKill()
    {
        Pattern p = Pattern.compile(".* -kill ([-?\\w]*)");
        Matcher m = p.matcher("Kill Command = /usr/local/hadoop/hadoop-release/libexec/../bin/hadoop job  -Dmapred.job.tracker=10.2.6.101:8021 -kill job_201212131543_17064");
        if (m.find()){
        System.out.println(m.group(1));}
    }
}