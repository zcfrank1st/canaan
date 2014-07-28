package com.dianping.data.warehouse.canaan.log;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;

import com.dianping.data.warehouse.canaan.connector.MySQLConnector;
import org.apache.log4j.Logger;

import com.dianping.data.warehouse.canaan.common.HiveCMDInfo;
import com.dianping.data.warehouse.canaan.common.HiveJobInfo;

public class MySQLHiveLog implements HiveLog {
	private static final Logger logger = Logger.getLogger(MySQLHiveLog.class);

	public static enum JOB_STAGE {
		MR_JOB_START, NONMR_JOB_START, MR_JOB_MR_INFO, MR_JOB_END, MR_JOB_HDFS_INFO, JOB_END
	}

	private String caldate;
	private MySQLConnector mysqlConnector;
	private HiveLogConf conf;
	private int taskid;
	private String taskname;

	public MySQLHiveLog() throws ClassNotFoundException, SQLException {
		conf = HiveLogConf.getConf();
		mysqlConnector = new MySQLConnector(conf.getMysqlConnectParams());
		caldate = conf.getCaldate();
		taskid = conf.getTaskid();
		taskname = conf.getTaskname();
	}

	private Timestamp toTimestamp(long l) {
		return new Timestamp(l);
	}

	private String toString(List<String> list, String seperate) {
		Iterator<String> iter = list.iterator();
		StringBuffer sb = new StringBuffer();
		sb.setLength(0);
		if (iter.hasNext()) {
			sb.append(iter.next());
		}
		while (iter.hasNext()) {
			sb.append(';');
			sb.append(iter.next());
		}
		return sb.toString();
	}

	private String substring(String str, int begin, int end) {
		if (str == null) return null;
		return str.length() >= end + 1 - begin ? str.substring(begin, end)
				: str;
	}

	@Override
	public void init(String s) throws SQLException, ClassNotFoundException {
		mysqlConnector.connect();
	}

	@Override
	public void destroy(String s) throws SQLException {
		mysqlConnector.close();
	}

	@Override
	public void log(HiveCMDInfo hiveCI, boolean isUpdate) throws SQLException, ClassNotFoundException {
		if (!isUpdate) {
			String sql = "insert into cal_hive_cmd_log(task_id,"
					+ "task_name,cmd_seq,cmd,is_sql,start_time,cal_dt) "
					+ "values(?,?,?,?,?,?,?)";
			PreparedStatement pstmt = mysqlConnector.getPstmt(sql);
			if (pstmt != null) {
				if (conf.getTaskid() == -999)
					pstmt.setNull(1, java.sql.Types.INTEGER);
				else
					pstmt.setInt(1, conf.getTaskid());
				pstmt.setString(2, taskname);
				pstmt.setInt(3, hiveCI.getSequence());
				pstmt.setString(4, substring(hiveCI.getCMD(), 0, 3999));
				pstmt.setInt(5, hiveCI.getIsSQL());
				pstmt.setTimestamp(6, toTimestamp(hiveCI.getStartTime()));
				// pstmt.setTimestamp(7, toTimestamp(hiveCI.getFinishTime()));
				// pstmt.setLong(8, hiveCI.getFinishTime() - hiveCI.StartTime();
				// pstmt.setInt(7, -1);
				pstmt.setDate(7, java.sql.Date.valueOf(this.caldate));
				logger.info(pstmt.toString());
				conf.setCurrentlogid((int) (mysqlConnector.executeUpdate(pstmt)));
			}
		} else {
			String sql = "update cal_hive_cmd_log " + "set job_cnt = ?,"
					+ "is_sql = ?," + "finish_time = ?,"
					+ "time_cost = ?,"
					+ "exit_code = ?,"
					+ "exit_info = ?,"
					// + "target_tables = ?," + "source_tables = ?,"
					+ "job_cnt = ?," + "tot_cpu_time = ?,"
					+ "tot_hdfs_read = ?," + "tot_hdfs_write = ?,"
					+ "tot_mapper_cnt = ?," + "tot_reduce_cnt = ? "
					+ "where log_id = ?";
			PreparedStatement pstmt = mysqlConnector.getPstmt(sql);
			if (pstmt != null) {
				pstmt.setInt(1, hiveCI.getJobNumber());
				pstmt.setInt(2, hiveCI.getIsSQL());
				pstmt.setTimestamp(3, toTimestamp(hiveCI.getFinishTime()));
				pstmt.setLong(4, hiveCI.getFinishTime() - hiveCI.getStartTime());
				pstmt.setInt(5, hiveCI.getStatus());
				pstmt.setString(6, substring(hiveCI.getExitinfo(), 0, 999));
				// pstmt.setString(7, toString(hiveCI.getTargetTableList(),
				// ";"));
				// pstmt.setString(8, toString(hiveCI.getSourceTableList(),
				// ";"));
				pstmt.setInt(7, hiveCI.getJobNumber());
				pstmt.setLong(8, hiveCI.getCPUTime());
				pstmt.setLong(9, hiveCI.getHDFSRead());
				pstmt.setLong(10, hiveCI.getHDFSWrite());
				pstmt.setInt(11, hiveCI.getMapperNumber());
				pstmt.setInt(12, hiveCI.getReducerNumber());
				pstmt.setLong(13, conf.getCurrentlogid());
				logger.info(pstmt.toString());
				mysqlConnector.executeUpdate(pstmt);
			}
		}
	}

	public long getLogid() {
		return conf.getCurrentlogid();
	}

	@Override
	public void log(HiveJobInfo hiveJI, int stage) throws SQLException, ClassNotFoundException {
		if (stage == JOB_STAGE.MR_JOB_START.ordinal()) {
			String sql = "insert into cal_hive_job_log" + "(job_id"
					+ ",job_seq" + ",is_mr_job" + ",job_tracking_url"
					+ ",job_kill_cmd" + ",start_time" + ",log_id) "
					+ "values (?,?,?,?,?,?,?);";
			PreparedStatement pstmt = mysqlConnector.getPstmt(sql);
			if (pstmt != null) {
				pstmt.setString(1, hiveJI.getJobID());
				pstmt.setInt(2, hiveJI.getJobOrder());
				pstmt.setInt(3, hiveJI.getIsMapReduce());
				pstmt.setString(4, hiveJI.getJobTrackingURL());
				pstmt.setString(5, hiveJI.getJobKillCMD());
				pstmt.setTimestamp(6, toTimestamp(hiveJI.getJobStartTime()));
				pstmt.setLong(7, conf.getCurrentlogid());
				logger.info(pstmt.toString());
				mysqlConnector.executeUpdate(pstmt);
			}
		} else if (stage == JOB_STAGE.MR_JOB_MR_INFO.ordinal()) {
			String sql = "update cal_hive_job_log " + "set mapper_cnt = ? "
					+ ",reducer_cnt = ? " + "where job_id = ? ";
			PreparedStatement pstmt = mysqlConnector.getPstmt(sql);
			if (pstmt != null) {
				pstmt.setInt(1, hiveJI.getMapperNumber());
				pstmt.setInt(2, hiveJI.getReducerNumber());
				pstmt.setString(3, hiveJI.getJobID());
				logger.info(pstmt.toString());
				mysqlConnector.executeUpdate(pstmt);
			}
		} else if (stage == JOB_STAGE.MR_JOB_HDFS_INFO.ordinal()) {
			String sql = "update cal_hive_job_log " + "set hdfs_read = ?"
					+ ",hdfs_write = ? " + "where job_id = ?";
			PreparedStatement pstmt = mysqlConnector.getPstmt(sql);
			if (pstmt != null) {
				pstmt.setLong(1, hiveJI.getHDFSRead());
				pstmt.setLong(2, hiveJI.getHDFSWrite());
				pstmt.setString(3, hiveJI.getJobID());
				logger.info(pstmt.toString());
				mysqlConnector.executeUpdate(pstmt);
			}
		} else if (stage == JOB_STAGE.MR_JOB_END.ordinal()) {
			String sql = "update cal_hive_job_log " + "set finish_time = ?"
					+ ",cpu_time = ?" + ",ret_code = ?" + ",ret_info = ? "
					+ "where job_id = ?";
			PreparedStatement pstmt = mysqlConnector.getPstmt(sql);
			if (pstmt != null) {
				pstmt.setTimestamp(1, toTimestamp(hiveJI.getJobFinishTime()));
				pstmt.setLong(2, hiveJI.getCPUTime());
				pstmt.setInt(3, hiveJI.getStatus());
				pstmt.setString(4, substring(hiveJI.getRetInfo(), 0, 999));
				pstmt.setString(5, hiveJI.getJobID());
				logger.info(pstmt.toString());
				mysqlConnector.executeUpdate(pstmt);
			}
		} else if (stage == JOB_STAGE.NONMR_JOB_START.ordinal()) {
			String sql = "insert into cal_hive_job_log" + "(job_id"
					+ ",job_seq" + ",is_mr_job" + ",ret_code" + ",ret_info"
					+ ",start_time" + ",finish_time) "
					+ "values (?,?,?,?,?,?,?);";
			PreparedStatement pstmt = mysqlConnector.getPstmt(sql);
			if (pstmt != null) {
				pstmt.setString(1, hiveJI.getJobID());
				pstmt.setInt(2, hiveJI.getJobOrder());
				pstmt.setInt(3, hiveJI.getIsMapReduce());
				pstmt.setInt(4, hiveJI.getStatus());
				pstmt.setString(5, substring(hiveJI.getRetInfo(), 0, 999));
				pstmt.setTimestamp(6, toTimestamp(hiveJI.getJobStartTime()));
				pstmt.setTimestamp(7, toTimestamp(hiveJI.getJobFinishTime()));
				logger.info(pstmt.toString());
				mysqlConnector.executeUpdate(pstmt);
			}
		} else if (stage == JOB_STAGE.JOB_END.ordinal()) {
			String sql = "update cal_hive_job_log " + "set finish_time = ?"
					+ ",ret_code = ?" + ",ret_info = ? " + "where job_id = ?";
			PreparedStatement pstmt = mysqlConnector.getPstmt(sql);
			if (pstmt != null) {
				pstmt.setTimestamp(1, toTimestamp(hiveJI.getJobFinishTime()));
				pstmt.setInt(2, hiveJI.getStatus());
				pstmt.setString(3, substring(hiveJI.getRetInfo(), 0, 999));
				pstmt.setString(4, hiveJI.getJobID());
				logger.info(pstmt.toString());
				mysqlConnector.executeUpdate(pstmt);
			}
		}
	}
}
