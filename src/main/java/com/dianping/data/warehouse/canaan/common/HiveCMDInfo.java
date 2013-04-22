/**
 * Project: canaan
 * 
 * File Created at 2012-9-27
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
package com.dianping.data.warehouse.canaan.common;

import java.util.ArrayList;

/**
 * Information about SQL Gather the information produced by hive
 * 
 * @author yifan.cao
 * 
 */
public class HiveCMDInfo {


	// information from HiveCMD
	private String CMD;
	private int sequence;
	private int isSQL;
	private long startTime;

	private long finishTime;
	private int status;
	private String exitinfo;
	private ArrayList<String> targetTableList;
	private ArrayList<String> sourceTableList;

	// information from jobs
	private int jobNumber;
	private long CPUTime;
	private long HDFSRead;
	private long HDFSWrite;
	private int mapperNumber;
	private int reducerNumber;
	private ArrayList<HiveJobInfo> jobList;
	public HiveCMDInfo()
	{
		this("");
	}
	
	public HiveCMDInfo(String string) {
		this.setCMD(string);
		this.startTime = 1001;
		this.finishTime = 1001;
		this.status = 2;
		this.exitinfo = "";

		this.sourceTableList = new ArrayList<String>();
		this.sourceTableList.clear();

		this.targetTableList = new ArrayList<String>();
		this.targetTableList.clear();

		this.jobNumber = 0;
		this.CPUTime = 0;
		this.HDFSRead = 0;
		this.HDFSWrite = 0;
		this.setMapperNumber(0);
		this.setReducerNumber(0);

		this.jobList = new ArrayList<HiveJobInfo>();
		this.jobList.clear();
	}

	public int isSQL(String cmd) {
		int flag = 1;
		String cmd_processed = cmd.trim().toLowerCase();
		for (int i = 0; i < Constants.NOSQL_PREFIX1.length; i++) {
			if (cmd_processed.startsWith(Constants.NOSQL_PREFIX1[i]))
			{
				flag = 0;
				break;
			}
		}

		for (int i = 0; i < Constants.NOSQL_PREFIX2.length; i++) {
			if (cmd_processed.startsWith(Constants.NOSQL_PREFIX2[i]))
			{	
				flag = 0;
				break;
			}
		}

		return flag;
	}

	public void setJobTotalInfo() {
		this.setJobNumber(getJobList().size());
		for (HiveJobInfo j : getJobList()) {
			this.setCPUTime(this.getCPUTime() + j.getCPUTime());
			this.setHDFSRead(this.getHDFSRead() + j.getHDFSRead());
			this.setHDFSWrite(this.getHDFSWrite() + j.getHDFSWrite());
			this.setMapperNumber(this.getMapperNumber() + j.getMapperNumber());
			this.setReducerNumber(this.getReducerNumber()
					+ j.getReducerNumber());
		}
	}

	/**
	 * @param startTime
	 *            the startTime to set
	 */
	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	/**
	 * @return the startTime
	 */
	public long getStartTime() {
		return startTime;
	}

	/**
	 * @param finishTime
	 *            the finishTime to set
	 */
	public void setFinishTime(long finishTime) {
		this.finishTime = finishTime;
	}

	/**
	 * @return the finishTime
	 */
	public long getFinishTime() {
		return finishTime;
	}

	/**
	 * @param status
	 *            the status to set
	 */
	public void setStatus(int status) {
		setJobTotalInfo();
		this.status = status;
	}

	/**
	 * @return the status
	 */
	public int getStatus() {
		return status;
	}

	/**
	 * @param jobNumber
	 *            the jobNumber to set
	 */
	public void setJobNumber(int jobNumber) {
		this.jobNumber = jobNumber;
	}

	/**
	 * @return the jobNumber
	 */
	public int getJobNumber() {
		return jobNumber;
	}

	/**
	 * @param cPUTime
	 *            the cPUTime to set
	 */
	public void setCPUTime(long cPUTime) {
		CPUTime = cPUTime;
	}

	/**
	 * @return the cPUTime
	 */
	public long getCPUTime() {
		return CPUTime;
	}

	/**
	 * @param hDFSRead
	 *            the hDFSRead to set
	 */
	public void setHDFSRead(long hDFSRead) {
		HDFSRead = hDFSRead;
	}

	/**
	 * @return the hDFSRead
	 */
	public long getHDFSRead() {
		return HDFSRead;
	}

	/**
	 * @param hDFSWrite
	 *            the hDFSWrite to set
	 */
	public void setHDFSWrite(long hDFSWrite) {
		HDFSWrite = hDFSWrite;
	}

	/**
	 * @return the hDFSWrite
	 */
	public long getHDFSWrite() {
		return HDFSWrite;
	}

	public void addJob(HiveJobInfo job) {
		this.jobList.add(job);
	}

	public HiveJobInfo getJob(int seq) {
		for (HiveJobInfo job : this.jobList) {
			if (job.getJobOrder() == seq)
				return job;
		}
		return null;
	}

    public HiveJobInfo getJob(String id) {
        for (HiveJobInfo job : this.jobList) {
            if (job.getJobID().equals(id))
                return job;
        }
        return null;
    }


    public void setJobList(ArrayList<HiveJobInfo> jobList) {
		this.jobList = jobList;
	}

	/**
	 * @return the jobList
	 */
	public ArrayList<HiveJobInfo> getJobList() {
		return jobList;
	}

	/**
	 * @param targetTableList
	 *            the targetTableList to set
	 */
	public void setTargetTableList(ArrayList<String> targetTableList) {
		this.targetTableList = targetTableList;
	}

	/**
	 * @return the targetTableList
	 */
	public ArrayList<String> getTargetTableList() {
		return targetTableList;
	}

	/**
	 * @param sourceTableList
	 *            the sourceTableList to set
	 */
	public void setSourceTableList(ArrayList<String> sourceTableList) {
		this.sourceTableList = sourceTableList;
	}

	/**
	 * @return the sourceTableList
	 */
	public ArrayList<String> getSourceTableList() {
		return sourceTableList;
	}

	/**
	 * @param sourceTable
	 *            the sourceTable to add
	 */
	public void addSouuceTable(String sourceTable) {
		this.sourceTableList.add(sourceTable);
	}

	/**
	 * @param targetTable
	 *            the targetTable to add
	 */
	public void addTargetTable(String targetTable) {
		this.targetTableList.add(targetTable);
	}

	/**
	 * @param mapperNumber
	 *            the mapperNumber to set
	 */
	public void setMapperNumber(int mapperNumber) {
		this.mapperNumber = mapperNumber;
	}

	/**
	 * @return the mapperNumber
	 */
	public int getMapperNumber() {
		return mapperNumber;
	}

	/**
	 * @param reducerNumber
	 *            the reducerNumber to set
	 */
	public void setReducerNumber(int reducerNumber) {
		this.reducerNumber = reducerNumber;
	}

	/**
	 * @return the reducerNumber
	 */
	public int getReducerNumber() {
		return reducerNumber;
	}

	/**
	 * @param cMD
	 *            the cMD to set
	 */
	public void setCMD(String cMD) {
		this.CMD = cMD;
		this.isSQL = this.isSQL(cMD);
	}

	/**
	 * @return the cMD
	 */
	public String getCMD() {
		return CMD;
	}

	public int getSequence() {
		return sequence;
	}

	public void setSequence(int sequence) {
		this.sequence = sequence;
	}

	public int getIsSQL() {
		return isSQL;
	}

	public void setIsSQL(int isSQL) {
		this.isSQL = isSQL;
	}

	public String getExitinfo() {
		return exitinfo;
	}

	public void setExitinfo(String exitinfo) {
		this.exitinfo = exitinfo;
	}

	public void addMRJob(HiveJobInfo job) {
		job.setIsMapReduce(1);
		this.jobList.add(job);
	}

	public HiveJobInfo getMRJob(int seq) {
		int i = 0;
		for (HiveJobInfo job : this.jobList) {
			if (job.getIsMapReduce() == 1)
				i++;
			if (i == seq) {
				return job;
			}
		}
		return null;
	}
}