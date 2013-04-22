/**
 * Project: canaan
 * 
 * File Created at 2012-10-8
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


/**
 * TODO Comment of JobInfo
 * @author yifan.cao
 *
 */
public class HiveJobInfo {
	private String jobID;
	private int jobOrder;
	private String jobTrackingURL;
	private String jobKillCMD;
	private int reducerNumber;
	private int mapperNumber;
	private long HDFSRead;
	private long HDFSWrite;	
	private long CPUTime;
	private int status;
	private String retInfo;
	
	private int isMapReduce;
	
	private long jobStartTime;
	private long jobFinishTime;
	

	public HiveJobInfo()
	{
		jobID = "";
		jobOrder = 0;
		jobTrackingURL = "";
		jobKillCMD = "";
		reducerNumber = 0;
		mapperNumber = 0;
		HDFSRead = 0;
		HDFSWrite = 0;	
		CPUTime = 0;
		status = -1;
		jobStartTime = 1000;
		jobFinishTime = 1000;
		setIsMapReduce(0);
	}
	
	public HiveJobInfo(int seq) {
		this();
		this.jobOrder = seq;
	}

	public String toString()
	{
		return "Job " +
				getJobOrder() + 
				": Map: " +
				getMapperNumber() +
				"Reduce: " +
				getReducerNumber() +
				"Accumulative CPU: " +
				getCPUTime() +
				"HDFS Read: " +
				getHDFSRead() +
				"HDFS Write: " +
				getHDFSWrite() +
				"Status: " +
				getStatus();
	}
	/**
	 * @param jobID the jobID to set
	 */
	public void setJobID(String jobID) {
		this.jobID = jobID;
	}
	/**
	 * @return the jobID
	 */
	public String getJobID() {
		return jobID;
	}
	/**
	 * @param jobOrder the jobOrder to set
	 */
	public void setJobOrder(int jobOrder) {
		this.jobOrder = jobOrder;
	}
	/**
	 * @return the jobOrder
	 */
	public int getJobOrder() {
		return jobOrder;
	}
	/**
	 * @param jobTrackingURL the jobTrackingURL to set
	 */
	public void setJobTrackingURL(String jobTrackingURL) {
		this.jobTrackingURL = jobTrackingURL;
	}
	/**
	 * @return the jobTrackingURL
	 */
	public String getJobTrackingURL() {
		return jobTrackingURL;
	}
	/**
	 * @param jobKillCMD the jobKillCMD to set
	 */
	public void setJobKillCMD(String jobKillCMD) {
		this.jobKillCMD = jobKillCMD;
	}
	/**
	 * @return the jobKillCMD
	 */
	public String getJobKillCMD() {
		return jobKillCMD;
	}
	/**
	 * @param reducerNumber the reducerNumber to set
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
	 * @param mapperNumber the mapperNumber to set
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
	 * @param hDFSRead the hDFSRead to set
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
	 * @param hDFSWrite the hDFSWrite to set
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
	/**
	 * @param cPUTime the cPUTime to set
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
	 * @param status the status to set
	 */
	public void setStatus(int status) {
		this.status = status;
	}
	/**
	 * @return the status
	 */
	public int getStatus() {
		return status;
	}

	public long getJobStartTime() {
		return jobStartTime;
	}

	public void setJobStartTime(long jobStartTime) {
		this.jobStartTime = jobStartTime;
	}
	
	public long getJobFinishTime() {
		return jobFinishTime;
	}

	public void setJobFinishTime(long jobFinishTime) {
		this.jobFinishTime = jobFinishTime;
	}


	public String getRetInfo() {
		return retInfo;
	}

	public void setRetInfo(String retInfo) {
		this.retInfo = retInfo;
	}

	public int getIsMapReduce() {
		return isMapReduce;
	}

	public void setIsMapReduce(int isMapReduce) {
		this.isMapReduce = isMapReduce;
	}

}
