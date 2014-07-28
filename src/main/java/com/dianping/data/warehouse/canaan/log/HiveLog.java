/**
 * Project: canaan
 * 
 * File Created at 2012-9-28
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
package com.dianping.data.warehouse.canaan.log;

import com.dianping.data.warehouse.canaan.common.HiveCMDInfo;
import com.dianping.data.warehouse.canaan.common.HiveJobInfo;

/**
 * Interface for different SQLLog
 * @author yifan.cao
 *
 */
public interface HiveLog {
	public void init(String s) throws Exception;
	public void destroy(String s) throws Exception;
	public void log(HiveCMDInfo content, boolean isUpdate) throws Exception;
	public void log(HiveJobInfo content, int stage) throws Exception;
}