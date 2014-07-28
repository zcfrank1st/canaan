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

import java.util.ArrayList;

import com.dianping.data.warehouse.canaan.common.HiveCMDInfo;

/**
 * TODO Comment of Driver
 * @author yifan.cao
 *
 */
public interface Driver {
    public int execute(ArrayList<HiveCMDInfo> list) throws Exception;
    public int execute(ArrayList<HiveCMDInfo> list,boolean isUpdated) throws Exception;
}
