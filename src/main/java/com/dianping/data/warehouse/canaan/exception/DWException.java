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
package com.dianping.data.warehouse.canaan.exception;

/**
 * TODO Comment of DWException
 * @author yifan.cao
 *
 */
public class DWException extends Exception {
	public DWException() {
	    super();
	  }

	  public DWException(String message) {
	    super(message);
	  }

	  public DWException(Throwable cause) {
	    super(cause);
	  }

	  public DWException(String message, Throwable cause) {
	    super(message, cause);
	  }
}