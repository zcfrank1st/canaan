/**
 * Project: canaan
 * 
 * File Created at Oct 16, 2012
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
 * TODO Comment of HiveClientNotFoundException
 * @author stephensonz
 *
 */
public class HiveClientNotFoundException extends DWException {
	private static final long serialVersionUID = 1L;

	  public HiveClientNotFoundException() {
	    super();
	  }

	  public HiveClientNotFoundException(String message) {
	    super(message);
	  }

	  public HiveClientNotFoundException(Throwable cause) {
	    super(cause);
	  }

	  public HiveClientNotFoundException(String message, Throwable cause) {
	    super(message, cause);
	  }
}
