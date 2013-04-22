/**
 * Project: canaan
 * 
 * File Created at Oct 18, 2012
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
 * TODO Comment of CanaanPrintHelpException
 * @author stephensonz
 *
 */
public class CanaanPrintHelpException extends DWException {
	private static final long serialVersionUID = 1L;

	  public CanaanPrintHelpException() {
	    super();
	  }

	  public CanaanPrintHelpException(String message) {
	    super(message);
	  }

	  public CanaanPrintHelpException(Throwable cause) {
	    super(cause);
	  }

	  public CanaanPrintHelpException(String message, Throwable cause) {
	    super(message, cause);
	  }
}

