/**
 * Project: canaan
 * 
 * File Created at Sep 25, 2012
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
 * TODO Comment of ParamNotSupportException
 * @author stephensonz
 *
 */
public class ParamNotSupportException extends DWException {
	private static final long serialVersionUID = 1L;

	  public ParamNotSupportException() {
	    super();
	  }

	  public ParamNotSupportException(String message) {
	    super(message);
	  }

	  public ParamNotSupportException(Throwable cause) {
	    super(cause);
	  }

	  public ParamNotSupportException(String message, Throwable cause) {
	    super(message, cause);
	  }
}
