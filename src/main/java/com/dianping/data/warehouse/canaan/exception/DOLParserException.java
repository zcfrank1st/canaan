package com.dianping.data.warehouse.canaan.exception;

public class DOLParserException extends DWException {
	private static final long serialVersionUID = 1L;

	  public DOLParserException() {
	    super();
	  }

	  public DOLParserException(String message) {
	    super(message);
	  }

	  public DOLParserException(Throwable cause) {
	    super(cause);
	  }

	  public DOLParserException(String message, Throwable cause) {
	    super(message, cause);
	  }
}
