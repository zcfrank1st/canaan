package com.dianping.data.warehouse.canaan.exception;

import com.dianping.data.warehouse.canaan.common.Constants;
import org.apache.log4j.Logger;

/**
 * Exception Handler for exception throwed
 * User: yifan.cao
 * Date: 13-1-5
 * Time: 10:29
 */
public class ExceptionHandler {
    public static Logger logger = null;

    public void initLogger() {
        logger = Logger.getLogger(ExceptionHandler.class);
    }

    public int handle(Exception e) {
        if (e instanceof CanaanPrintHelpException)
            return Constants.RET_SUCCESS;
        else {
            if (logger != null)
                logger.error(e);
            System.err.println(e);
            return Constants.RET_FAILED;
        }
    }
}
