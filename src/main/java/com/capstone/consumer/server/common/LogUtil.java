package com.capstone.consumer.server.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class LogUtil {

    private LogUtil() {}

    /**
     * Trace log
     */
    public static Logger traceLog = LoggerFactory.getLogger("trace");

    /**
     * Error log
     */
    public static Logger errorLog = LoggerFactory.getLogger("error");

    /**
     * binary log
     */
    public static Logger binaryLog = LoggerFactory.getLogger("binary");

    /**
     * System log
     */
    public static Logger systemLog = LoggerFactory.getLogger("system");

}
