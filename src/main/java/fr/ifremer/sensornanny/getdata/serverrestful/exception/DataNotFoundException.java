package fr.ifremer.sensornanny.getdata.serverrestful.exception;

import java.io.IOException;

/**
 * Data not found exception
 * 
 * @author athorel
 *
 */
public class DataNotFoundException extends IOException {

    /**
     * SerialUID
     */
    private static final long serialVersionUID = -9167751153956751478L;

    /**
     * Declare a data not found exception
     * 
     * @param message name of the resource
     */
    public DataNotFoundException(String message) {
        super(message);
    }

    /**
     * Declare a data not found exception
     * 
     * @param message name of the resource
     */
    public DataNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
