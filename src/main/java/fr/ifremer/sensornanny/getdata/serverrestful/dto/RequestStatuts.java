package fr.ifremer.sensornanny.getdata.serverrestful.dto;

/**
 * Elastic search request status
 * 
 * @author athorel
 *
 */
public enum RequestStatuts {

    /** Succes response */
    SUCCESS("success"),

    /** Timeout (the search took too much time to execute) */
    TIMEOUT("timeout"),

    /** The search raised a too large number of document */
    TOOMANY("tooMany"),

    /** The serach raised 0 documents */
    EMPTY("empty");

    /** JSON message */
    private String jsonMessage;

    private RequestStatuts(String jsonMessage) {
        this.jsonMessage = jsonMessage;
    }

    @Override
    public String toString() {
        return jsonMessage;
    }
}
