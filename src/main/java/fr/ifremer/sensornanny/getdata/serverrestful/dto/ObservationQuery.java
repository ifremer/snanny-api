package fr.ifremer.sensornanny.getdata.serverrestful.dto;

import org.elasticsearch.common.geo.GeoPoint;

/**
 * Representation of a query for observation
 * 
 * @author athorel
 *
 */
public class ObservationQuery {

    /** Bottom Left point of the map */
    private GeoPoint from;
    /** Top Right of the map */
    private GeoPoint to;

    /** Start range time */
    private Long timeFrom;
    /** End range time */
    private Long timeTo;

    /** types keywords */
    private String keywords;

    /**
     * Percentage of depth where the measure have been taken ( distance /
     * max_distance)
     */
    private Float depthPercent;

    public GeoPoint getFrom() {
        return from;
    }

    public void setFrom(GeoPoint from) {
        this.from = from;
    }

    public GeoPoint getTo() {
        return to;
    }

    public void setTo(GeoPoint to) {
        this.to = to;
    }

    public Long getTimeFrom() {
        return timeFrom;
    }

    public void setTimeFrom(Long timeFrom) {
        this.timeFrom = timeFrom;
    }

    public Long getTimeTo() {
        return timeTo;
    }

    public void setTimeTo(Long timeTo) {
        this.timeTo = timeTo;
    }

    public String getKeywords() {
        return keywords;
    }

    public void setKeywords(String keywords) {
        this.keywords = keywords;
    }

    public Float getDepthPercent() {
        return depthPercent;
    }

    public void setDepthPercent(Float depthPercent) {
        this.depthPercent = depthPercent;
    }

    @Override
    public String toString() {

        return "["
                //
                + "bbox : { from: " + from + ", to:" + to
                //
                + "}, time : { from: " + timeFrom + ", to: " + timeTo
                //
                + ", keywords : " + keywords + "]";
    }

}
