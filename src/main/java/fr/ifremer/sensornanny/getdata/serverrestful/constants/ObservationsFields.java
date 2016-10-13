package fr.ifremer.sensornanny.getdata.serverrestful.constants;

/**
 * Constants of Observations fields used for search
 * 
 * @author athorel
 *
 */
public final class ObservationsFields {

    /** observations metadata , stored document in elasticSearch */
    public static final String DOC_FIELD = "doc";

    public static final String SNANNY = "snanny";

    /** observations coordinates (geopoint) */
    public static final String COORDINATES = SNANNY + "-coordinates";

    /** observation timestamp */
    public static final String RESULTTIMESTAMP = SNANNY + "-resulttimestamp";

    /** Aggragat */
    public static final String AGGREGAT = "agg";

    /** Aggragat on term descriptions and names */
    public static final String AGGREGAT_TERM = "term_" + AGGREGAT;

    /** Aggragat on date */
    public static final String AGGREGAT_DATE = "date_" + AGGREGAT;

    /** Aggragat on geo coordinates */
    public static final String AGGREGAT_GEOGRAPHIQUE = "geo_" + AGGREGAT;

    /** Internal aggregat (Zooming) box */
    public static final String ZOOM_IN_AGGREGAT_GEOGRAPHIQUE = "zoom_in_" + AGGREGAT;

    public static final String SNANNY_UUID = SNANNY + "-uuid";

    public static final String SNANNY_NAME = SNANNY + "-name";

    public static final String SNANNY_DEPLOYMENTID = SNANNY + "-deploymentid";

    public static final String SNANNY_ANCESTORS = SNANNY + "-ancestors";

    public static final String SNANNY_ANCESTOR_NAME = SNANNY + "-ancestor-name";

    public static final String SNANNY_ANCESTOR_DEPLOYMENTID = SNANNY + "-ancestor-deploymentid";

    public static final String SNANNY_ANCESTOR_DESCRIPTION = SNANNY + "-ancestor-description";

    public static final String SNANNY_ANCESTOR_TERMS = SNANNY + "-ancestor-terms";

    public static final String SNANNY_ANCESTOR_UUID = SNANNY + "-ancestor-uuid";

    private ObservationsFields() {

    }
}
