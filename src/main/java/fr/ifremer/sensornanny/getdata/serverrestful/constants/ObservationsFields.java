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

    /** observations coordinates (geopoint) */
    public static final String COORDINATES = "snanny-coordinates";

    /** observation timestamp */
    public static final String RESULTTIMESTAMP = "snanny-resulttimestamp";

    /** Aggragat on term descriptions and names */
    public static final String AGGREGAT_TERM = "term_agg";

    /** Aggragat on date */
    public static final String AGGREGAT_DATE = "date_agg";

    /** Aggragat on geo coordinates */
    public static final String AGGREGAT_GEOGRAPHIQUE = "geo_agg";

    /** Internal aggregat (Zooming) box */
    public static final String ZOOM_IN_AGGREGAT_GEOGRAPHIQUE = "zoom_in_agg";

    private ObservationsFields() {

    }
}
