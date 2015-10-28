package fr.ifremer.sensornanny.getdata.serverrestful.dto;

/**
 * Create a point degrees division to allow grouping siblings elements together
 * 
 * @author athorel
 *
 */
public class DegreesDivision {

    /** Start latitute of division */
    private double lat;
    /** Start longitude of division */
    private double lon;

    public DegreesDivision(double lat, double lon) {
        super();
        this.lat = lat;
        this.lon = lon;
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public double getLon() {
        return lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        long temp;
        temp = Double.doubleToLongBits(lat);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(lon);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        DegreesDivision other = (DegreesDivision) obj;
        if (Double.doubleToLongBits(lat) != Double.doubleToLongBits(other.lat))
            return false;
        if (Double.doubleToLongBits(lon) != Double.doubleToLongBits(other.lon))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "[" + lat + ", " + lon + "]";
    }

}
