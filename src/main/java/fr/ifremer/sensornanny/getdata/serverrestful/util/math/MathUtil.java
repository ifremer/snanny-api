package fr.ifremer.sensornanny.getdata.serverrestful.util.math;

public class MathUtil {

    private static final int PERCENT = 100;

    public static double floorTwoDigits(double value) {
        return ((double) java.lang.Math.round(value * PERCENT)) / PERCENT;
    }

    public static double ratioTwoDigits(double value, double divider) {
        return floorTwoDigits((value * PERCENT) / divider);
    }

}
