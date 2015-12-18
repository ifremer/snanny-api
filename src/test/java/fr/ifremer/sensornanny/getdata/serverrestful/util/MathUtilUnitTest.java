package fr.ifremer.sensornanny.getdata.serverrestful.util;

import org.junit.Assert;
import org.junit.Test;

import fr.ifremer.sensornanny.getdata.serverrestful.util.math.MathUtil;

public class MathUtilUnitTest {

    @Test
    public void testFloor() {
        double result = MathUtil.floorTwoDigits(1);
        Assert.assertEquals(1d, result, 0d);

    }

    @Test
    public void testFloorTwoDigits() {
        double result = MathUtil.floorTwoDigits(1.25);
        Assert.assertEquals(1.25d, result, 0d);
    }

    @Test
    public void testFloorThreeDigits() {
        double result = MathUtil.floorTwoDigits(1.251);
        Assert.assertEquals(1.25d, result, 0d);
        result = MathUtil.floorTwoDigits(1.259);
        Assert.assertEquals(1.26d, result, 0d);
    }

    @Test
    public void testFloorLongDigits() {
        double result = MathUtil.floorTwoDigits(Math.PI);
        Assert.assertEquals(3.14, result, 0d);
    }

    @Test
    public void testRatioDigitsPercent() {
        double result = MathUtil.ratioTwoDigits(1, 3);
        Assert.assertEquals(33.33d, result, 0d);
    }

}
