package fr.ifremer.sensornanny.getdata.serverrestful.util;

import org.elasticsearch.common.geo.GeoPoint;
import org.junit.Assert;
import org.junit.Test;

import fr.ifremer.sensornanny.getdata.serverrestful.dto.ObservationQuery;
import fr.ifremer.sensornanny.getdata.serverrestful.util.query.QueryResolver;

public class QueryResolverUnitTest {

    @Test
    public void testEmptyQuery() {
        ObservationQuery result = QueryResolver.resolveQueryObservation(null, null, null);
        Assert.assertNotNull("result must not be null", result);
        Assert.assertNull("from must be null", result.getFrom());
        Assert.assertNull("to must be null", result.getTo());
        Assert.assertNull("timeFrom must be null", result.getTimeFrom());
        Assert.assertNull("timeTo must be null", result.getTimeTo());
        Assert.assertNull("keywords must be null", result.getKeywords());
    }

    @Test
    public void testBBoxQuery() {
        ObservationQuery result = QueryResolver.resolveQueryObservation("0,1,2,3", null, null);
        Assert.assertNotNull("result must not be null", result);
        GeoPoint from = result.getFrom();
        Assert.assertNotNull("from must not be null", from);
        Assert.assertEquals(0d, from.getLat(), 0);
        Assert.assertEquals(1d, from.getLon(), 0);
        GeoPoint to = result.getTo();
        Assert.assertNotNull("to must not be null", to);
        Assert.assertEquals(2d, to.getLat(), 0);
        Assert.assertEquals(3d, to.getLon(), 0);
        Assert.assertNull("timeFrom must be null", result.getTimeFrom());
        Assert.assertNull("timeTo must be null", result.getTimeTo());
        Assert.assertNull("keywords must be null", result.getKeywords());
    }

    @Test
    public void testTimeQuery() {
        ObservationQuery result = QueryResolver.resolveQueryObservation(null, "123000,456000", null);
        Assert.assertNotNull("result must not be null", result);
        Assert.assertNotNull("result must not be null", result);
        Assert.assertNull("from must be null", result.getFrom());
        Assert.assertNull("to must be null", result.getTo());
        Long timeFrom = result.getTimeFrom();
        Assert.assertNotNull("timeFrom must not be null", timeFrom);
        Assert.assertEquals(Long.valueOf(123000), timeFrom);
        Long timeTo = result.getTimeTo();
        Assert.assertNotNull("timeTo must not be null", timeTo);
        Assert.assertEquals(Long.valueOf(456000), timeTo);
        Assert.assertNull("keywords must be null", result.getKeywords());
    }

    @Test
    public void testKeyWordsQuery() {
        ObservationQuery result = QueryResolver.resolveQueryObservation(null, null, "keywords");
        Assert.assertNotNull("result must not be null", result);
        Assert.assertNull("from must be null", result.getFrom());
        Assert.assertNull("to must be null", result.getTo());
        Assert.assertNull("timeFrom must be null", result.getTimeFrom());
        Assert.assertNull("timeTo must be null", result.getTimeTo());
        String keywords = result.getKeywords();
        Assert.assertEquals("keywords", keywords);

    }
}
