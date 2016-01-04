package serverrestful;

import java.util.Calendar;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.couchbase.client.java.document.json.JsonObject;

import fr.ifremer.sensornanny.getdata.serverrestful.io.NodeManager;
import fr.ifremer.sensornanny.getdata.serverrestful.rest.resources.ObservationsESResource;

public class MapResourcesTest {

    private NodeManager nodeManager;

    private ObservationsESResource resource;

    @Before
    public void onStartup() {
        nodeManager = new NodeManager();
        nodeManager.contextInitialized(null);
        resource = new ObservationsESResource();
    }

    @After
    public void onShutdown() {
        nodeManager.contextDestroyed(null);
    }

    @Test
    public void testGetGrid() {
        resource.getObservationsMap("-40.00,0.74,40.74,80.35", null, null);
    }

    @Test
    public void testGetTimeLine() {
        Object result = resource.getObservationsTime("-40.00,0.74,40.74,80.35", null);

        System.out.println(result);
    }

    @Test
    public void testQueryFront() {
        resource.getObservations(null, null, "THALASSA");

        resource.getObservations(null, null, null);

        resource.getObservations("-21.54,-46.39,57.56,51.17", null, null);

        resource.getObservations(null, null, "THALASSA");

        resource.getObservations("-21.54,-46.39,27.56,21.17", null, "THALASSA");

        Calendar calendar = Calendar.getInstance();
        calendar.set(2012, 10, 03);

        calendar.add(Calendar.MONTH, 1);
        resource.getObservations("-21.54,-46.39,57.56,51.17", "1298166171428,1312696182857", "THALASSA");

    }

    @Test
    public void testScroll() {

        int count = 0;
        while (count < 5) {
            JsonObject observations = (JsonObject) resource.getObservations("17.81,-22.22,57.36,26.56",
                    "1292923514018,1323991177570", null);

            String scrollId = (String) observations.get("scroll");
            while (scrollId != null) {
                JsonObject observations2 = (JsonObject) resource.getObservations(scrollId);
                scrollId = (String) observations2.get("scroll");
            }
            count++;
        }
    }

    @Test
    public void queryIgnoreCase() {
        Long resultsUpperCase = resource.getObservations(null, null, "THALASSA").getLong("totalCount");
        Long resultsLowerCases = resource.getObservations(null, null, "thalassa").getLong("totalCount");
        Long resultsMixedCases = resource.getObservations(null, null, "tHaLAsSa").getLong("totalCount");

        Assert.assertEquals(resultsUpperCase, resultsMixedCases);
        Assert.assertEquals(resultsUpperCase, resultsLowerCases);

    }

    @Test
    public void queryWithAndElements() {
        Long resultYear = resource.getObservations(null, null, "2012").getLong("totalCount");
        Long resultWordA = resource.getObservations(null, null, "tHaLAsSa").getLong("totalCount");
        Long resultWordAndYear = resource.getObservations(null, null, "tHaLAsSa 2012").getLong("totalCount");

        Assert.assertTrue(resultYear > resultWordAndYear);
        Assert.assertTrue(resultWordA > resultWordAndYear);
    }

}
