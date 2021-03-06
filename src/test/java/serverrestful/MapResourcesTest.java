package serverrestful;

import java.util.Calendar;

import org.jasig.cas.client.util.AssertionHolder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.gson.JsonObject;

import fr.ifremer.sensornanny.getdata.serverrestful.io.NodeManager;
import fr.ifremer.sensornanny.getdata.serverrestful.rest.resources.ObservationsESResource;

public class MapResourcesTest {

    private static NodeManager nodeManager;

    private static ObservationsESResource resource;

    @BeforeClass
    public static void onStartup() {
        nodeManager = new NodeManager();
        nodeManager.contextInitialized(null);
        resource = new ObservationsESResource();
    }

    @AfterClass
    public static void onShutdown() {
        nodeManager.contextDestroyed(null);
    }

    @Test
    public void testGetGrid() {

        JsonObject map = (JsonObject) resource.getObservations("-180.00,-90.74,180.74,80.35", null, null);
        System.out.println(map.get("totalCount"));
    }

    @Test
    public void testGetGridWithUserFilter(){

        AssertionHolder.setAssertion(new FakeAssertion("admin"));
        JsonObject map = resource.getObservations("-76.44,-172.9,76.44,172.9", null, null);
        Assert.assertEquals(map.get("status").getAsString(), "tooMany");

        AssertionHolder.setAssertion(new FakeAssertion("ljhsfsfjsdfh"));
        map = resource.getObservations("-76.44,-172.9,76.44,172.9", null, null);
        Assert.assertEquals(map.get("status").getAsString(), "empty");
    }

    @Test
    public void testGetTimeLine() {
        Object result = resource.getObservationsTime("-76.44,-150.03,76.44,150.03", null, null);
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
    public void queryIgnoreCase() {
        JsonObject observations = resource.getObservations(null, null, "Borel");
        Long resultsUpperCase = observations.get("totalCount").getAsLong();
        Long resultsLowerCases = resource.getObservations(null, null, "borel").get("totalCount").getAsLong();
        Long resultsMixedCases = resource.getObservations(null, null, "BorEL").get("totalCount").getAsLong();

        Assert.assertEquals(resultsUpperCase, resultsMixedCases);
        Assert.assertEquals(resultsUpperCase, resultsLowerCases);

    }

    @Test
    public void queryWithAndElements() {
        Long resultYear = resource.getObservations(null, null, "2012").get("totalCount").getAsLong();
        Long resultWordA = resource.getObservations(null, null, "tHaLAsSa").get("totalCount").getAsLong();
        Long resultWordAndYear = resource.getObservations(null, null, "tHaLAsSa 2012").get("totalCount").getAsLong();

        Assert.assertTrue(resultYear > resultWordAndYear);
        Assert.assertTrue(resultWordA > resultWordAndYear);
    }

}
