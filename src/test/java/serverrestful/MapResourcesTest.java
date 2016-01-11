package serverrestful;

import java.util.Calendar;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.gson.JsonObject;

import fr.ifremer.sensornanny.getdata.serverrestful.context.CurrentUserProvider;
import fr.ifremer.sensornanny.getdata.serverrestful.context.Role;
import fr.ifremer.sensornanny.getdata.serverrestful.context.User;
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

        JsonObject map = (JsonObject) resource.getObservationsMap("-180.00,-90.74,180.74,80.35", null, null);

        System.out.println(map.get("totalCount"));

        User user = new User();
        user.setLogin("admin");
        user.setRole(Role.ADMIN);

        CurrentUserProvider.put(user);
        map = (JsonObject) resource.getObservationsMap("-180.00,-90.74,180.74,80.35", null, null);

        System.out.println(map.get("totalCount"));
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
    public void queryIgnoreCase() {
        Long resultsUpperCase = resource.getObservations(null, null, "THALASSA").get("totalCount").getAsLong();
        Long resultsLowerCases = resource.getObservations(null, null, "thalassa").get("totalCount").getAsLong();
        Long resultsMixedCases = resource.getObservations(null, null, "tHaLAsSa").get("totalCount").getAsLong();

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
