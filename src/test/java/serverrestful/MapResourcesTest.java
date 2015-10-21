package serverrestful;

import java.util.Calendar;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.couchbase.client.java.document.json.JsonObject;

import fr.ifremer.sensornanny.getdata.serverrestful.io.elastic.NodeManager;
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
        resource.getObservationsMap("-40.00,0.74,40.74,80.35", null, null);
    }

    @Test
    public void testGetTimeLine() {
        resource.getObservationsTime("-40.00,0.74,40.74,80.35", null);
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

}
