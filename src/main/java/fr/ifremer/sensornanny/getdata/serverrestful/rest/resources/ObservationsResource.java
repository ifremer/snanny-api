package fr.ifremer.sensornanny.getdata.serverrestful.rest.resources;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;

import fr.ifremer.sensornanny.getdata.serverrestful.io.couchbase.ObservationsDB;

@Path(ObservationsResource.PATH)
public class ObservationsResource {

    private static final Logger logger = Logger.getLogger(ObservationsResource.class.getName());

    private ObservationsDB db = new ObservationsDB();

    public static final String PATH = "/observations";

    /**
     * Get list of observations
     * 
     * @return list of observation in JSON format
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Object getObservations(@QueryParam("bbox") @DefaultValue("") String bboxQuery,
            @QueryParam("time") @DefaultValue("") String timeQuery) {

        try {
            return db.getObservations(bboxQuery, timeQuery);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error while retrieving observations filtering with " + Arrays.asList(bboxQuery,
                    timeQuery), e);
        }

        return "Error while filtering with " + Arrays.asList(bboxQuery, timeQuery);
    }

    /**
     * Get list of observations
     * 
     * @return list of observation in JSON format
     */
    @GET
    @Path("synthetic/map")
    @Produces(MediaType.APPLICATION_JSON)
    public Object getObservationsCountMap(@QueryParam("bbox") @DefaultValue("") String bboxQuery,
            @QueryParam("time") @DefaultValue("") String timeQuery) {

        try {
            return db.getObservationsCountForMapZoomUsingCache(bboxQuery, timeQuery);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error while retrieving observations filtering with " + Arrays.asList(bboxQuery,
                    timeQuery), e);
        }

        return "Error while filtering with " + Arrays.asList(bboxQuery, timeQuery);
    }

    /**
     * Get list of observations
     * 
     * @return list of observation in JSON format
     */
    @GET
    @Path("synthetic/timeline")
    @Produces(MediaType.APPLICATION_JSON)
    public Object getObservationsCountTimeline(@QueryParam("bbox") @DefaultValue("") String bboxQuery,
            @QueryParam("time") @DefaultValue("") String timeQuery) {

        try {
            return db.getObservationsCountForTimelineZoomUsingCache(bboxQuery, timeQuery);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error while retrieving observations filtering with " + Arrays.asList(bboxQuery,
                    timeQuery), e);
        }

        return "Error while filtering with " + Arrays.asList(bboxQuery, timeQuery);
    }

    /**
     * Get results of observation which match id
     * 
     * @param id
     *            id of the observation
     * @return results of the observation
     */
    @GET
    @Path("{id}/results")
    public Object results(@PathParam("id") String id) {
        String resultPath = null;
        File file = null;

        try {
            resultPath = db.getResultPath(id);
            file = new File(resultPath);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error while retrieving observation with id = " + id, e);
        }

        if (file != null && file.exists()) {
            ResponseBuilder response = Response.ok((Object) file);
            response.header("Content-Disposition", "attachment; filename=\"" + file.getName() + "\"");
            return response.build();
        } else {
            ResponseBuilder response = Response.status(Status.NOT_FOUND);
            logger.log(Level.SEVERE, "Error while retrieving observation with id = " + id + ", unable to read file "
                    + resultPath);
            response.entity("Unable to read file " + resultPath);
            return response.build();
        }
    }

    /**
     * Get results of observation which match id
     * 
     * @param id
     *            id of the observation
     * @return results of the observation in JSON
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/results.json")
    public JsonArray resultsAsJSON(@PathParam("id") String id) {
        final JsonArray ret = JsonArray.empty();

        BufferedReader resultsReader = null;
        try {
            String resultPath = db.getResultPath(id);
            if (resultPath != null) {
                File resultsFile = new File(resultPath);
                resultsReader = new BufferedReader(new FileReader(resultsFile));

                // FIXME: More generic, as is it assumes it is a CSV file
                resultsReader.lines() // read lines
                        .forEach(new Consumer<String>() {
                            @Override
                            public void accept(String line) {
                                String[] split = line.split(",");

                                if (ret.size() == 0) { // header, initialize
                                    // structure
                                    for (int i = 0; i < split.length - 1; i++) {
                                        ret.add(JsonObject.empty().put("key", split[i + 1]).put("values", JsonArray
                                                .empty()));
                                    }
                                } else { // results, fill structure
                                    for (int i = 1; i < split.length; i++) {
                                        try {
                                            JsonArray timeSeriesValues = ((JsonObject) ret.get(i - 1)).getArray(
                                                    "values");
                                            JsonArray value = JsonArray.empty()
                                                    // FIXME: More generic, as
                                                    // is it assumes time is
                                                    // first field (split[0])
                                                    // and time format is
                                                    // yyyy-MM-dd'T'HH:mm:ss.SSS
                                                    .add(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").parse(
                                                            split[0]).getTime()).add(Double.parseDouble(split[i]));
                                            timeSeriesValues.add(value);
                                        } catch (Exception e) {
                                            logger.log(Level.SEVERE,
                                                    "Error while retrieving results of observation with id = " + id
                                                            + " processing line = " + line, e);
                                        }
                                    }
                                }

                            }
                        });
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error while retrieving results of observation with id = " + id, e);
        } finally {
            if (resultsReader != null) {
                try {
                    resultsReader.close();
                } catch (IOException e) {
                    logger.log(Level.SEVERE, "Error while retrieving results of observation with id = " + id, e);
                }
            }
        }

        return ret;
    }

    /**
     * Get observation by id
     * 
     * @param id
     *            id of the observation
     * @return an observation in JSON format
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    public Object getObservationById(@PathParam("id") String id) {
        try {
            return db.getObservation(id);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error while retrieving observation with id = " + id, e);
        }

        return "Error while retrieving observation with id = " + id;
    }

}
