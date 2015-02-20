package fr.ifremer.sensornanny.getdata.serverrestful.rest.resources;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;

import fr.ifremer.sensornanny.getdata.serverrestful.io.couchbase.ObservationsDB;

@Path(ResultsResource.PATH)
public class ResultsResource {

	private static final Logger logger = Logger.getLogger(ResultsResource.class.getName());

	private ObservationsDB db = new ObservationsDB();

	public static final String PATH = "/observations";

	/**
	 * Get results of observation which match id
	 * 
	 * @param id
	 *            id of the observation
	 * @return results of the observation
	 */
	@GET
	//@Produces(MediaType.APPLICATION_JSON)
	@Path("{id}/results")
	public Object results(@PathParam("id") String id) {
		final JsonArray ret = JsonArray.empty();
		
		try {
			return new File(db.getResultPath(id));
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Error while retrieving observation with id = " + id, e);
		}

		return ret;
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
				resultsReader
				.lines() // read lines
				.forEach(new Consumer<String>() {
					@Override
					public void accept(String line) {
						String[] split = line.split(",");
						
						if (ret.size() == 0) { // header, initialize structure
							for (int i = 0; i < split.length - 1; i++) {
								ret.add(JsonObject.empty().put("key", split[i + 1]).put("values", JsonArray.empty()));
							}
						} else { // results, fill structure
							for (int i = 1; i < split.length; i++) {
								try {
									JsonArray timeSeriesValues = ((JsonObject) ret.get(i - 1)).getArray("values");
									JsonArray value = JsonArray.empty()
											// FIXME: More generic, as is it assumes time is first field (split[0]) and time format is yyyy-MM-dd'T'HH:mm:ss.SSS
											.add(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").parse(split[0]).getTime()) 
											.add(Double.parseDouble(split[i]))
									;
									timeSeriesValues.add(value);
								} catch (Exception e) {
									logger.log(Level.SEVERE, "Error while retrieving results of observation with id = " + id + " processing line = " + line, e);
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

}
