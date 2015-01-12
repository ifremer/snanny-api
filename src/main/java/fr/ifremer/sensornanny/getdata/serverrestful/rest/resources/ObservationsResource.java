package fr.ifremer.sensornanny.getdata.serverrestful.rest.resources;

import java.util.Arrays;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import fr.ifremer.sensornanny.getdata.serverrestful.io.couchbase.ObservationsDB;

@Path(ObservationsResource.PATH)
public class ObservationsResource {
	
	private ObservationsDB db = new ObservationsDB();
	
	public static final String PATH = "/observations";

	/**
	 * Get list of observations
	 * 
	 * @return list of observation in JSON format
	 */
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Object getObservationsBrowser(
			@QueryParam("bbox") @DefaultValue("") String bboxQuery,
			@QueryParam("from") @DefaultValue("") String fromQuery,
			@QueryParam("to") @DefaultValue("") String toQuery
			) {
		
		try {
			return db.getObservations(bboxQuery, fromQuery, toQuery);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return "Error while filtering with " + Arrays.asList(bboxQuery, fromQuery, toQuery);
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
		return db.getObservation(id);
	}

}
