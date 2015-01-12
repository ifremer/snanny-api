package fr.ifremer.sensornanny.getdata.serverrestful.rest.resources;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import fr.ifremer.sensornanny.getdata.serverrestful.io.couchbase.SystemsDB;

@Path(SystemsResource.PATH)
public class SystemsResource {
	
	private SystemsDB db = new SystemsDB();
	
	public static final String PATH = "/systems";

	/**
	 * Get list of observations
	 * 
	 * @return list of observation in JSON format
	 */
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Object getObservationsBrowser(
//			@QueryParam("bbox") @DefaultValue("") String bboxQuery,
//			@QueryParam("from") @DefaultValue("") String fromQuery,
//			@QueryParam("to") @DefaultValue("") String toQuery
			) {
		
		try {
			return db.getSystems();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return "Error while retrieving systems";
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
	public Object getSystemById(@PathParam("id") String id) {
		return db.getSystem(id);
	}

}
