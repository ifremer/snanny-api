package fr.ifremer.sensornanny.getdata.serverrestful.rest.resources;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import fr.ifremer.sensornanny.getdata.serverrestful.io.couchbase.SystemsDB;

@Path(SystemsResource.PATH)
public class SystemsResource {
	
	private static final Logger logger = Logger.getLogger(SystemsResource.class.getName());
	
	private SystemsDB db = new SystemsDB();
	
	public static final String PATH = "/systems";

	/**
	 * Get list of systems
	 * 
	 * @return list of systems in JSON format
	 */
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Object getSystems(
//			@QueryParam("bbox") @DefaultValue("") String bboxQuery,
//			@QueryParam("from") @DefaultValue("") String fromQuery,
//			@QueryParam("to") @DefaultValue("") String toQuery
			) {
		
		try {
			return db.getSystems();
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Error while retrieving systems", e);
		}
		
		return "Error while retrieving systems";
	}

	/**
	 * Get system by id
	 * 
	 * @param id
	 *            id of the system
	 * @return a system in JSON format
	 */
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("{id}")
	public Object getSystemById(@PathParam("id") String id) {
		try {
			return db.getSystem(id);
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Error while retrieving system with id = " + id, e);
		}
		
		return "Error while retrieving system with id = " + id; 
	}

}
