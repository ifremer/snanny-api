package fr.ifremer.sensornanny.getdata.serverrestful.rest.resources;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.gson.JsonObject;

import fr.ifremer.sensornanny.getdata.serverrestful.context.CurrentUserProvider;
import fr.ifremer.sensornanny.getdata.serverrestful.context.User;

/**
 * User rest services
 * 
 * @author athorel
 *
 */
@Path("/user")
public class UserResources {

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JsonObject getUser() {

        JsonObject result = new JsonObject();
        User user = CurrentUserProvider.get();

        result.addProperty("logged", user != null);
        if (user != null) {
            result.addProperty("user", user.getLogin());
        }
        return result;
    }
}
