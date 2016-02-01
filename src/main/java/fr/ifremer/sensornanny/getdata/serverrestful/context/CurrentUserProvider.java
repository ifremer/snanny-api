package fr.ifremer.sensornanny.getdata.serverrestful.context;

import java.util.Collection;

import org.jasig.cas.client.util.AssertionHolder;
import org.jasig.cas.client.validation.Assertion;

import fr.ifremer.sensornanny.getdata.serverrestful.Config;

/**
 * Util class that wrap context for usage in the whole application
 * 
 * @author athorel
 *
 */
public class CurrentUserProvider {

    /**
     * Return the connected user
     * 
     * @return user with role if connected otherwise <code>null</code>
     */
    public static User get() {
        Assertion assertion = AssertionHolder.getAssertion();
        if (assertion != null) {
            String name = assertion.getPrincipal().getName();
            // Check role using whitelist
            Collection<String> adminWhitelist = Config.casAdminWhitelist();
            Role role = (adminWhitelist != null && adminWhitelist.contains(name)) ? Role.ADMIN : Role.CONTRIB;
            // Return user
            return new User(name, role);
        }
        return null;
    }

}
