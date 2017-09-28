package fr.ifremer.sensornanny.getdata.serverrestful.io.rest;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.GsonHttpMessageConverter;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import fr.ifremer.sensornanny.getdata.serverrestful.Config;

/**
 * OwnCloud Data Access Layer
 * @author gpagniez
 */
public class OwncloudDao {

    // auth
    private static final String BASIC_HEADER = "Basic ";
    private static final String AUTHORIZATION_HEADER = "Authorization";
    // services

    private static final Logger LOGGER = Logger.getLogger(OwncloudDao.class.getName());

    private RestTemplate init(HttpMessageConverter<?> converter) {
        RestTemplate restTemplate = new RestTemplate();
        if (converter != null) {
            restTemplate.setMessageConverters(Collections.singletonList(converter));
        }
        return restTemplate;
    }

    /**
     * Call the OwnCloud API endpoint /users/{user_id}/groups
     * @param key ${user_id}
     * @return A list of strings
     */
    public List<String> getUserGroups(String key) {
        URI uri = UriComponentsBuilder.fromHttpUrl(Config.owncloudEndpoint())
                                      .path("/users/")
                                      .path(key)
                                      .path("/groups")
                                      // GetUri
                                      .build().encode().toUri();

        return get(uri, ArrayList.class, new GsonHttpMessageConverter(), "/user/:userid/groups");
    }

    /**
     * Allow to call rest template using headers with authentication
     *
     * @param uri   URI to acces with
     * @param clazz returned class
     * @return result of the get action
     */
    private <T> T get(URI uri, Class<T> clazz, HttpMessageConverter<?> converter, String resourceName) {
        RestTemplate template = init(converter);
        if (resourceName != null) {
            template.setErrorHandler(OwncloudRestErrorHandler.of(resourceName));
        }

        LOGGER.info("Call " + uri);
        ResponseEntity<T> response = template.exchange(uri, HttpMethod.GET, createEntity(), clazz);

        return response.getBody();

    }

    private HttpEntity<?> createEntity() {
        HttpHeaders headers = new HttpHeaders();
        headers.set(AUTHORIZATION_HEADER, BASIC_HEADER + Config.owncloudCredentials());
        return new HttpEntity<>(headers);
    }
}

