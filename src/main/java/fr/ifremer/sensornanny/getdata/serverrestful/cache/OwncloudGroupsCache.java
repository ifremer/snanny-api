package fr.ifremer.sensornanny.getdata.serverrestful.cache;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import fr.ifremer.sensornanny.getdata.serverrestful.io.rest.OwncloudDao;

/**
 * Cache management for Groups linked to Users in Owncloud system.
 * Recover from owncloud api the groups from the given user id asynchronously.
 * It uses a guava cache system to automatically destroy unused key after 12 hours of inactivity.
 * 5 minutes after been queried, the user is available for refresh which is done on the next query.
 * @author gpagniez
 */
public class OwncloudGroupsCache {

    private LoadingCache<String, List<String>> ocCache;
    private OwncloudDao ocDao;

    private static OwncloudGroupsCache instance;

    private static final Logger LOGGER = Logger.getLogger(OwncloudGroupsCache.class.getName());

    private OwncloudGroupsCache() {
        ocCache = CacheBuilder.newBuilder()
                              .refreshAfterWrite(5, TimeUnit.MINUTES) //at least 5 minutes between each refresh
                              .expireAfterWrite(12, TimeUnit.HOURS)
                              .build(buildCacheLoader());
        ocDao = new OwncloudDao();
    }

    public static OwncloudGroupsCache getInstance() {
        if (instance == null) {
            instance = new OwncloudGroupsCache();
        }
        return instance;
    }

    private CacheLoader<String, List<String>> buildCacheLoader() {
        return new CacheLoader<String, List<String>>() {
            @Override
            public List<String> load(String key) throws Exception {
                //load from owncloud dao if key is not set or should be refreshed
                return ocDao.getUserGroups(key);
            }
        };
    }

    /**
     * Retrieve the list of groups linked to the given user
     * @param userId an owncloud user identifier
     * @return a list of groups
     */
    public List<String> getData(String userId) {
        List<String> result = null;

        try {
            result = ocCache.get(userId);
        } catch (ExecutionException e) {
            LOGGER.warning("Couldn't recover groups for user "+userId+". Error message was "+e.getMessage());
        }

        return result;
    }

}
