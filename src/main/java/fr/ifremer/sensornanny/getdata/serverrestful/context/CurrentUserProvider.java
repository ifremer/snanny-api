package fr.ifremer.sensornanny.getdata.serverrestful.context;

/**
 * Util class that wrap context for usage in the whole application
 * 
 * @author athorel
 *
 */
public class CurrentUserProvider {

    /***
     * Thread local to keep context for a transaction
     */
    private static ThreadLocal<User> threadLocalUser = new ThreadLocal<>();

    /**
     * Return the connected user
     * 
     * @return user with role if connected otherwise <code>null</code>
     */
    public static User get() {
        return threadLocalUser.get();
    }

    /**
     * Allow to specify the current user at the beginning of the transaction
     * 
     * @param user current user at the beginning
     */
    public static void put(User user) {
        threadLocalUser.set(user);
    }

    /**
     * Allow to clear the current user at the end of the transaction
     */
    public static void clear() {
        threadLocalUser.remove();
    }

}
