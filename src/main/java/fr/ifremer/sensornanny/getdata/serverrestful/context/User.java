package fr.ifremer.sensornanny.getdata.serverrestful.context;

/**
 * Representation of an user with its role
 * 
 * @author athorel
 *
 */
public class User {

    /** Login of the user */
    private String login;

    /** Role of the user */
    private Role role;

    public String getLogin() {
        return login;
    }

    public void setLogin(String login) {
        this.login = login;
    }

    public Role getRole() {
        return role;
    }

    public void setRole(Role role) {
        this.role = role;
    }

    @Override
    public String toString() {
        return "User [login=" + login + "]";
    }

}
