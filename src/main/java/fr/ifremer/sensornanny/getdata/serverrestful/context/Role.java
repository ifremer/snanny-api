package fr.ifremer.sensornanny.getdata.serverrestful.context;

/**
 * Permission Role
 * 
 * @author athorel
 *
 */
public enum Role {

    /** Administration role that allow access to full data */
    ADMIN,

    /** Contributeur user that allow access to shared data and owned data */
    CONTRIB;

}
