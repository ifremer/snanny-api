package fr.ifremer.sensornanny.getdata.serverrestful.filter;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.FilterRegistration.Dynamic;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.jasig.cas.client.authentication.AuthenticationFilter;
import org.jasig.cas.client.util.AssertionThreadLocalFilter;
import org.jasig.cas.client.util.HttpServletRequestWrapperFilter;
import org.jasig.cas.client.validation.Cas20ProxyReceivingTicketValidationFilter;

import fr.ifremer.sensornanny.getdata.serverrestful.Config;

public class FilterContextListener implements ServletContextListener {

    @Override
    public void contextInitialized(ServletContextEvent sce) {

        ServletContext servletContext = sce.getServletContext();
        Map<String, String> configs = new HashMap<>();
        configs.put("casServerLoginUrl", Config.casAuthUrl() + "/cas/login");
        configs.put("casServerLogoutUrl", Config.casAuthUrl() + "/cas/logout");
        configs.put("serverName", Config.casServerName());
        configs.put("casServerUrlPrefix", Config.casAuthUrl() + "/cas");

        // Filter authentication
        Dynamic filter = servletContext.addFilter("auth", AuthenticationFilter.class);
        filter.setInitParameters(configs);
        filter.addMappingForUrlPatterns(null, true, "/register.jsp");

        // Filter logout
        filter = servletContext.addFilter("logout", LogoutFilter.class);
        filter.setInitParameters(configs);
        filter.addMappingForUrlPatterns(null, true, "/unregister.jsp");

        // Filter ticket validation
        filter = servletContext.addFilter("validation", Cas20ProxyReceivingTicketValidationFilter.class);
        filter.setInitParameters(configs);
        filter.addMappingForUrlPatterns(null, true, "/*");

        // No initialConfig
        servletContext.addFilter("wrapper", HttpServletRequestWrapperFilter.class)
                //
                .addMappingForUrlPatterns(null, true, "/*");

        servletContext.addFilter("threadLocal", AssertionThreadLocalFilter.class)
                //
                .addMappingForUrlPatterns(null, true, "/*");

    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        // TODO Auto-generated method stub

    }

    /**
     * <filter>
     * <filter-name>CAS Authentication Filter</filter-name>
     * <filter-class>org.jasig.cas.client.authentication.AuthenticationFilter</filter-class>
     * <init-param>
     * <param-name>casServerLoginUrl</param-name>
     * <param-value>https://auth.ifremer.fr/cas/login</param-value>
     * </init-param>
     * <init-param>
     * <param-name>serverName</param-name>
     * <param-value>http://localhost.ifremer.fr:8080</param-value>
     * </init-param>
     * </filter>
     * 
     * <filter>
     * <filter-name>CAS Logout Filter</filter-name>
     * <filter-class>fr.ifremer.sensornanny.getdata.serverrestful.filter.LogoutFilter</filter-class>
     * <init-param>
     * <param-name>casServerLogoutUrl</param-name>
     * <param-value>https://auth.ifremer.fr/cas/logout</param-value>
     * </init-param>
     * <init-param>
     * <param-name>serverName</param-name>
     * <param-value>http://localhost.ifremer.fr:8080</param-value>
     * </init-param>
     * </filter>
     * 
     * <filter>
     * <filter-name>CAS Validation Filter</filter-name>
     * <filter-class>org.jasig.cas.client.validation.Cas20ProxyReceivingTicketValidationFilter</filter-class>
     * <init-param>
     * <param-name>casServerUrlPrefix</param-name>
     * <param-value>https://auth.ifremer.fr/cas</param-value>
     * </init-param>
     * <init-param>
     * <param-name>serverName</param-name>
     * <param-value>http://localhost.ifremer.fr:8080</param-value>
     * </init-param>
     * </filter>
     * 
     * <filter>
     * <filter-name>CAS HttpServletRequest Wrapper Filter</filter-name>
     * <filter-class>org.jasig.cas.client.util.HttpServletRequestWrapperFilter</filter-class>
     * </filter>
     * 
     * <filter>
     * <filter-name>CAS Assertion Thread Local Filter</filter-name>
     * <filter-class>org.jasig.cas.client.util.AssertionThreadLocalFilter</filter-class>
     * </filter>
     * 
     * 
     * <filter-mapping>
     * <filter-name>CAS Authentication Filter</filter-name>
     * <url-pattern>/register.jsp</url-pattern>
     * </filter-mapping>
     * 
     * <filter-mapping>
     * <filter-name>CAS Logout Filter</filter-name>
     * <url-pattern>/unregister.jsp</url-pattern>
     * </filter-mapping>
     * 
     * <!-- Ticket validation on every items -->
     * <filter-mapping>
     * <filter-name>CAS Validation Filter</filter-name>
     * <url-pattern>/*</url-pattern>
     * </filter-mapping>
     * 
     * <filter-mapping>
     * <filter-name>CAS HttpServletRequest Wrapper Filter</filter-name>
     * <url-pattern>/*</url-pattern>
     * </filter-mapping>
     * 
     * <!-- Add in thread local assertion -->
     * <filter-mapping>
     * <filter-name>CAS Assertion Thread Local Filter</filter-name>
     * <url-pattern>/*</url-pattern>
     * </filter-mapping>
     */

}
