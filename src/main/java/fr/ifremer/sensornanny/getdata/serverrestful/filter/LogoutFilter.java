package fr.ifremer.sensornanny.getdata.serverrestful.filter;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.jasig.cas.client.util.AbstractCasFilter;
import org.jasig.cas.client.util.CommonUtils;

/**
 * Filter implementation to intercept all requests and attempt to authenticate
 * the user by redirecting them to CAS (unless the user has a ticket).
 * <p>
 * This filter allows you to specify the following parameters (at either the context-level or the filter-level):
 * <ul>
 * <li><code>casServerLoginUrl</code> - the url to log into CAS, i.e. https://cas.rutgers.edu/login</li>
 * <li><code>renew</code> - true/false on whether to use renew or not.</li>
 * <li><code>gateway</code> - true/false on whether to use gateway or not.</li>
 * </ul>
 *
 * <p>
 * Please see AbstractCasFilter for additional properties.
 * </p>
 *
 * @author Scott Battaglia
 * @version $Revision: 11768 $ $Date: 2007-02-07 15:44:16 -0500 (Wed, 07 Feb 2007) $
 * @since 3.0
 */
public class LogoutFilter extends AbstractCasFilter {

    /**
     * The URL to CAS Server logout
     */
    private String casServerLogoutUrl;

    protected void initInternal(final FilterConfig filterConfig) throws ServletException {
        if (!isIgnoreInitConfiguration()) {
            super.initInternal(filterConfig);
            // Logout URL
            setCasServerLogoutUrl(getPropertyFromInitParams(filterConfig, "casServerLogoutUrl", null));
            log.trace("Loaded CasServerLogoutUrl parameter: " + this.casServerLogoutUrl);
        }
    }

    public void init() {
        super.init();
        CommonUtils.assertNotNull(this.casServerLogoutUrl, "casServerLogoutUrl cannot be null.");
    }

    public final void doFilter(final ServletRequest servletRequest, final ServletResponse servletResponse,
            final FilterChain filterChain) throws IOException, ServletException {
        final HttpServletRequest request = (HttpServletRequest) servletRequest;
        final HttpServletResponse response = (HttpServletResponse) servletResponse;
        final HttpSession session = request.getSession(false);
        // Check unregister fct
        final String serviceUrl = request.getParameter("bf");
        if(session != null) {
            session.removeAttribute(CONST_CAS_ASSERTION);
        }

        final String urlToRedirectTo = CommonUtils.constructRedirectUrl(this.casServerLogoutUrl,
                getServiceParameterName(), serviceUrl, false, false);

        response.sendRedirect(urlToRedirectTo);
    }

    public final void setCasServerLogoutUrl(final String casServerLogoutUrl) {
        this.casServerLogoutUrl = casServerLogoutUrl;
    }

}
