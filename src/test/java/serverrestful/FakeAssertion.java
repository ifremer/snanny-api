package serverrestful;

import java.util.Date;
import java.util.Map;

import org.jasig.cas.client.authentication.AttributePrincipal;
import org.jasig.cas.client.authentication.AttributePrincipalImpl;
import org.jasig.cas.client.validation.Assertion;

public class FakeAssertion implements Assertion {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private AttributePrincipalImpl attributePrincipal;

    public FakeAssertion(String name) {
        attributePrincipal = new AttributePrincipalImpl(name);
    }

    @Override
    public Date getValidFromDate() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Date getValidUntilDate() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Map<String, Object> getAttributes() {
        return null;
    }

    @Override
    public AttributePrincipal getPrincipal() {
        return attributePrincipal;
    }

}
