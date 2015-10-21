package fr.ifremer.sensornanny.getdata.serverrestful.base;

import org.easymock.EasyMock;
import org.easymock.EasyMockRule;
import org.easymock.EasyMockSupport;
import org.easymock.IExpectationSetters;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

public abstract class MockTest extends EasyMockSupport {

    @Rule
    public EasyMockRule rule = new EasyMockRule(this);

    private boolean replayState = false;

    @Before
    public void beforeMockTest() {
        resetAll();
    }

    @After
    public void afterMockTest() {
        if (replayState) {
            verifyAll();
        }
    }

    @Override
    public void resetAll() {
        replayState = false;
        super.resetAll();
    }

    @Override
    public void replayAll() {
        replayState = true;
        super.replayAll();
    }

    /**
     * Syntax sugar method
     * 
     * @see EasyMock#expect(Object)
     */
    public <T> IExpectationSetters<T> expect(T value) {
        return EasyMock.expect(value);
    }

    /**
     * Syntax sugar method
     * 
     * @see EasyMock#expectLastCall()
     */
    public <T> IExpectationSetters<T> expectLastCall() {
        return EasyMock.expectLastCall();

    }

    public <T> T anyObject(Class<T> clazz) {
        return EasyMock.anyObject(clazz);
    }

}
