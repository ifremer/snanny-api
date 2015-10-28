package fr.ifremer.sensornanny.getdata.serverrestful.resources;

import org.easymock.Mock;
import org.easymock.TestSubject;
import org.elasticsearch.ElasticsearchException;
import org.junit.Assert;
import org.junit.Test;

import com.couchbase.client.java.document.json.JsonObject;

import fr.ifremer.sensornanny.getdata.serverrestful.base.MockTest;
import fr.ifremer.sensornanny.getdata.serverrestful.dto.ObservationQuery;
import fr.ifremer.sensornanny.getdata.serverrestful.dto.RequestStatuts;
import fr.ifremer.sensornanny.getdata.serverrestful.io.elastic.ObservationsSearch;
import fr.ifremer.sensornanny.getdata.serverrestful.rest.resources.ObservationsESResource;

public class MapResourcesMockTest extends MockTest {

    @TestSubject
    private ObservationsESResource resource = new ObservationsESResource();

    @Mock
    ObservationsSearch observationSearch;

    @Test
    public void testEmptyObservations() {
        expect(observationSearch.getObservations(anyObject(ObservationQuery.class))).andThrow(
                new ElasticsearchException(""));
        replayAll();
        JsonObject observations = resource.getObservations("", "", "");
        Assert.assertEquals(RequestStatuts.TIMEOUT.toString(), observations.get("status"));
    }

}
