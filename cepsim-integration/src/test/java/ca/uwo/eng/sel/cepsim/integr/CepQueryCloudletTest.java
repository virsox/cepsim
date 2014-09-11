package ca.uwo.eng.sel.cepsim.integr;

import ca.uwo.eng.sel.cepsim.QueryCloudlet;
import ca.uwo.eng.sel.cepsim.metric.History;
import ca.uwo.eng.sel.cepsim.placement.Placement;
import ca.uwo.eng.sel.cepsim.query.EventProducer;
import ca.uwo.eng.sel.cepsim.query.Operator;
import ca.uwo.eng.sel.cepsim.query.Vertex;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import scala.collection.JavaConversions;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyDouble;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;




public class CepQueryCloudletTest {

	@Mock private QueryCloudlet queryCloudlet;
    @Mock private Placement     placement;
    @Mock private EventProducer p1;
    @Mock private Operator      f1;

	
	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);
        when(queryCloudlet.placement()).thenReturn(placement);
        when(queryCloudlet.run(anyDouble(), anyDouble(), anyDouble())).thenReturn(History.apply());

        Set<Vertex> vertices = new HashSet<>();
        vertices.add(p1);
        vertices.add(f1);
        when(placement.vertices()).thenReturn(JavaConversions.asScalaSet(vertices).<Vertex>toSet());
		when(placement.duration()).thenReturn(100L);
	}
	
	@Test
	public void testUpdateCloudlet() {

		// 1st invocation
		CepQueryCloudlet cloudlet = new CepQueryCloudlet(1, queryCloudlet, false);
        cloudlet.updateQuery(100, 30, 0, 1000);
		
		verify(queryCloudlet).run(100, 0, 1000);
		assertEquals(70.0, cloudlet.getEstimatedTimeToFinish(), 0.0001);
		
		// 2nd invocation		
        cloudlet.updateQuery(100, 60, 30, 1000);
		verify(queryCloudlet).run(100, 30, 1000);
		assertEquals(40.0, cloudlet.getEstimatedTimeToFinish(), 0.0001);
		
		// 3rd invocation
        cloudlet.updateQuery(100, 100, 60, 1000);
		verify(queryCloudlet).run(100, 60, 1000);
		assertEquals(0.0, cloudlet.getEstimatedTimeToFinish(), 0.0001);
		assertEquals(0, cloudlet.getRemainingCloudletLength());
	}
	
	@Test
	public void testUpdateCloudletWithExtraTime() {
		// 1st invocation
        CepQueryCloudlet cloudlet = new CepQueryCloudlet(1, queryCloudlet, false);
        cloudlet.updateQuery(100, 80, 0, 1000);
		
		verify(queryCloudlet).run(100, 0, 1000);
		assertEquals(20.0, cloudlet.getEstimatedTimeToFinish(), 0.0001);
		
		
		// 2nd invocation		
        cloudlet.updateQuery(100, 180, 80, 1000);
		verify(queryCloudlet).run(20, 80, 1000);
		assertEquals(0.0, cloudlet.getEstimatedTimeToFinish(), 0.0001);
		assertEquals(0, cloudlet.getRemainingCloudletLength());
	}
	
    @Test
    public void testGetVertices() {
        CepQueryCloudlet cloudlet = new CepQueryCloudlet(1, queryCloudlet, false);

        Set<Vertex> expected = new HashSet<>();
        expected.add(p1);
        expected.add(f1);

        Set<Vertex> result = cloudlet.getVertices();

        assertEquals(expected.size(), result.size());
        assertTrue(expected.containsAll(result));
    }




}
