package ca.uwo.eng.sel.cepsim.integr;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;


public class CepQueryResCloudletTest {

	@Mock private CepQueryCloudlet cepCloudlet;
	
	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);
		when(cepCloudlet.getDuration()).thenReturn(100L);
	}
	
	@Test
	public void testUpdateCloudlet() {
		
//		Set<Vertex> vertices = new HashSet<>();
//		vertices = vertices.$plus()
//		
//		
//		Map<Vertex, Set<Edge>> edges = new HashMap<>();
//		
//		Query q = new Query(vertices, edges, 10L);
				
		// 1st invocation
		CepQueryResCloudlet resCloudlet = new CepQueryResCloudlet(cepCloudlet); 		
		resCloudlet.updateCloudletFinishedSoFar(100, 30, 0, 1000);		
		
		verify(cepCloudlet).updateQuery(100, 0, 1000);		
		assertEquals(70.0, resCloudlet.getEstimatedTimeToFinish(), 0.0001);
		
		// 2nd invocation		
		resCloudlet.updateCloudletFinishedSoFar(100, 60, 30, 1000);
		verify(cepCloudlet).updateQuery(100, 30, 1000);		
		assertEquals(40.0, resCloudlet.getEstimatedTimeToFinish(), 0.0001);		
		
		// 3rd invocation
		resCloudlet.updateCloudletFinishedSoFar(100, 100, 60, 1000);
		verify(cepCloudlet).updateQuery(100, 60, 1000);		
		assertEquals(0.0, resCloudlet.getEstimatedTimeToFinish(), 0.0001);
		assertEquals(0, resCloudlet.getRemainingCloudletLength());		
	}
	
	@Test
	public void testUpdateCloudletWithExtraTime() {
		// 1st invocation
		CepQueryResCloudlet resCloudlet = new CepQueryResCloudlet(cepCloudlet); 		
		resCloudlet.updateCloudletFinishedSoFar(100, 80, 0, 1000);		
		
		verify(cepCloudlet).updateQuery(100, 0, 1000);		
		assertEquals(20.0, resCloudlet.getEstimatedTimeToFinish(), 0.0001);
		
		
		// 2nd invocation		
		resCloudlet.updateCloudletFinishedSoFar(100, 180, 80, 1000);
		verify(cepCloudlet).updateQuery(20, 80, 1000);		
		assertEquals(0.0, resCloudlet.getEstimatedTimeToFinish(), 0.0001);		
		assertEquals(0, resCloudlet.getRemainingCloudletLength());
	}
	
	
}
