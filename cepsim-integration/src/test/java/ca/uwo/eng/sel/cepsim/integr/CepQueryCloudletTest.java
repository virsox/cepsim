package ca.uwo.eng.sel.cepsim.integr;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyDouble;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ca.uwo.eng.sel.cepsim.QueryCloudlet;
import ca.uwo.eng.sel.cepsim.metric.History;
import ca.uwo.eng.sel.cepsim.placement.Placement;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;


public class CepQueryCloudletTest {

	@Mock private QueryCloudlet queryCloudlet;
    @Mock private Placement     placement;

	
	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);
        when(queryCloudlet.placement()).thenReturn(placement);
        when(queryCloudlet.run(anyDouble(), anyDouble(), anyDouble())).thenReturn(History.apply());
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
	
	
}
