package ca.uwo.eng.sel.cepsim.integr;

import ca.uwo.eng.sel.cepsim.PlacementExecutor;
import ca.uwo.eng.sel.cepsim.history.History;
import ca.uwo.eng.sel.cepsim.history.SimEvent;
import ca.uwo.eng.sel.cepsim.event.EventSet;
import ca.uwo.eng.sel.cepsim.metric.MetricCalculator;
import ca.uwo.eng.sel.cepsim.network.CepNetworkEvent;
import ca.uwo.eng.sel.cepsim.network.NetworkInterface;
import ca.uwo.eng.sel.cepsim.placement.Placement;
import ca.uwo.eng.sel.cepsim.query.EventConsumer;
import ca.uwo.eng.sel.cepsim.query.EventProducer;
import ca.uwo.eng.sel.cepsim.query.Operator;
import ca.uwo.eng.sel.cepsim.query.Vertex;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import scala.collection.JavaConversions;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyDouble;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;




public class CepQueryCloudletTest {

	@Mock private PlacementExecutor placementExecutor;
    @Mock private MetricCalculator  calculator;
    @Mock private Placement         placement;
    @Mock private EventProducer     p1;
    @Mock private Operator          f1;
    @Mock private EventConsumer     c1;
    @Mock private NetworkInterface  network;

	
	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);
        when(placementExecutor.id()).thenReturn("cl1");
        when(placementExecutor.placement()).thenReturn(placement);


        Set<Vertex> vertices = new HashSet<>();
        vertices.add(p1);
        vertices.add(f1);
        when(placement.vertices()).thenReturn(JavaConversions.asScalaSet(vertices).<Vertex>toSet());
		when(placement.duration()).thenReturn(100L);
	}
	
	@Test
	public void testUpdateCloudlet() {
        when(placementExecutor.run(anyDouble(), anyDouble(), anyDouble())).thenReturn(new History<SimEvent>());

		// 1st invocation
        // long instructions, double currentTime, double previousTime, double capacity
		CepQueryCloudlet cloudlet = new CepQueryCloudlet(1, placementExecutor, false, calculator);
        cloudlet.updateQuery(30_000_000L, 30, 0, 1); // 10s

        verify(placementExecutor).init(0);
		verify(placementExecutor).run(30_000_000L, 0, 1);
		assertEquals(70.0, cloudlet.getEstimatedTimeToFinish(), 0.0001);
		
		// 2nd invocation		
        cloudlet.updateQuery(30_000_000L, 60, 30, 1);
		verify(placementExecutor).run(30_000_000L, 30_000, 1);
		assertEquals(40.0, cloudlet.getEstimatedTimeToFinish(), 0.0001);
		
		// 3rd invocation
        cloudlet.updateQuery(40_000_000L, 100, 60, 1);
		verify(placementExecutor).run(40_000_000L, 60_000, 1);
		assertEquals(0.0, cloudlet.getEstimatedTimeToFinish(), 0.0001);
		assertEquals(0, cloudlet.getRemainingCloudletLength());
	}
	
	@Test
	public void testUpdateCloudletWithExtraTime() {
        when(placementExecutor.run(anyDouble(), anyDouble(), anyDouble())).thenReturn(new History<SimEvent>());

		// 1st invocation
        CepQueryCloudlet cloudlet = new CepQueryCloudlet(1, placementExecutor, false, calculator);
        cloudlet.updateQuery(80_000_000L, 80, 0, 1);

        verify(placementExecutor).init(0);
		verify(placementExecutor).run(80_000_000L, 0, 1);
		assertEquals(20.0, cloudlet.getEstimatedTimeToFinish(), 0.0001);
		
		
		// 2nd invocation		
        cloudlet.updateQuery(100_000_000L, 180, 80, 1);
		verify(placementExecutor).run(20_000_000L, 80000, 1);
		assertEquals(0.0, cloudlet.getEstimatedTimeToFinish(), 0.0001);
		assertEquals(0, cloudlet.getRemainingCloudletLength());
	}


    @Test
    public void testUpdateCloudletWithEventsReceived() {
        when(placementExecutor.run(anyDouble(), anyDouble(), anyDouble())).thenReturn(new History<SimEvent>());

        CepQueryCloudlet cloudlet = new CepQueryCloudlet(1, placementExecutor, false, calculator);

        // enqueue network events
        EventSet es1 = new EventSet(1000, 1.0, 0.0, Collections.<EventProducer, Object>singletonMap(p1, 1000.0));
        EventSet es2 = new EventSet(2000, 1.0, 0.0, Collections.<EventProducer, Object>singletonMap(p1, 2000.0));
        EventSet es3 = new EventSet(5000, 1.0, 0.0, Collections.<EventProducer, Object>singletonMap(p1, 5000.0));

        CepNetworkEvent net1 = new CepNetworkEvent(1.0, p1, 6.0,  f1, es1);
        CepNetworkEvent net2 = new CepNetworkEvent(1.0, p1, 8.0,  f1, es2);
        CepNetworkEvent net3 = new CepNetworkEvent(1.0, p1, 15.0, f1, es3);
        cloudlet.enqueue(net1);
        cloudlet.enqueue(net2);
        cloudlet.enqueue(net3);

        // process them and check if they are correctly processed
        cloudlet.updateQuery(100, 20, 10, 1000);

        verify(placementExecutor).init(10000);
        verify(placementExecutor).enqueue(6000.0, p1, f1, es1);
        verify(placementExecutor).enqueue(8000.0, p1, f1, es2);
        verify(placementExecutor).run(100, 10000, 1000);
    }


    @Test
    public void testGetVertices() {
        CepQueryCloudlet cloudlet = new CepQueryCloudlet(1, placementExecutor, false, calculator);

        Set<Vertex> expected = new HashSet<>();
        expected.add(p1);
        expected.add(f1);

        Set<Vertex> result = cloudlet.getVertices();

        assertEquals(expected.size(), result.size());
        assertTrue(expected.containsAll(result));
    }




}
