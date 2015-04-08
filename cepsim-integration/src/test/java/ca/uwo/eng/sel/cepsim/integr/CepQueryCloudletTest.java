package ca.uwo.eng.sel.cepsim.integr;

import ca.uwo.eng.sel.cepsim.QueryCloudlet;
import ca.uwo.eng.sel.cepsim.history.History;
import ca.uwo.eng.sel.cepsim.history.SimEvent;
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

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyDouble;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;




public class CepQueryCloudletTest {

	@Mock private QueryCloudlet    queryCloudlet;
    @Mock private MetricCalculator calculator;
    @Mock private Placement        placement;
    @Mock private EventProducer    p1;
    @Mock private Operator         f1;
    @Mock private EventConsumer c1;
    @Mock private NetworkInterface network;

	
	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);
        when(queryCloudlet.id()).thenReturn("cl1");
        when(queryCloudlet.placement()).thenReturn(placement);


        Set<Vertex> vertices = new HashSet<>();
        vertices.add(p1);
        vertices.add(f1);
        when(placement.vertices()).thenReturn(JavaConversions.asScalaSet(vertices).<Vertex>toSet());
		when(placement.duration()).thenReturn(100L);
	}
	
	@Test
	public void testUpdateCloudlet() {
        when(queryCloudlet.run(anyDouble(), anyDouble(), anyDouble())).thenReturn(new History<SimEvent>());

		// 1st invocation
        // long instructions, double currentTime, double previousTime, double capacity
		CepQueryCloudlet cloudlet = new CepQueryCloudlet(1, queryCloudlet, false, network);
        cloudlet.setMetricCalculator(calculator);
        cloudlet.updateQuery(100, 30, 0, 1000);

        verify(queryCloudlet).init(0, calculator);
		verify(queryCloudlet).run(100, 0, 1000);
		assertEquals(70.0, cloudlet.getEstimatedTimeToFinish(), 0.0001);
		
		// 2nd invocation		
        cloudlet.updateQuery(100, 60, 30, 1000);
		verify(queryCloudlet).run(100, 30000, 1000);
		assertEquals(40.0, cloudlet.getEstimatedTimeToFinish(), 0.0001);
		
		// 3rd invocation
        cloudlet.updateQuery(100, 100, 60, 1000);
		verify(queryCloudlet).run(100, 60000, 1000);
		assertEquals(0.0, cloudlet.getEstimatedTimeToFinish(), 0.0001);
		assertEquals(0, cloudlet.getRemainingCloudletLength());
	}
	
	@Test
	public void testUpdateCloudletWithExtraTime() {
        when(queryCloudlet.run(anyDouble(), anyDouble(), anyDouble())).thenReturn(new History<SimEvent>());

		// 1st invocation
        CepQueryCloudlet cloudlet = new CepQueryCloudlet(1, queryCloudlet, false, network);
        cloudlet.setMetricCalculator(calculator);
        cloudlet.updateQuery(100, 80, 0, 1000);

        verify(queryCloudlet).init(0, calculator);
		verify(queryCloudlet).run(100, 0, 1000);
		assertEquals(20.0, cloudlet.getEstimatedTimeToFinish(), 0.0001);
		
		
		// 2nd invocation		
        cloudlet.updateQuery(100, 180, 80, 1000);
		verify(queryCloudlet).run(20, 80000, 1000);
		assertEquals(0.0, cloudlet.getEstimatedTimeToFinish(), 0.0001);
		assertEquals(0, cloudlet.getRemainingCloudletLength());
	}


    // TODO review these two tests when dealing with networked queries
//    @Test
//    public void testUpdateCloudletWithEventsSent() {
//        History history = new History<SimEvent>();
//        history = history.logProcessed("cl1", 0.0, f1, 1000);
//        history = history.logSent("cl1", 1000.0, f1, c1,  1000);
//        when(queryCloudlet.run(anyDouble(), anyDouble(), anyDouble())).thenReturn(history);
//
//        CepQueryCloudlet cloudlet = new CepQueryCloudlet(1, queryCloudlet, false, network);
//        cloudlet.setMetricCalculator(calculator);
//        cloudlet.updateQuery(100, 30, 0, 1000);
//
//        verify(queryCloudlet).init(0, calculator);
//        verify(queryCloudlet).run(100, 0, 1000);
//        verify(network).sendMessage(1.0, f1, c1, 1000);
//    }
//
//    @Test
//    public void testUpdateCloudletWithEventsReceived() {
//        CepQueryCloudlet cloudlet = new CepQueryCloudlet(1, queryCloudlet, false, network);
//        cloudlet.setMetricCalculator(calculator);
//
//        // enqueue network events
//        CepNetworkEvent net1 = new CepNetworkEvent(1.0, p1, 6.0,  f1, 1000);
//        CepNetworkEvent net2 = new CepNetworkEvent(1.0, p1, 8.0,  f1, 2000);
//        CepNetworkEvent net3 = new CepNetworkEvent(1.0, p1, 15.0, f1, 5000);
//        cloudlet.enqueue(net1);
//        cloudlet.enqueue(net2);
//        cloudlet.enqueue(net3);
//
//        // configure the cloudlet to return histories
//        History<History.Entry> hist1 = new History<>();
//        hist1 = hist1.logReceived("cl1", 6000.0, f1, p1, 1000);
//
//        History<History.Entry> hist2 = new History<>();
//        hist2 = hist2.logReceived("cl1", 8000.0, f1, p1, 2000);
//
//        when(queryCloudlet.enqueue(6000.0, f1, p1, 1000)).thenReturn(hist1);
//        when(queryCloudlet.enqueue(8000.0, f1, p1, 2000)).thenReturn(hist2);
//
//        History history = new History<History.Entry>();
//        when(queryCloudlet.run(anyDouble(), anyDouble(), anyDouble())).thenReturn(history);
//
//
//        // process them and check if they are correctly processed
//        cloudlet.updateQuery(100, 20, 10, 1000);
//
//        verify(queryCloudlet).init(10000, calculator);
//        verify(queryCloudlet).enqueue(6000.0, f1, p1, 1000);
//        verify(queryCloudlet).enqueue(8000.0, f1, p1, 2000);
//        verify(queryCloudlet).run(100, 10000, 1000);
//
//        //instructions: Double, startTime: Double, capacity: Double)
//
//    }


    @Test
    public void testGetVertices() {
        CepQueryCloudlet cloudlet = new CepQueryCloudlet(1, queryCloudlet, false, network);

        Set<Vertex> expected = new HashSet<>();
        expected.add(p1);
        expected.add(f1);

        Set<Vertex> result = cloudlet.getVertices();

        assertEquals(expected.size(), result.size());
        assertTrue(expected.containsAll(result));
    }




}
