package ca.uwo.eng.sel.cepsim.network;


import ca.uwo.eng.sel.cepsim.integr.CepSimBroker;
import ca.uwo.eng.sel.cepsim.integr.CepSimTags;
import ca.uwo.eng.sel.cepsim.event.EventSet;
import ca.uwo.eng.sel.cepsim.query.EventProducer;
import ca.uwo.eng.sel.cepsim.query.InputVertex;
import ca.uwo.eng.sel.cepsim.query.OutputVertex;
import org.cloudbus.cloudsim.Vm;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;


import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by virso on 2014-11-10.
 */
public class FixedDelayNetworkInterfaceTest {

    @Mock private EventProducer producer;
    @Mock private OutputVertex orig;
    @Mock private InputVertex dest;
    @Mock private Vm destVm;
    @Mock private CepSimBroker broker;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testSendMessage() {

        when(broker.getVmAllocation(dest)).thenReturn(destVm);
        when(broker.getDatacenterId(destVm)).thenReturn(1);

        Map<EventProducer, Object> totals = new HashMap<>();
        totals.put(producer, 5000.0);
        EventSet es = new EventSet(5000.0, 10.0, 1.0, totals);

        FixedDelayNetworkInterface ni = new FixedDelayNetworkInterface(broker, 0.5); // 500 ms

        // this is invoked by CEPSim core - so, it is in ms
        ni.sendMessage(100, orig, dest, es);

        CepNetworkEvent expected = new CepNetworkEvent(0.1, orig, 0.6, dest, es);
        verify(broker).schedule(1, 0.5, CepSimTags.CEP_EVENT_SENT, expected);

    }
}
