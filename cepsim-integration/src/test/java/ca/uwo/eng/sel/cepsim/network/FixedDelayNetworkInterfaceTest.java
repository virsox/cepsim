package ca.uwo.eng.sel.cepsim.network;


import ca.uwo.eng.sel.cepsim.integr.CepSimBroker;
import ca.uwo.eng.sel.cepsim.integr.CepSimTags;
import ca.uwo.eng.sel.cepsim.query.Vertex;
import org.cloudbus.cloudsim.Vm;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by virso on 2014-11-10.
 */
public class FixedDelayNetworkInterfaceTest {

    @Mock private Vertex orig;
    @Mock private Vertex dest;
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

        FixedDelayNetworkInterface ni = new FixedDelayNetworkInterface(broker, 50);
        ni.sendMessage(100, orig, dest, 5000);

        CepNetworkEvent expected = new CepNetworkEvent(100, orig, 150, dest, 5000);
        verify(broker).schedule(1, 50, CepSimTags.CEP_EVENT_SENT, expected);

    }
}
