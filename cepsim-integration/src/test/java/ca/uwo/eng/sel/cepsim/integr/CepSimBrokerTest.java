package ca.uwo.eng.sel.cepsim.integr;

import ca.uwo.eng.sel.cepsim.query.Vertex;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Created by virso on 2014-09-14.
 */
public class CepSimBrokerTest {

    @Mock private CepQueryCloudlet cloudlet1;
    @Mock private CepQueryCloudlet cloudlet2;
    @Mock private CepQueryCloudlet cloudlet3;

    @Mock private Vm vm1;
    @Mock private Vm vm2;

    @Mock private Vertex v1, v2, v3, v4;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        when(cloudlet1.getVmId()).thenReturn(-1);
        when(cloudlet2.getVmId()).thenReturn(-1);
        when(cloudlet3.getVmId()).thenReturn(2);

        Set<Vertex> c1Vertices = new HashSet<>();
        c1Vertices.add(v1);
        c1Vertices.add(v2);
        when(cloudlet1.getVertices()).thenReturn(c1Vertices);
        when(cloudlet2.getVertices()).thenReturn(Collections.singleton(v3));
        when(cloudlet3.getVertices()).thenReturn(Collections.singleton(v4));

        when(vm1.getId()).thenReturn(1);
        when(vm2.getId()).thenReturn(2);

        //when(broker.sendNow(anyString(), anyInt(), anyObject())).thenReturn(null);

    }

    @Test
    public void testSubmitCloudlet() throws Exception {
        CloudSim.init(1, Calendar.getInstance(), false, 10);

        List<Cloudlet> cloudlets = new ArrayList<>();
        cloudlets.add(cloudlet1);
        cloudlets.add(cloudlet2);
        cloudlets.add(cloudlet3);

        CepSimBroker broker = spy(new CepSimBroker("broker", 10000, 10));

        List<Vm> createdVms = new ArrayList<>();
        createdVms.add(vm1);
        createdVms.add(vm2);
        when(broker.getVmsCreatedList()).thenReturn(createdVms);
        doNothing().when(broker).submitCloudlet(any(Vm.class), any(Cloudlet.class));

        //when(broker.getVmsToDatacentersMap()).thenReturn(new HashMap<Integer, Integer>());

        broker.submitCloudletList(cloudlets);
        broker.submitCloudlets();


        assertEquals(vm1, broker.getVmAllocation(v1));
        assertEquals(vm1, broker.getVmAllocation(v2));
        assertEquals(vm2, broker.getVmAllocation(v3));
        assertEquals(vm2, broker.getVmAllocation(v4));

        verify(cloudlet3, atLeastOnce()).getVmId();

    }



}
