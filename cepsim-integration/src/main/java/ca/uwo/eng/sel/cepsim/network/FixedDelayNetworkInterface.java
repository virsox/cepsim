package ca.uwo.eng.sel.cepsim.network;

import ca.uwo.eng.sel.cepsim.integr.CepSimBroker;
import ca.uwo.eng.sel.cepsim.integr.CepSimTags;
import ca.uwo.eng.sel.cepsim.query.Vertex;
import org.cloudbus.cloudsim.Vm;

/**
 * Created by virso on 2014-11-10.
 */
public class FixedDelayNetworkInterface implements NetworkInterface {

    private CepSimBroker broker;
    private double delay;

    public FixedDelayNetworkInterface(CepSimBroker broker, double delay) {
        this.broker = broker;
        this.delay = delay;
    }

    @Override
    public void sendMessage(double timestamp, Vertex orig, Vertex dest, int quantity) {
        //Vm origVm = broker.getVmAllocation(orig);
        Vm destVm = broker.getVmAllocation(dest);

        Integer datacenterId = broker.getDatacenterId(destVm);

        // destination, delay, tag, content
        broker.schedule(datacenterId, this.delay, CepSimTags.CEP_EVENT_SENT,
                new CepNetworkEvent(timestamp, orig, timestamp + this.delay, dest, quantity));
    }

}
