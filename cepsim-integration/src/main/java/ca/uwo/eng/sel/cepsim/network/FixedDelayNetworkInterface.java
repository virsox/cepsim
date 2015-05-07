package ca.uwo.eng.sel.cepsim.network;

import ca.uwo.eng.sel.cepsim.integr.CepSimBroker;
import ca.uwo.eng.sel.cepsim.integr.CepSimTags;
import ca.uwo.eng.sel.cepsim.event.EventSet;
import ca.uwo.eng.sel.cepsim.query.InputVertex;
import ca.uwo.eng.sel.cepsim.query.OutputVertex;
import org.cloudbus.cloudsim.Vm;

/**
 * Created by virso on 2014-11-10.
 */
public class FixedDelayNetworkInterface implements NetworkInterface {

    private CepSimBroker broker;
    private double delay; // in seconds


    public FixedDelayNetworkInterface(CepSimBroker broker, double delay) {
        this.broker = broker;
        this.delay = delay;
    }

    @Override
    public void sendMessage(double timestamp, OutputVertex orig, InputVertex dest, EventSet eventSet) {
        Vm destVm = broker.getVmAllocation(dest);

        Integer datacenterId = broker.getDatacenterId(destVm);
        double start = timestamp / 1000.0; // transform to seconds

        // destination, delay, tag, content
        broker.schedule(datacenterId, this.delay, CepSimTags.CEP_EVENT_SENT,
                new CepNetworkEvent(start, orig, start + this.delay, dest, eventSet));
    }

}
