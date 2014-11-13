package ca.uwo.eng.sel.cepsim.integr;

import ca.uwo.eng.sel.cepsim.query.Vertex;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.lists.VmList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CepSimBroker extends DatacenterBroker {
    
	protected static final int TIMER_TAG = 99900 + 20;
	
	
	private boolean isTimerRunning = false;
    private Map<Vertex, Vm> verticesToVm = new HashMap<>();
    
	/** Length of the simulation (in ms)*/
	private double simulationLength;
	
	/** Simulation interval in milliseconds. */
	private double simulationInterval;
    
	public CepSimBroker(String name, double simulationLength, double simulationInterval) throws Exception {
		super(name);

		this.simulationLength = simulationLength;
		this.simulationInterval = simulationInterval;
	}

	
	
	@Override
	public void processEvent(SimEvent ev) {
		// this method to include isTimerRunning verification
		if (!isTimerRunning) {
			isTimerRunning = true;
			//sendNow(getId(), TIMER_TAG);
		}

        switch (ev.getTag()) {
            case CepSimTags.CEP_EVENT_SENT:
                System.out.println(ev.getData());

            default:
                super.processEvent(ev);
                break;
        }

		//super.processEvent(ev);
	}
	
	public Vm getVmAllocation(Vertex v) {
        return verticesToVm.get(v);
    }


    public Integer getDatacenterId(Vm vm) {
        return getVmsToDatacentersMap().get(vm.getId());
    }

	@Override
	protected void submitCloudlets() {
        int vmIndex = 0;
        List<Cloudlet> successfullySubmitted = new ArrayList<Cloudlet>();
        for (Cloudlet cloudlet : getCloudletList()) {
            Vm vm;
            // if user didn't bind this cloudlet and it has not been executed yet
            if (cloudlet.getVmId() == -1) {
                vm = getVmsCreatedList().get(vmIndex);
            } else { // submit to the specific vm
                vm = VmList.getById(getVmsCreatedList(), cloudlet.getVmId());
                if (vm == null) { // vm was not created
                    if(!Log.isDisabled()) {
                        Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Postponing execution of cloudlet ",
                                cloudlet.getCloudletId(), ": bount VM not available");
                    }
                    continue;
                }
            }

            if (!Log.isDisabled()) {
                Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Sending cloudlet ",
                        cloudlet.getCloudletId(), " to VM #", vm.getId());
            }

            cloudlet.setVmId(vm.getId());
            submitCloudlet(vm, cloudlet);

            // --------------------------------------------
            // [WAH] - Added - build a map from Vertices to Vms
            if (cloudlet instanceof CepQueryCloudlet) {
                CepQueryCloudlet cepCloudlet = (CepQueryCloudlet) cloudlet;
                for (Vertex v : cepCloudlet.getVertices()) {
                    verticesToVm.put(v, vm);
                }
            }
            // --------------------------------------------

            cloudletsSubmitted++;


            vmIndex = (vmIndex + 1) % getVmsCreatedList().size();
            getCloudletSubmittedList().add(cloudlet);
            successfullySubmitted.add(cloudlet);
        }

        // remove submitted cloudlets from waiting list
        getCloudletList().removeAll(successfullySubmitted);
	}

	public void submitCloudlet(Vm vm, Cloudlet cloudlet) {
        sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
    }

    @Override
    protected void processOtherEvent(final SimEvent ev) {
        switch (ev.getTag()) {
//            case TIMER_TAG:
//                if (CloudSim.clock() < this.simulationLength) {
//                    send(getId(), this.simulationInterval, TIMER_TAG);
//                }
//                break;
            default:
                super.processOtherEvent(ev);
        }
    }
	

}
