package ca.uwo.eng.sel.cepsim.integr;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ca.uwo.eng.sel.cepsim.network.CepNetworkEvent;
import ca.uwo.eng.sel.cepsim.query.Vertex;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletScheduler;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicy;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;

public class CepSimDatacenter extends Datacenter {


    private Map<Vertex, CepQueryCloudlet> vertexToCloudlet = new HashMap<>();

	public CepSimDatacenter(String name,
			DatacenterCharacteristics characteristics,
			VmAllocationPolicy vmAllocationPolicy,
			List<Storage> storageList,
			double schedulingInterval) throws Exception {
		super(name, characteristics, vmAllocationPolicy, storageList, schedulingInterval);
	}


    @Override
    protected void processOtherEvent(SimEvent ev) {
        switch (ev.getTag()) {
            case CepSimTags.CEP_EVENT_SENT:
                this.processCepEventSent(ev);
                break;
            default:
                super.processOtherEvent(ev);
                break;
        }
    }

    private void processCepEventSent(SimEvent ev) {
        CepNetworkEvent netEvent = (CepNetworkEvent) ev.getData();

        CepQueryCloudlet cloudlet = vertexToCloudlet.get(netEvent.getDest());
        if (cloudlet == null) {
            throw new IllegalStateException("Vertex not found in any cloudlet");
        }

        cloudlet.enqueue(netEvent);
    }


    @Override
	protected void processCloudletSubmit(SimEvent ev, boolean ack) {
		updateCloudletProcessing();

		try {
			// gets the Cloudlet object
			Cloudlet cl = (Cloudlet) ev.getData();

			// checks whether this Cloudlet has finished or not
			if (cl.isFinished()) {
				String name = CloudSim.getEntityName(cl.getUserId());
				Log.printConcatLine(getName(), ": Warning - Cloudlet #", cl.getCloudletId(), " owned by ", name,
						" is already completed/finished.");
				Log.printLine("Therefore, it is not being executed again");
				Log.printLine();

				// NOTE: If a Cloudlet has finished, then it won't be processed.
				// So, if ack is required, this method sends back a result.
				// If ack is not required, this method don't send back a result.
				// Hence, this might cause CloudSim to be hanged since waiting
				// for this Cloudlet back.
				if (ack) {
					int[] data = new int[3];
					data[0] = getId();
					data[1] = cl.getCloudletId();
					data[2] = CloudSimTags.FALSE;

					// unique tag = operation tag
					int tag = CloudSimTags.CLOUDLET_SUBMIT_ACK;
					sendNow(cl.getUserId(), tag, data);
				}

				sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_RETURN, cl);

				return;
			}

			// process this Cloudlet to this CloudResource
			cl.setResourceParameter(getId(), getCharacteristics().getCostPerSecond(), getCharacteristics()
					.getCostPerBw());

			int userId = cl.getUserId();
			int vmId = cl.getVmId();

			// time to transfer the files
			double fileTransferTime = predictFileTransferTime(cl.getRequiredFiles());

			Host host = getVmAllocationPolicy().getHost(vmId, userId);
			Vm vm = host.getVm(vmId, userId);
			CloudletScheduler scheduler = vm.getCloudletScheduler();
			double estimatedFinishTime = scheduler.cloudletSubmit(cl, fileTransferTime);

			// if this cloudlet is in the exec queue
			if (estimatedFinishTime > 0.0 && !Double.isInfinite(estimatedFinishTime)) {
				estimatedFinishTime += fileTransferTime;								
				send(getId(), estimatedFinishTime, CloudSimTags.VM_DATACENTER_EVENT);
			}
			
			//  [WAH] --------  Added -------------------------------------------------------
			send(getId(), this.getSchedulingInterval(), CloudSimTags.VM_DATACENTER_EVENT);
            if (cl instanceof CepQueryCloudlet) {
                CepQueryCloudlet cepCl = (CepQueryCloudlet) cl;
                for (Vertex v : cepCl.getVertices()) {
                    this.vertexToCloudlet.put(v, cepCl);
                }
            }
            // -------------------------------------------------------------------------------
			
			if (ack) {
				int[] data = new int[3];
				data[0] = getId();
				data[1] = cl.getCloudletId();
				data[2] = CloudSimTags.TRUE;

				// unique tag = operation tag
				int tag = CloudSimTags.CLOUDLET_SUBMIT_ACK;
				sendNow(cl.getUserId(), tag, data);
			}
		} catch (ClassCastException c) {
			Log.printLine(getName() + ".processCloudletSubmit(): " + "ClassCastException error.");
			c.printStackTrace();
		} catch (Exception e) {
			Log.printLine(getName() + ".processCloudletSubmit(): " + "Exception error.");
			e.printStackTrace();
		}

		checkCloudletCompletion();
	}
	

	@Override
	protected void updateCloudletProcessing() {
		// if some time passed since last processing
		// R: for term is to allow loop at simulation start. Otherwise, one initial
		// simulation step is skipped and schedulers are not properly initialized
		if (CloudSim.clock() < 0.111 ||
				CloudSim.clock() >= getLastProcessTime() + CloudSim.getMinTimeBetweenEvents()) {
			
			List<? extends Host> list = getVmAllocationPolicy().getHostList();
			double smallestTime = Double.MAX_VALUE;
						
			// for each host...
			for (int i = 0; i < list.size(); i++) {
				Host host = list.get(i);				
				// inform VMs to update processing
				double time = host.updateVmsProcessing(CloudSim.clock());
				
				// what time do we expect that the next cloudlet will finish?
				if (time < smallestTime) {
					smallestTime = time;
				}
			}
			
			// smallestTime == Double.MAX_VALUE when there are no cloudlets to be executed
			// (it happens in the beginning and at the end of simulation)
			if (smallestTime != Double.MAX_VALUE) {
				// guarantee that the next event is sent periodically
				if (smallestTime > CloudSim.clock() + this.getSchedulingInterval()) {
					smallestTime = CloudSim.clock() + this.getSchedulingInterval();
				}
				
				schedule(getId(), smallestTime - CloudSim.clock(), CloudSimTags.VM_DATACENTER_EVENT);				
			}
			
			
			setLastProcessTime(CloudSim.clock());
		}
	}
}
