package ca.uwo.eng.sel.cepsim.integr;

import java.util.ArrayList;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.Consts;
import org.cloudbus.cloudsim.ResCloudlet;
import org.cloudbus.cloudsim.core.CloudSim;

public class CepQueryCloudletScheduler extends CloudletSchedulerTimeShared {
	
	@Override
	public double cloudletSubmit(Cloudlet cloudlet, double fileTransferTime) {

		
		// [WAH] ---------------------------------------- 
		// The ResCloudlet object encapsulates information about the cloudlet execution
		ResCloudlet rcl = null;
		if (cloudlet instanceof CepQueryCloudlet) {
			rcl = new CepQueryResCloudlet((CepQueryCloudlet) cloudlet);	
		} else {
			rcl = new ResCloudlet(cloudlet);
		}
		// ----------------------------------------------
		
		rcl.setCloudletStatus(Cloudlet.INEXEC);		
		getCloudletExecList().add(rcl);
				
		for (int i = 0; i < cloudlet.getNumberOfPes(); i++) {
			rcl.setMachineAndPeId(0, i);
		}

		// [WAH] ----------------------------------------
		if (cloudlet instanceof CepQueryCloudlet) {
			// time to finish 
			return ((CepQueryCloudlet) cloudlet).getDuration();
			
		}
		// ----------------------------------------------
		else {
			// use the current capacity to estimate the extra amount of
			// time to file transferring. It must be added to the cloudlet length
			double extraSize = getCapacity(getCurrentMipsShare()) * fileTransferTime;
			long length = (long) (cloudlet.getCloudletLength() + extraSize);
			cloudlet.setCloudletLength(length);

			return cloudlet.getCloudletLength() / getCapacity(getCurrentMipsShare());			
		}
	}
	
	@Override
	public double updateVmProcessing(double currentTime, List<Double> mipsShare) {
		setCurrentMipsShare(mipsShare);
		double timeSpam = currentTime - getPreviousTime();

		if (getCloudletExecList().size() == 0) {
			setPreviousTime(currentTime);
			return 0.0;
		}

        double peSpeed = mipsShare.get(0); // assume it is always the same
		for (ResCloudlet rcl : getCloudletExecList()) {
			double capacity = getCapacity(mipsShare);

			long instructions = (long) (capacity * timeSpam * rcl.getNumberOfPes() * Consts.MILLION);
			
			// [WAH] ----------------------------------------
			if (rcl instanceof CepQueryResCloudlet) {
				((CepQueryResCloudlet) rcl).updateCloudletFinishedSoFar(instructions, currentTime, 
						getPreviousTime(), capacity);
			}
			// ----------------------------------------------
			else {
				rcl.updateCloudletFinishedSoFar(instructions);
			}
			
		}
		
		// check finished cloudlets
		List<ResCloudlet> toRemove = new ArrayList<ResCloudlet>();
		for (ResCloudlet rcl : getCloudletExecList()) {
			long remainingLength = rcl.getRemainingCloudletLength();
			if (remainingLength == 0) {// finished: remove from the list
				toRemove.add(rcl);
				cloudletFinish(rcl);
				continue;
			}
		}
		getCloudletExecList().removeAll(toRemove);

		// estimate finish time of cloudlets
		double nextEvent = Double.MAX_VALUE;
		for (ResCloudlet rcl : getCloudletExecList()) {
			double estimatedFinishTime = 0;
			// [WAH] ----------------------------------------
			if (rcl instanceof CepQueryResCloudlet) {
				estimatedFinishTime = currentTime + ((CepQueryResCloudlet) rcl).getEstimatedTimeToFinish();
			}
			// ----------------------------------------------
			else {
				estimatedFinishTime = currentTime + (rcl.getRemainingCloudletLength() /
						(getCapacity(mipsShare) * rcl.getNumberOfPes()));

			}			
			if (estimatedFinishTime - currentTime < CloudSim.getMinTimeBetweenEvents()) {
				estimatedFinishTime = currentTime + CloudSim.getMinTimeBetweenEvents();
			}

			if (estimatedFinishTime < nextEvent) {
				nextEvent = estimatedFinishTime;
			}
		}

		setPreviousTime(currentTime);
		return nextEvent;
	}


    // workaround - overriding to increase the visibility from protected to public
    // in the SNAPSHOT version of CloudSim this is not needed, because the method is
    // already public
    public <T extends ResCloudlet> List<T> getCloudletExecList() {
    	return (List<T>) super.getCloudletExecList();
    }

}
