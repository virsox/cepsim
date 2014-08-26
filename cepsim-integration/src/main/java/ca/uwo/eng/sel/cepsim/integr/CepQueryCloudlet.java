package ca.uwo.eng.sel.cepsim.integr;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModelFull;

import ca.uwo.eng.sel.cepsim.QueryCloudlet;



public class CepQueryCloudlet extends Cloudlet {
    private static final UtilizationModelFull UTIL_MODEL_FULL = new UtilizationModelFull();

    private QueryCloudlet cloudlet;

  
	public CepQueryCloudlet(int cloudletId, QueryCloudlet cloudlet, boolean record) {		
		// we are passing some "default parameters" for the following arguments
		
		// cloudletLength = Long.MAX_VALUE (this value is not used for CepCloudlets)
		// pesNumber = 1
		// cloudletFileSize = 0
		// cloudletOutputSize = 0
		// utilizationModelCpu = UtilizationModelFull
		// utilizationModelRam = UtilizationModelFull
		// utilizationModelBw =	UtilizationModelFull	
		super (cloudletId, Long.MAX_VALUE, 1,
				0, 0, UTIL_MODEL_FULL, UTIL_MODEL_FULL,
				UTIL_MODEL_FULL, record);
				
		this.cloudlet = cloudlet;
		
		// the vmId can be obtained from the placement
		setVmId(this.cloudlet.placement().vmId());
	}

	
	/**
	 * Get the duration (in seconds) of this cloudlet. It delegates the calculation to
	 * the placement. 
	 * @return duration (in seconds) of this cloudlet. 
	 */
	public long getDuration() {
		return cloudlet.placement().duration();
	}
	
	public void updateQuery(double instructions, double startTime, double capacity) {	
		this.cloudlet.run(instructions, startTime, capacity);
	}
	
	
	
}
