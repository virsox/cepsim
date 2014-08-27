package ca.uwo.eng.sel.cepsim.integr;

import org.cloudbus.cloudsim.ResCloudlet;

public class CepQueryResCloudlet extends ResCloudlet {

	private CepQueryCloudlet cepCloudlet;

	public CepQueryResCloudlet(CepQueryCloudlet cloudlet) {
		super(cloudlet);
		this.cepCloudlet = cloudlet;
	}
	
	public void updateCloudletFinishedSoFar(long instructions, double currentTime, 
			double previousTime, double capacity) {
			
        this.cepCloudlet.updateQuery(instructions, currentTime, previousTime, capacity);
	}
	
	
	public double getEstimatedTimeToFinish() {
		return this.cepCloudlet.getEstimatedTimeToFinish();
	}
	
	@Override
	public long getRemainingCloudletLength() {
		return this.cepCloudlet.getRemainingCloudletLength();
	}
	
	
	
}
