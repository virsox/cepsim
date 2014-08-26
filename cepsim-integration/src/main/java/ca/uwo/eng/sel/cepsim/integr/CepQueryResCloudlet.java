package ca.uwo.eng.sel.cepsim.integr;

import org.cloudbus.cloudsim.ResCloudlet;

public class CepQueryResCloudlet extends ResCloudlet {

	private CepQueryCloudlet cepCloudlet;
	
	private double  executionTime;
	private boolean hasFinished;
	
	public CepQueryResCloudlet(CepQueryCloudlet cloudlet) {
		super(cloudlet);
		
		this.cepCloudlet = cloudlet;
		this.executionTime = 0;
		this.hasFinished = false;
	}
	
	public void updateCloudletFinishedSoFar(long instructions, double currentTime, 
			double previousTime, double capacity) {
			
		long instructionsToExecute = instructions;
		this.executionTime += (currentTime - previousTime);
		
		// this means the cepCloudlet has finished between previousTime and the currentTime
		if (this.cepCloudlet.getDuration() <= this.executionTime) {
			hasFinished = true;
			instructionsToExecute -=
					((this.executionTime - this.cepCloudlet.getDuration()) * instructions) / 
					(currentTime - previousTime); 
		}
		

		this.cepCloudlet.updateQuery(instructionsToExecute, previousTime, capacity);

	}
	
	
	public double getEstimatedTimeToFinish() {
		return (hasFinished) ? 0 : (this.cepCloudlet.getDuration() - this.executionTime);
	}
	
	@Override
	public long getRemainingCloudletLength() {
		return (hasFinished) ? 0 : Long.MAX_VALUE; 
	}
	
	
	
}
