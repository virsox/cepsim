package ca.uwo.eng.sel.cepsim.integr;

import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.core.SimEvent;

public class CepSimBroker extends DatacenterBroker {
    
	protected static final int TIMER_TAG = 99900 + 20;
	
	
	private boolean isTimerRunning = false;
    
    
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
		
		super.processEvent(ev);
	}
	
	

	@Override
	protected void submitCloudlets() {
		super.submitCloudlets();
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
