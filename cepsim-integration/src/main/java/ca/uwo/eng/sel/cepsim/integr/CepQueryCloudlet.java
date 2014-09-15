package ca.uwo.eng.sel.cepsim.integr;

import ca.uwo.eng.sel.cepsim.metric.LatencyMetric;
import ca.uwo.eng.sel.cepsim.metric.ThroughputMetric;
import ca.uwo.eng.sel.cepsim.query.Query;
import ca.uwo.eng.sel.cepsim.query.Vertex;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModelFull;

import ca.uwo.eng.sel.cepsim.QueryCloudlet;
import ca.uwo.eng.sel.cepsim.metric.History;
import scala.Option;
import scala.collection.JavaConversions;

import java.util.Set;

import static scala.collection.JavaConversions.asJavaSet;



public class CepQueryCloudlet extends Cloudlet {
    private static final UtilizationModelFull UTIL_MODEL_FULL = new UtilizationModelFull();

    private QueryCloudlet cloudlet;
    private History history;

    private double  executionTime;
    private boolean hasFinished;
  
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
		this.history = History.apply();
        this.executionTime = 0;
        this.hasFinished = false;

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
	
	/**
	 * Gets the execution history of the cloudlet.
	 * @return execution history of the cloudlet.
	 */
	public History getExecutionHistory() {
		return this.history;
	}

    public double getEstimatedTimeToFinish() {
        return (hasFinished) ? 0 : (this.getDuration() - this.executionTime);
    }

    public long getRemainingCloudletLength() {
        return (hasFinished) ? 0 : Long.MAX_VALUE;
    }


    public Double getThroughput(String queryId) {
        return ThroughputMetric.calculate(this.getQuery(queryId), this.executionTime);
    }

    public Double getLatency(String queryId) {
        return LatencyMetric.calculate(this.getQuery(queryId), this.history);
    }

	
	public void updateQuery(long instructions, double currentTime, double previousTime, double capacity) {

        long instructionsToExecute = instructions;

        this.executionTime += (currentTime - previousTime);

        // this means the cepCloudlet has finished between previousTime and the currentTime
        // the 0.01 is a workaround - rounding errors have been preventing the query to finish at the right time
        if (this.getDuration() <= this.executionTime + 0.01) {
            hasFinished = true;
            instructionsToExecute -= ((this.executionTime - this.getDuration()) * instructions) /
                    (currentTime - previousTime);
        }

        history = history.merge(this.cloudlet.run(instructionsToExecute, previousTime, capacity));
	}

    public Set<Vertex> getVertices() {
        return JavaConversions.asJavaSet(this.cloudlet.placement().vertices());
    }

	private Query getQuery(String queryId) {
        Option<Query> option = this.cloudlet.placement().query(queryId);
        if (option.isDefined()) return option.get();
        else throw new IllegalArgumentException("Non-existent queryId");
    }
	
}
