package ca.uwo.eng.sel.cepsim.integr;

import ca.uwo.eng.sel.cepsim.metric.LatencyMetric;
import ca.uwo.eng.sel.cepsim.metric.ThroughputMetric;
import ca.uwo.eng.sel.cepsim.network.CepNetworkEvent;
import ca.uwo.eng.sel.cepsim.network.NetworkInterface;
import ca.uwo.eng.sel.cepsim.query.InputVertex;
import ca.uwo.eng.sel.cepsim.query.OutputVertex;
import ca.uwo.eng.sel.cepsim.query.Query;
import ca.uwo.eng.sel.cepsim.query.Vertex;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModelFull;

import ca.uwo.eng.sel.cepsim.QueryCloudlet;
import ca.uwo.eng.sel.cepsim.history.History;
import scala.Option;
import scala.collection.JavaConversions;

import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import static scala.collection.JavaConversions.asJavaList;



public class CepQueryCloudlet extends Cloudlet {

    private static final UtilizationModelFull UTIL_MODEL_FULL = new UtilizationModelFull();
    private NetworkInterface networkInterface;


    private QueryCloudlet cloudlet;
    private History<History.Entry> history;
    private Queue<CepNetworkEvent> networkEvents;

    private double  executionTime;
    private boolean hasFinished;
  
	public CepQueryCloudlet(int cloudletId, QueryCloudlet cloudlet, boolean record,
                            NetworkInterface networkInterface) {
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
		this.history = new History<>();
        this.executionTime = 0;
        this.hasFinished = false;

		// the vmId can be obtained from the placement
		setVmId(this.cloudlet.placement().vmId());

        // list of network interfaces
        this.networkInterface = networkInterface;

        this.networkEvents = new PriorityQueue<>();
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


//    public Double getThroughput(String queryId) {
//        return ThroughputMetric.calculate(this.getQuery(queryId), this.executionTime);
//    }
//
//    public Double getLatency(String queryId) {
//        return LatencyMetric.calculate(this.getQuery(queryId), this.history);
//    }

	
	public void updateQuery(long instructions, double currentTime, double previousTime, double capacity) {

        // TODO CloudSim uses seconds, and the CepSim core is using milliseconds as time unit
        // This difference has been generating all sorts of bugs! Careful!

        long instructionsToExecute = instructions;
        double previousTimeInMs = previousTime * 1000;

        // it is the first time this method has been invoked
        if (this.executionTime == 0) {
            this.cloudlet.init(previousTimeInMs,
                    LatencyMetric.calculator(this.cloudlet.placement()));
        }
        this.executionTime += (currentTime - previousTime);

        // events have been received
        CepNetworkEvent netEvent = null;
        while (((netEvent = this.networkEvents.peek()) != null) && (netEvent.getDestTimestamp() < previousTime)) {
            this.networkEvents.remove();

            History receivedHistory = this.cloudlet.enqueue(
                    netEvent.getDestTimestamp() * 1000, (InputVertex) netEvent.getDest(),
                    (OutputVertex) netEvent.getOrig(), netEvent.getQuantity());

            history = history.merge(receivedHistory);
        }

        // this means the cepCloudlet has finished between previousTime and the currentTime
        // the 0.01 is a workaround - rounding errors have been preventing the query to finish at the right time
        if (this.getDuration() <= this.executionTime + 0.01) {
            hasFinished = true;
            instructionsToExecute -= ((this.executionTime - this.getDuration()) * instructions) /
                    (currentTime - previousTime);
        }

        // need to transform from seconds to milliseconds
        History<History.Entry> execHistory = this.cloudlet.run(instructionsToExecute, previousTimeInMs, capacity);
        for (History.Entry entry : asJavaList(execHistory)) {
            if (entry instanceof History.Sent) {
                History.Sent sentEntry = (History.Sent) entry;
                this.networkInterface.sendMessage(sentEntry.time() / 1000, sentEntry.v(), sentEntry.dest(), sentEntry.quantity());
            }
        }

        history = history.merge(execHistory);
	}

    public Set<Vertex> getVertices() {
        return JavaConversions.asJavaSet(this.cloudlet.placement().vertices());
    }

    public Set<Query> getQueries() {
        return JavaConversions.asJavaSet(this.cloudlet.placement().queries());
    }

	private Query getQuery(String queryId) {
        Option<Query> option = this.cloudlet.placement().query(queryId);
        if (option.isDefined()) return option.get();
        else throw new IllegalArgumentException("Non-existent queryId");
    }

    public void enqueue(CepNetworkEvent netEvent) {
        this.networkEvents.offer(netEvent);
    }

    public double getLatency(Vertex consumer) {
        return this.cloudlet.metric(LatencyMetric.ID(), consumer);
    }

    public double getThroughput(Vertex consumer) {
        return this.cloudlet.metric(ThroughputMetric.ID(), consumer);
    }
}
