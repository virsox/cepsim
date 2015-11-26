package ca.uwo.eng.sel.cepsim.integr;

import ca.uwo.eng.sel.cepsim.PlacementExecutor;
import ca.uwo.eng.sel.cepsim.history.History;
import ca.uwo.eng.sel.cepsim.history.SimEvent;
import ca.uwo.eng.sel.cepsim.metric.LatencyMetric;
import ca.uwo.eng.sel.cepsim.metric.LatencyThroughputCalculator;
import ca.uwo.eng.sel.cepsim.metric.MetricCalculator;
import ca.uwo.eng.sel.cepsim.metric.ThroughputMetric;
import ca.uwo.eng.sel.cepsim.network.CepNetworkEvent;
import ca.uwo.eng.sel.cepsim.query.Query;
import ca.uwo.eng.sel.cepsim.query.Vertex;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModelFull;
import scala.Option;
import scala.collection.JavaConversions;

import java.util.*;



public class CepQueryCloudlet extends Cloudlet {

    private static final UtilizationModelFull UTIL_MODEL_FULL = new UtilizationModelFull();


    private PlacementExecutor executor;
    private History<SimEvent> history;
    private Queue<CepNetworkEvent> networkEvents;

    private double  executionTime;
    private boolean hasFinished;

    private boolean record;

    public CepQueryCloudlet(int cloudletId, PlacementExecutor executor, int pesNumber,
                            boolean record, MetricCalculator calculator) {
        // we are passing some "default parameters" for the following arguments

        // cloudletLength = Long.MAX_VALUE (this value is not used for CepCloudlets)
        // pesNumber = 1
        // cloudletFileSize = 0
        // cloudletOutputSize = 0
        // utilizationModelCpu = UtilizationModelFull
        // utilizationModelRam = UtilizationModelFull
        // utilizationModelBw =	UtilizationModelFull
        super (cloudletId, Long.MAX_VALUE, pesNumber,
                0, 0, UTIL_MODEL_FULL, UTIL_MODEL_FULL,
                UTIL_MODEL_FULL, record);

        this.record = record;

        this.executor = executor;
        this.history = new History<>();
        this.executionTime = 0;
        this.hasFinished = false;

        // the vmId can be obtained from the placement
        setVmId(this.executor.placement().vmId());

        // list of network interfaces
        this.networkEvents = new PriorityQueue<>();

        this.executor.registerCalculator(calculator);
    }


    public CepQueryCloudlet(int cloudletId, PlacementExecutor executor, boolean record, MetricCalculator calculator) {
        this(cloudletId, executor, 1, record, calculator);
    }

    public CepQueryCloudlet(int cloudletId, PlacementExecutor executor, int pesNumber, boolean record) {
        this(cloudletId, executor, pesNumber, record, LatencyThroughputCalculator.apply(executor.placement()));
    }


    public CepQueryCloudlet(int cloudletId, PlacementExecutor executor, boolean record) {
		this(cloudletId, executor, 1, record, LatencyThroughputCalculator.apply(executor.placement()));
	}

	
	/**
	 * Get the duration (in seconds) of this cloudlet. It delegates the calculation to
	 * the placement. 
	 * @return duration (in seconds) of this cloudlet. 
	 */
	public long getDuration() {
		return executor.placement().duration();
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


	public void updateQuery(long instructions, double currentTime, double previousTime, double capacity) {
//        if ((getCloudletId() == 1) && (currentTime >= 150.0) && (currentTime < 150.1)) {
//            System.gc();
//            System.out.println("XX-Memory [" + Runtime.getRuntime().totalMemory() + ", "
//                    + Runtime.getRuntime().freeMemory() + ", "
//                    + Runtime.getRuntime().maxMemory() + "]-XX");
////            try {
////                System.in.read();
////            } catch (IOException e) {
////                e.printStackTrace();
////            }
//        }

        // CloudSim uses seconds, and the CepSim core is using milliseconds as time unit
        long instructionsToExecute = instructions;
        double previousTimeInMs = previousTime * 1000;

        // it is the first time this method has been invoked
        if (this.executionTime == 0) {
            this.executor.init(previousTimeInMs);
        }
        this.executionTime += (currentTime - previousTime);

        // events have been received
        CepNetworkEvent netEvent = null;
        while (((netEvent = this.networkEvents.peek()) != null) && (netEvent.getDestTimestamp() < previousTime)) {
            this.networkEvents.remove();

            // need to transform back into ms
            this.executor.enqueue(netEvent.getDestTimestamp() * 1000, netEvent.getOrig(),
                  netEvent.getDest(), netEvent.getEventSet());
        }

        // this means the cepCloudlet has finished between previousTime and the currentTime
        // the 0.01 is a workaround - rounding errors have been preventing the query to finish at the right time
        if (this.getDuration() <= this.executionTime + 0.01) {
            hasFinished = true;
            instructionsToExecute -= ((this.executionTime - this.getDuration()) * instructions) /
                    (currentTime - previousTime);
        }

        // need to transform from seconds to milliseconds
        History<SimEvent> execHistory = this.executor.run(instructionsToExecute, previousTimeInMs, capacity);
        if (record) {
            history.append(execHistory);
        }
	}

    public Set<Vertex> getVertices() {
        return JavaConversions.asJavaSet(this.executor.placement().vertices());
    }

    public Set<Query> getQueries() {
        return JavaConversions.asJavaSet(this.executor.placement().queries());
    }

	private Query getQuery(String queryId) {
        Option<Query> option = this.executor.placement().query(queryId);
        if (option.isDefined()) return option.get();
        else throw new IllegalArgumentException("Non-existent queryId");
    }

    public void enqueue(CepNetworkEvent netEvent) {
        this.networkEvents.offer(netEvent);
    }

    public double getLatency(Vertex consumer) {
        return this.executor.metric(LatencyMetric.ID(), consumer);
    }

    public SortedMap<Integer, Double> getLatencyByMinute(Vertex consumer) {
        Map<Object, Object> tmpMap = JavaConversions.asJavaMap(this.executor.metrics(LatencyMetric.ID(), consumer));
        SortedMap<Integer, Double> sorted = new TreeMap<>();
        for (Map.Entry<Object, Object> entry : tmpMap.entrySet()) {
            sorted.put((Integer) entry.getKey(), (Double) entry.getValue());
        }
        return sorted;
    }

    public double getThroughput(Vertex consumer) {
        return this.executor.metric(ThroughputMetric.ID(), consumer);
    }

    public SortedMap<Integer, Double> getThroughputByMinute(Vertex consumer) {
        Map<Object, Object> tmpMap = JavaConversions.asJavaMap(this.executor.metrics(ThroughputMetric.ID(), consumer));
        SortedMap<Integer, Double> sorted = new TreeMap<>();
        for (Map.Entry<Object, Object> entry : tmpMap.entrySet()) {
            sorted.put((Integer) entry.getKey(), (Double) entry.getValue());
        }
        return sorted;
    }


}
