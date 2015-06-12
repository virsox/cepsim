package ca.uwo.eng.sel.cepsim.example;

import ca.uwo.eng.sel.cepsim.QueryCloudlet;
import ca.uwo.eng.sel.cepsim.gen.Generator;
import ca.uwo.eng.sel.cepsim.gen.UniformGenerator;
import ca.uwo.eng.sel.cepsim.history.History;
import ca.uwo.eng.sel.cepsim.integr.CepQueryCloudlet;
import ca.uwo.eng.sel.cepsim.integr.CepQueryCloudletScheduler;
import ca.uwo.eng.sel.cepsim.integr.CepSimBroker;
import ca.uwo.eng.sel.cepsim.integr.CepSimDatacenter;
import ca.uwo.eng.sel.cepsim.placement.Placement;
import ca.uwo.eng.sel.cepsim.query.*;
import ca.uwo.eng.sel.cepsim.sched.DefaultOpScheduleStrategy;
import ca.uwo.eng.sel.cepsim.sched.DynOpScheduleStrategy;
import ca.uwo.eng.sel.cepsim.sched.OpScheduleStrategy;
import ca.uwo.eng.sel.cepsim.sched.alloc.AllocationStrategy;
import ca.uwo.eng.sel.cepsim.sched.alloc.UniformAllocationStrategy;
import ca.uwo.eng.sel.cepsim.sched.alloc.WeightedAllocationStrategy;
import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import scala.Tuple3;

import java.text.DecimalFormat;
import java.util.*;


public class SimParametersTest {

    //private static final Double SIM_INTERVAL = 0.1;
    private static final Long DURATION = 301L;


    private static final int VM_NUMBER = 1;
    private static final int QUERIES_PER_VM = 100;


    public enum SchedStrategyEnum {
        DEFAULT, DYNAMIC
    }

    public enum AllocStrategyEnum {
        UNIFORM, WEIGHTED
    }



	public static void main(String[] args) {
		new SimParametersTest().simulate(SchedStrategyEnum.DYNAMIC, AllocStrategyEnum.UNIFORM, 0.01, 1);
	}


	public void simulate(SchedStrategyEnum schedStrategy, AllocStrategyEnum allocStrategy,
                         double simInterval, int iterations) {
		Log.printLine("Starting CepSimAvgWindow...");


		try {
			// First step: Initialize the CloudSim package. It should be called before creating any entities.
			int num_user = 1; // number of cloud users
			Calendar calendar = Calendar.getInstance(); // Calendar whose fields have been initialized with the current date and time.
 			boolean trace_flag = false; // trace events


			CloudSim.init(num_user, calendar, trace_flag, simInterval);

			// Second step: Create Datacenters
			// Datacenters are the resource providers in CloudSim. We need at
			// list one of them to run a CloudSim simulation
			Datacenter datacenter0 = createDatacenter("Datacenter_0", simInterval);

			// Third step: Create Broker
			DatacenterBroker broker = createBroker(simInterval);
			int brokerId = broker.getId();

			// Fourth step: Create one virtual machine
			List<Vm> vmlist = new ArrayList<Vm>();
            for (int i = 1; i <= VM_NUMBER; i++) {
                // VM description
                int vmid = i;
                int mips = 2500;
                long size = 10000; // image size (MB)
                int ram = 1024; // vm memory (MB)
                long bw = 100;
                int pesNumber = 1; // number of cpus
                String vmm = "Xen"; // VMM name

                // create VM
                Vm vm = new Vm(vmid, brokerId, mips, pesNumber, ram, bw, size, vmm, new CepQueryCloudletScheduler());
                vmlist.add(vm);
            }

			// submit vm list to the broker
			broker.submitVmList(vmlist);

			// Fifth step: Create one Cloudlet
			List<Cloudlet> cloudletList = new ArrayList<Cloudlet>();
			cloudletList.addAll(createCloudlets(brokerId, schedStrategy, allocStrategy, iterations));

			// submit cloudlet list to the broker
			broker.submitCloudletList(cloudletList);

			// Sixth step: Starts the simulation
			long start = System.nanoTime();
			CloudSim.startSimulation();
			CloudSim.stopSimulation();
			double time = (System.nanoTime() - start) / (1E6);

			System.out.println("Time [" + time + "]");

			//Final step: Print results when simulation is over
			List<Cloudlet> newList = broker.getCloudletReceivedList();
			printCloudletList(newList);


			for (Cloudlet cl : newList) {
                if (!(cl instanceof CepQueryCloudlet)) continue;

				CepQueryCloudlet cepCl = (CepQueryCloudlet) cl;

                Query q = cepCl.getQueries().iterator().next();
                Vertex consumer = q.consumers().head();

                History history = cepCl.getExecutionHistory().from(consumer);

                System.out.println("Latencies: " + cepCl.getLatencyByMinute(consumer));
                System.out.println("Throughputs: " + cepCl.getThroughputByMinute(consumer));
			}

			Log.printLine("CloudSimExample1 finished!");
		} catch (Exception e) {
			e.printStackTrace();
			Log.printLine("Unwanted errors happen");
		}
	}



	private static Set<Cloudlet> createCloudlets(int brokerId, SchedStrategyEnum schedStrategy,
                                                 AllocStrategyEnum allocStrategy, int iterations) {
		// 100_000_000 I / interval
		// 100 events / interval

        final int MAX_QUERIES = 1;
        final int NUM_SENSORS = 1000;

		Set<Cloudlet> cloudlets = new HashSet<>();
        Set<Query> queries = new HashSet<Query>();
        Map<Vertex, Object> weights = new HashMap<>();


        for (int i = 1; i <= VM_NUMBER; i++) {

            for (int j = 1; j <= QUERIES_PER_VM; j++) {

                int id = ((i - 1) * QUERIES_PER_VM) + j;

                Generator gen = new UniformGenerator(NUM_SENSORS * 10);
//                EventProducer p = new EventProducer("spout" + j, 10_000, gen, true);
//                Operator jsonParser = new Operator("jsonParser" + j, 41_250, 2048);
//                Operator validate = new Operator("validate" + j, 25_000, 2048);
//                Operator xml = new Operator("xmlOutput" + j, 31_250, 2048);
//                EventConsumer c = new EventConsumer("end" + j, 10_000, 2048);
//
//                Set<Vertex> vertices = new HashSet<>();
//                vertices.add(p);
//                vertices.add(jsonParser);
//                vertices.add(validate);
//                vertices.add(xml);
//                vertices.add(c);
//
//                Tuple3<OutputVertex, InputVertex, Object> e1 = new Tuple3<OutputVertex, InputVertex, Object>(p, jsonParser, 1.0);
//                Tuple3<OutputVertex, InputVertex, Object> e2 = new Tuple3<OutputVertex, InputVertex, Object>(jsonParser, validate, 1.0);
//                Tuple3<OutputVertex, InputVertex, Object> e3 = new Tuple3<OutputVertex, InputVertex, Object>(validate, xml, 0.95);
//                Tuple3<OutputVertex, InputVertex, Object> e4 = new Tuple3<OutputVertex, InputVertex, Object>(xml, c, 1.0);
//
//                Set<Tuple3<OutputVertex, InputVertex, Object>> edges = new HashSet<>();
//                edges.add(e1);
//                edges.add(e2);
//                edges.add(e3);
//                edges.add(e4);
//
//
//                Query q = Query.apply("testjson" + j, vertices, edges, DURATION);
//                queries.add(q);

                EventProducer p = new EventProducer("spout" + id, 1_000, gen, true);
                Operator outlierDetector = new Operator("outlierDetector" + id, 18_000, 2048);
                Operator average = WindowedOperator.apply("average" + id, 18_000, 15000, 15000, WindowedOperator.constant(NUM_SENSORS), 2048);
                Operator db = new Operator("db" + id, 11_000_000, 2048);
                EventConsumer c = new EventConsumer("end" + id, 1_000, 2048);

                Set<Vertex> vertices = new HashSet<>();
                vertices.add(p);
                vertices.add(outlierDetector);
                vertices.add(average);
                vertices.add(db);
                vertices.add(c);

                Tuple3<OutputVertex, InputVertex, Object> e1 = new Tuple3<OutputVertex, InputVertex, Object>(p, outlierDetector, 1.0);
                Tuple3<OutputVertex, InputVertex, Object> e2 = new Tuple3<OutputVertex, InputVertex, Object>(outlierDetector, average, 0.95);
                Tuple3<OutputVertex, InputVertex, Object> e3 = new Tuple3<OutputVertex, InputVertex, Object>(average, db, 1.0);
                Tuple3<OutputVertex, InputVertex, Object> e4 = new Tuple3<OutputVertex, InputVertex, Object>(db, c, 1.0);

                Set<Tuple3<OutputVertex, InputVertex, Object>> edges = new HashSet<>();
                edges.add(e1);
                edges.add(e2);
                edges.add(e3);
                edges.add(e4);

                Query q = Query.apply("testavg" + id, vertices, edges, DURATION);
                queries.add(q);
            }
            Placement placement = Placement.withQueries(queries, i);

            // ----------------------- parameters --------------------------------------
            AllocationStrategy aStrategy = (allocStrategy == AllocStrategyEnum.UNIFORM)
                    ? UniformAllocationStrategy.apply() : WeightedAllocationStrategy.apply();

            OpScheduleStrategy sStrategy = (schedStrategy == SchedStrategyEnum.DEFAULT)
                    ? DefaultOpScheduleStrategy.apply(aStrategy) : DynOpScheduleStrategy.apply(aStrategy);

            QueryCloudlet qCloudlet = QueryCloudlet.apply("cl" + i, placement, sStrategy, iterations);
            // -------------------------------------------------------------------------


            CepQueryCloudlet cloudlet = new CepQueryCloudlet(i, qCloudlet, false);
            cloudlet.setUserId(brokerId);
            cloudlets.add(cloudlet);
        }

        return cloudlets;
	}
	
	/**
	 * Creates the datacenter.
	 *
	 * @param name the name
	 *
	 * @return the datacenter
	 */
	private static Datacenter createDatacenter(String name, double simInterval) {

        // Here are the steps needed to create a PowerDatacenter:
        // 1. We need to create a list to store
        // our machine
        List<Host> hostList = new ArrayList<>();


        int numberOfHosts = (VM_NUMBER == 1) ? 1 : (VM_NUMBER / 10);
        for (int i = 1; i <= numberOfHosts; i++) {

            List<Pe> peList = new ArrayList<>();
            int mips = 2500;
            for (int j = 0; j < 12; j++) {
                peList.add(new Pe(i, new PeProvisionerSimple(mips)));
            }

            // 4. Create Host with its id and list of PEs and add them to the list
            // of machines
            int hostId = i;
            int ram = 98304; // host memory (MB)
            long storage = 100_000_000; // host storage
            int bw = 10000;

            hostList.add(
                    new Host(
                            hostId,
                            new RamProvisionerSimple(ram),
                            new BwProvisionerSimple(bw),
                            storage,
                            peList,
                            new VmSchedulerTimeShared(peList)
                    )
            ); // This is our machine
        }


        // 5. Create a DatacenterCharacteristics object that stores the
        // properties of a data center: architecture, OS, list of
        // Machines, allocation policy: time- or space-shared, time zone
        // and its price (G$/Pe time unit).
        String arch = "x86"; // system architecture
        String os = "Linux"; // operating system
        String vmm = "Xen";
        double time_zone = 10.0; // time zone this resource located
        double cost = 3.0; // the cost of using processing in this resource
        double costPerMem = 0.05; // the cost of using memory in this resource
        double costPerStorage = 0.001; // the cost of using storage in this
        // resource
        double costPerBw = 0.0; // the cost of using bw in this resource
        LinkedList<Storage> storageList = new LinkedList<>(); // we are not adding SAN devices by now

        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                arch, os, vmm, hostList, time_zone, cost, costPerMem,
                costPerStorage, costPerBw);

        // 6. Finally, we need to create a PowerDatacenter object.
        Datacenter datacenter = null;
        try {
            datacenter = new CepSimDatacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, simInterval);
        } catch (Exception e) {
            e.printStackTrace();
        }


        return datacenter;
	}

	// We strongly encourage users to develop their own broker policies, to
	// submit vms and cloudlets according
	// to the specific rules of the simulated scenario
	/**
	 * Creates the broker.
	 *
	 * @return the datacenter broker
	 */
	private static DatacenterBroker createBroker(double simInterval) {
		DatacenterBroker broker = null;
		try {
			broker = new CepSimBroker("CepBroker", 100, simInterval);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return broker;
	}

	/**
	 * Prints the Cloudlet objects.
	 *
	 * @param list list of Cloudlets
	 */
	private static void printCloudletList(List<Cloudlet> list) {
		int size = list.size();
		Cloudlet cloudlet;

		String indent = "    ";
		Log.printLine();
		Log.printLine("========== OUTPUT ==========");
		Log.printLine("Cloudlet ID" + indent + "STATUS" + indent
				+ "Data center ID" + indent + "VM ID" + indent + "Time" + indent
				+ "Start Time" + indent + "Finish Time");

		DecimalFormat dft = new DecimalFormat("###.##");
		for (int i = 0; i < size; i++) {
			cloudlet = list.get(i);
			Log.print(indent + cloudlet.getCloudletId() + indent + indent);

			if (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS) {
				Log.print("SUCCESS");

				Log.printLine(indent + indent + cloudlet.getResourceId()
						+ indent + indent + indent + cloudlet.getVmId()
						+ indent + indent
						+ dft.format(cloudlet.getActualCPUTime()) + indent
						+ indent + dft.format(cloudlet.getExecStartTime())
						+ indent + indent
						+ dft.format(cloudlet.getFinishTime()));
			}
		}
	}
}
