package ca.uwo.eng.sel.cepsim.example.cepsim;

import ca.uwo.eng.sel.cepsim.QueryCloudlet;
import ca.uwo.eng.sel.cepsim.gen.Generator;
import ca.uwo.eng.sel.cepsim.gen.UniformGenerator;
import ca.uwo.eng.sel.cepsim.integr.CepQueryCloudlet;
import ca.uwo.eng.sel.cepsim.integr.CepQueryCloudletScheduler;
import ca.uwo.eng.sel.cepsim.integr.CepSimBroker;
import ca.uwo.eng.sel.cepsim.integr.CepSimDatacenter;
import ca.uwo.eng.sel.cepsim.network.FixedDelayNetworkInterface;
import ca.uwo.eng.sel.cepsim.network.NetworkInterface;
import ca.uwo.eng.sel.cepsim.placement.Placement;
import ca.uwo.eng.sel.cepsim.query.*;
import ca.uwo.eng.sel.cepsim.sched.DynOpScheduleStrategy;
import ca.uwo.eng.sel.cepsim.sched.alloc.UniformAllocationStrategy;
import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import scala.Tuple3;
import scala.collection.JavaConversions;

import java.text.DecimalFormat;
import java.util.*;


public class CepSimAvgWindowNetwork {


    private static final Long DURATION = 301L;
    private static final int MAX_QUERIES = 1;
    private static final int NUM_SENSORS = 3250;

	public static void main(String[] args) {
		new CepSimAvgWindowNetwork().simulate(0.01, 1);
	}

	/**
	 * Creates main() to run this example.
	 */
	public void simulate(double simInterval, int iterations) {
		Log.printLine("Starting CepSimAvgWindowNetwork...");


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
            CepSimBroker broker = createBroker(simInterval);
			int brokerId = broker.getId();

			// Fourth step: Create one virtual machine
			List<Vm> vmlist = new ArrayList<>();

			// VM description
			int mips = 2500;
			long size = 10000; // image size (MB)
			int ram = 2048; // vm memory (MB)
			long bw = 1000;
			int pesNumber = 2; // number of cpus
			String vmm = "Xen"; // VMM name

			// create VM
			Vm vm1 = new Vm(1, brokerId, mips, pesNumber, ram, bw, size, vmm, new CepQueryCloudletScheduler());
			Vm vm2 = new Vm(2, brokerId, mips, pesNumber, ram, bw, size, vmm, new CepQueryCloudletScheduler());

			// add the VM to the vmList
			vmlist.add(vm1);
			vmlist.add(vm2);

			// submit vm list to the broker
			broker.submitVmList(vmlist);

			// Fifth step: Create one Cloudlet
			List<Cloudlet> cloudletList = new ArrayList<>();
			cloudletList.addAll(createCloudlets(broker, iterations));

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

                for (Query q : cepCl.getQueries()) {
                    System.out.println("Query [" + q.id() + "]");
                    for (Vertex consumer: JavaConversions.asJavaIterable(q.consumers())) {
                        System.out.println("Latencies: " + cepCl.getLatencyByMinute(consumer));
                        System.out.println("Throughputs: " + cepCl.getThroughputByMinute(consumer));
                    }
                    System.out.println("------");
            	}
			}

			Log.printLine("CloudSimExample1 finished!");
		} catch (Exception e) {
			e.printStackTrace();
			Log.printLine("Unwanted errors happen");
		}
	}



	private static Set<Cloudlet> createCloudlets(CepSimBroker broker, int iterations) {
		// 100_000_000 I / interval
		// 100 events / interval

		Set<Cloudlet> cloudlets = new HashSet<>();
        Map<Vertex, Object> weights = new HashMap<>();

        Set<Vertex> p1Vertices = new HashSet<>();
        Set<Vertex> p2Vertices = new HashSet<>();

        for (int i = 1; i <= MAX_QUERIES; i++) {
            Generator gen = new UniformGenerator(NUM_SENSORS * 10); //, (long) Math.floor(SIM_INTERVAL * 1000));

            EventProducer p = new EventProducer("spout" + i, 1_000, gen, true);

            Operator outlierDetector = new Operator("outlierDetector" + i, 18_000, 2048);
            Operator average = WindowedOperator.apply("average" + i, 18_000, 15000, 15000,
					WindowedOperator.constant(NUM_SENSORS), 2048);
            Operator db = new Operator("db" + i, 11_000_000, 2048);

            EventConsumer c = new EventConsumer("end" + i, 1_000, 2048);


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

            weights.put(p, 100.0);
            weights.put(outlierDetector, 50.0);
            weights.put(average, 50.0);
            weights.put(db, 1.0);
            weights.put(c, 1.0);

            Query q = Query.apply("testavg" + i, vertices, edges, DURATION);

            // ------------- VM 1
            p1Vertices.add(p);
            p1Vertices.add(outlierDetector);
            p1Vertices.add(average);
            p1Vertices.add(c);

            // ------------- VM2 2
            p2Vertices.add(db);

        }
        NetworkInterface network = new FixedDelayNetworkInterface(broker, 0.001);

        Placement placement1 = Placement.apply(p1Vertices, 1);
        QueryCloudlet qCloudlet1 = QueryCloudlet.apply("cl1", placement1,
                DynOpScheduleStrategy.apply(UniformAllocationStrategy.apply()), iterations, network);

        CepQueryCloudlet cloudlet1 = new CepQueryCloudlet(1, qCloudlet1, false);
        cloudlet1.setUserId(broker.getId());
        cloudlets.add(cloudlet1);

        Placement placement2 = Placement.apply(p2Vertices, 2);
        QueryCloudlet qCloudlet2 = QueryCloudlet.apply("cl2", placement2,
                DynOpScheduleStrategy.apply(UniformAllocationStrategy.apply()), iterations, network);

        CepQueryCloudlet cloudlet2 = new CepQueryCloudlet(2, qCloudlet2, false);
        cloudlet2.setUserId(broker.getId());
        cloudlets.add(cloudlet2);


        cloudlets.add(cloudlet1);
        cloudlets.add(cloudlet2);
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

		// 2. A Machine contains one or more PEs or CPUs/Cores.
		List<Pe> peList = new ArrayList<>();

		// 3. Create PEs and add these into a list.
        int mips = 2500;
        for (int i = 0; i < 12; i++) {
            peList.add(new Pe(i, new PeProvisionerSimple(mips))); // need to store Pe id and MIPS Rating
        }


		// 4. Create Host with its id and list of PEs and add them to the list
		// of machines
		int hostId = 0;
		int ram = 98304; // host memory (MB)
		long storage = 1000000; // host storage
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
		LinkedList<Storage> storageList = new LinkedList<>(); // we are not adding SAN
													// devices by now

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
	private static CepSimBroker createBroker(double simInterval) {
        CepSimBroker broker = null;
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
