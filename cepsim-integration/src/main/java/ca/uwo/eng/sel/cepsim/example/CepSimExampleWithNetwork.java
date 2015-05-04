package ca.uwo.eng.sel.cepsim.example;

import ca.uwo.eng.sel.cepsim.QueryCloudlet;
import ca.uwo.eng.sel.cepsim.gen.Generator;
import ca.uwo.eng.sel.cepsim.gen.UniformGenerator;
import ca.uwo.eng.sel.cepsim.history.SimEvent;
import ca.uwo.eng.sel.cepsim.integr.CepQueryCloudlet;
import ca.uwo.eng.sel.cepsim.integr.CepQueryCloudletScheduler;
import ca.uwo.eng.sel.cepsim.integr.CepSimBroker;
import ca.uwo.eng.sel.cepsim.integr.CepSimDatacenter;
import ca.uwo.eng.sel.cepsim.history.History;
import ca.uwo.eng.sel.cepsim.network.FixedDelayNetworkInterface;
import ca.uwo.eng.sel.cepsim.network.NetworkInterface;
import ca.uwo.eng.sel.cepsim.placement.Placement;
import ca.uwo.eng.sel.cepsim.query.*;
import ca.uwo.eng.sel.cepsim.sched.DefaultOpScheduleStrategy;
import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import scala.Tuple3;
import scala.collection.JavaConversions;

import java.text.DecimalFormat;
import java.util.*;


public class CepSimExampleWithNetwork {

    private static final Double SIM_INTERVAL = 0.01;

	/** The cloudlet list. */
	private static List<CepQueryCloudlet> cloudletList;
	/** The vmList. */
	private static List<Vm> vmList;

	/**
	 * Creates main() to run this example.
	 *
	 * @param args the args
	 */
	public static void main(String[] args) {
		Log.printLine("Starting CepSimExample...");

		try {
			// First step: Initialize the CloudSim package. It should be called before creating any entities.
			int num_user = 1; // number of cloud users
			Calendar calendar = Calendar.getInstance(); // Calendar whose fields have been initialized with the current date and time.
 			boolean trace_flag = false; // trace events


			CloudSim.init(num_user, calendar, trace_flag, SIM_INTERVAL);

			// Second step: Create Datacenters
			// Datacenters are the resource providers in CloudSim. We need at
			// list one of them to run a CloudSim simulation
			Datacenter datacenter0 = createDatacenter("Datacenter_0");

			// Third step: Create Broker
            CepSimBroker broker = createBroker();
			int brokerId = broker.getId();

			// Fourth step: Create two virtual machines
			vmList = new ArrayList<Vm>();

			// VM description
			int vmid = 1;
			int mips = 1000;
			long size = 10000; // image size (MB)
			int ram = 512; // vm memory (MB)
			long bw = 1000;
			int pesNumber = 1; // number of cpus
			String vmm = "Xen"; // VMM name

			// create VMs
			Vm vm1 = new Vm(vmid,     brokerId, mips, pesNumber, ram, bw, size, vmm, new CepQueryCloudletScheduler());
			Vm vm2 = new Vm(vmid + 1, brokerId, mips, pesNumber, ram, bw, size, vmm, new CepQueryCloudletScheduler());

			// add the VMs to the vmList
			vmList.add(vm1);
            vmList.add(vm2);

			// submit vm list to the broker
			broker.submitVmList(vmList);

			// Fifth step: Create one Cloudlet
			cloudletList = new ArrayList<CepQueryCloudlet>();

			// Cloudlet properties
//			int id = 0;
//			long length = 400000;
//			long fileSize = 300;
//			long outputSize = 300;
//			UtilizationModel utilizationModel = new UtilizationModelFull();

//			Cloudlet cloudlet = new Cloudlet(id, length, pesNumber, fileSize, outputSize, utilizationModel, utilizationModel, utilizationModel);
//			cloudlet.setUserId(brokerId);
//			cloudlet.setVmId(vmid);

			// add the cloudlet to the list
			cloudletList.addAll(createCloudlets(broker));

			// submit cloudlet list to the broker
			broker.submitCloudletList(cloudletList);

			// Sixth step: Starts the simulation
			CloudSim.startSimulation();

			CloudSim.stopSimulation();

			//Final step: Print results when simulation is over
			List<Cloudlet> newList = broker.getCloudletReceivedList();
			printCloudletList(newList);


            // get all queries and history from all cloudlets
            History<SimEvent> fullHistory = new History<>();
            Set<Query> queries = new HashSet<>();
			for (Cloudlet cl : newList) {
				CepQueryCloudlet cepCl = (CepQueryCloudlet) cl;
                fullHistory.merge(cepCl.getExecutionHistory());
                queries.addAll(cepCl.getQueries());
			}

//            for (Query q : queries) {
//				Vertex consumer = q.consumers().head();
//
//				System.out.println("Latency: " + cepCl.getLatency(consumer));
//				System.out.println("Throughput: " + cepCl.getThroughput(consumer));
//
//
//				System.out.println("Throughput: " + ThroughputMetric.calculate(q, q.duration()));
//                System.out.println("Latency   : " + LatencyMetric.calculate(q, fullHistory));
//            }


            Log.printLine("CloudSimExample1 finished!");
		} catch (Exception e) {
			e.printStackTrace();
			Log.printLine("Unwanted errors happen");
		}
	}



	private static Set<CepQueryCloudlet> createCloudlets(CepSimBroker broker) {
		// 100_000_000 I / interval
		// 100 events / interval
		
		Set<CepQueryCloudlet> cloudlets = new HashSet<>();

		//for (int i = 1; i <= 5; i++) {
		Generator gen = new UniformGenerator(1000);//, (long) Math.floor(SIM_INTERVAL * 1000));
		EventProducer p = new EventProducer("p1", 10000, gen, true);
		Operator f1 = new Operator("f1", 100000, 1000);
		Operator f2 = new Operator("f2", 100000, 1000);
		EventConsumer c = new EventConsumer("c1", 10000, 1000);
			
		Set<Vertex> vertices = new HashSet<>();
		vertices.add(p);
		vertices.add(f1);
		vertices.add(f2);
		vertices.add(c);

		Tuple3<OutputVertex, InputVertex, Object> e1 = new Tuple3<OutputVertex, InputVertex, Object>(p, f1, 1.0);
		Tuple3<OutputVertex, InputVertex, Object> e2 = new Tuple3<OutputVertex, InputVertex, Object>(f1, f2, 0.5);
		Tuple3<OutputVertex, InputVertex, Object> e3 = new Tuple3<OutputVertex, InputVertex, Object>(f2, c, 0.1);
		Set<Tuple3<OutputVertex, InputVertex, Object>> edges = new HashSet<>();
		edges.add(e1);
		edges.add(e2);
		edges.add(e3);

		Query q = Query.apply("q1", vertices, edges, 10L);
		Set<Query> queries = new HashSet<Query>();
		queries.add(q);

        // ------------------ create placements
        Set<Vertex> p1Vertices = new HashSet<>();
        p1Vertices.add(p);
        p1Vertices.add(f1);

        Set<Vertex> p2Vertices = new HashSet<>();
        p2Vertices.add(f2);
        p2Vertices.add(c);

        Placement placement1 = Placement.apply(JavaConversions.asScalaSet(p1Vertices).<Vertex>toSet(), 1,
				scala.collection.immutable.List.<Vertex>empty());
        Placement placement2 = Placement.apply(JavaConversions.asScalaSet(p2Vertices).<Vertex>toSet(), 2,
				scala.collection.immutable.List.<Vertex>empty());

        // ------------------  create cloudlets
		QueryCloudlet p1Cloudlet = QueryCloudlet.apply("cl1", placement1, DefaultOpScheduleStrategy.weighted(), 1);
		QueryCloudlet p2Cloudlet = QueryCloudlet.apply("cl2", placement2, DefaultOpScheduleStrategy.weighted(), 1);

        NetworkInterface network = new FixedDelayNetworkInterface(broker, 0.05);
		CepQueryCloudlet cloudlet1 = new CepQueryCloudlet(1, p1Cloudlet, false);
        CepQueryCloudlet cloudlet2 = new CepQueryCloudlet(2, p2Cloudlet, false);

        cloudlet1.setUserId(broker.getId());
        cloudlet2.setUserId(broker.getId());
			
		cloudlets.add(cloudlet1);
		cloudlets.add(cloudlet2);
		//}
		
		return cloudlets;
	}
	
	/**
	 * Creates the datacenter.
	 *
	 * @param name the name
	 *
	 * @return the datacenter
	 */
	private static Datacenter createDatacenter(String name) {

		// Here are the steps needed to create a PowerDatacenter:
		// 1. We need to create a list to store
		// our machine
		List<Host> hostList = new ArrayList<>();

		// 2. A Machine contains one or more PEs or CPUs/Cores.
		// In this example, it will have only one core.
		List<Pe> peList = new ArrayList<>();

		int mips = 1000;

		// 3. Create PEs and add these into a list.
		peList.add(new Pe(0, new PeProvisionerSimple(mips))); // need to store Pe id and MIPS Rating
		peList.add(new Pe(1, new PeProvisionerSimple(mips))); // need to store Pe id and MIPS Rating

		// 4. Create Host with its id and list of PEs and add them to the list
		// of machines
		int hostId = 0;
		int ram = 2048; // host memory (MB)
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
			datacenter = new CepSimDatacenter(name, characteristics, new VmAllocationPolicySimple(hostList),
                    storageList, SIM_INTERVAL);
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
	private static CepSimBroker createBroker() {
        CepSimBroker broker = null;
		try {
			broker = new CepSimBroker("CepBroker", 100, SIM_INTERVAL);
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
