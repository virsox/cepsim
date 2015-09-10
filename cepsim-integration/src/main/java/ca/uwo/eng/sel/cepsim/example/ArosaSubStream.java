package ca.uwo.eng.sel.cepsim.example;

import ca.uwo.eng.sel.cepsim.QueryCloudlet;
import ca.uwo.eng.sel.cepsim.gen.Generator;
import ca.uwo.eng.sel.cepsim.gen.UniformGenerator;
import ca.uwo.eng.sel.cepsim.integr.CepQueryCloudlet;
import ca.uwo.eng.sel.cepsim.integr.CepQueryCloudletScheduler;
import ca.uwo.eng.sel.cepsim.integr.CepSimBroker;
import ca.uwo.eng.sel.cepsim.integr.CepSimDatacenter;
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


public class ArosaSubStream {

    private static final Double SIM_INTERVAL = 0.1;
    private static final Long DURATION = 301L;
	private static final int MAX_QUERIES = 4;
	private static final int NUM_SENSORS = 1000;

	/** The cloudlet list. */
	private static List<Cloudlet> cloudletList;
	/** The vmlist. */
	private static List<Vm> vmlist;

	/**
	 * Creates main() to run this example.
	 *
	 * @param args the args
	 */
	public static void main(String[] args) {
		Log.printLine("Starting ArosaSubStream...");

		try {
			//System.in.read();

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
			DatacenterBroker broker = createBroker();
			int brokerId = broker.getId();

			// Fourth step: Create one virtual machine
			vmlist = new ArrayList<Vm>();

			// VM description
			int vmid = 1;
			int mips = 2500;
			long size = 10000; // image size (MB)
			int ram = 1024; // vm memory (MB)
			long bw = 1000;
			int pesNumber = 2; // number of cpus
			String vmm = "Xen"; // VMM name

			// create VM
			Vm vm = new Vm(vmid, brokerId, mips, pesNumber, ram, bw, size, vmm, new CepQueryCloudletScheduler());

			// add the VM to the vmList
			vmlist.add(vm);

			// submit vm list to the broker
			broker.submitVmList(vmlist);

			// Fifth step: Create one Cloudlet
			cloudletList = new ArrayList<Cloudlet>();
			cloudletList.addAll(createCloudlets(brokerId));

			// submit cloudlet list to the broker
			broker.submitCloudletList(cloudletList);

			// Sixth step: Starts the simulation
			CloudSim.startSimulation();

			CloudSim.stopSimulation();

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

			Log.printLine("ArosaSubStream finished!");
		} catch (Exception e) {
			e.printStackTrace();
			Log.printLine("Unwanted errors happen");
		}
	}



	private static Set<Cloudlet> createCloudlets(int brokerId) {
		// 100_000_000 I / interval
		// 100 events / interval



		Set<Cloudlet> cloudlets = new HashSet<>();
        Set<Query> queries = new HashSet<Query>();
        Map<Vertex, Object> weights = new HashMap<>();

        for (int i = 1; i <= MAX_QUERIES; i++) {
            Generator gen1 = new UniformGenerator(NUM_SENSORS * 10);
            Generator gen2 = new UniformGenerator(NUM_SENSORS * 10);

            EventProducer p1 = new EventProducer("spout1_" + i, 1_000, gen1, true);
            EventProducer p2 = new EventProducer("spout2_" + i, 1_000, gen2, true);

            Operator outlier1 = new Operator("outlier1_" + i, 20_000, 2048);
            Operator outlier2 = new Operator("outlier2_" + i, 20_000, 2048);
            Operator merge = new Operator("merge_" + i, 15_000, 2048);
            Operator average = WindowedOperator.apply("average_" + i, 18_000, 10000, 10000, WindowedOperator.constant(NUM_SENSORS), 2048);

            EventConsumer c = new EventConsumer("end_" + i, 1_000, 2048);


            Set<Vertex> vertices = new HashSet<>();
            vertices.add(p1);
            vertices.add(p2);
            vertices.add(outlier1);
            vertices.add(outlier2);
            vertices.add(merge);
            vertices.add(average);
            vertices.add(c);

            Tuple3<OutputVertex, InputVertex, Object> e1 = new Tuple3<OutputVertex, InputVertex, Object>(p1, outlier1, 1.0);
            Tuple3<OutputVertex, InputVertex, Object> e2 = new Tuple3<OutputVertex, InputVertex, Object>(p2, outlier2, 1.0);
            Tuple3<OutputVertex, InputVertex, Object> e3 = new Tuple3<OutputVertex, InputVertex, Object>(outlier1, merge, 0.5);
            Tuple3<OutputVertex, InputVertex, Object> e4 = new Tuple3<OutputVertex, InputVertex, Object>(outlier2, merge, 0.5);
            Tuple3<OutputVertex, InputVertex, Object> e5 = new Tuple3<OutputVertex, InputVertex, Object>(merge,  average, 1.0);
            Tuple3<OutputVertex, InputVertex, Object> e6 = new Tuple3<OutputVertex, InputVertex, Object>(average, c, 1.0);

            Set<Tuple3<OutputVertex, InputVertex, Object>> edges = new HashSet<>();
            edges.add(e1);
            edges.add(e2);
            edges.add(e3);
            edges.add(e4);
            edges.add(e5);
            edges.add(e6);



//            Operator merge = new Operator("merge_" + i, 15_000, 2048);
//            Operator outlier = new Operator("outlier_" + i, 20_000, 2048);
//            Operator average = WindowedOperator.apply("average_" + i, 18_000, 10000, 10000, WindowedOperator.constant(NUM_SENSORS), 2048);
//            EventConsumer c = new EventConsumer("end_" + i, 1_000, 2048);
//
//
//            Set<Vertex> vertices = new HashSet<>();
//            vertices.add(p1);
//            vertices.add(p2);
//            vertices.add(merge);
//            vertices.add(outlier);
//            vertices.add(average);
//            vertices.add(c);
//
//            Tuple3<OutputVertex, InputVertex, Object> e1 = new Tuple3<OutputVertex, InputVertex, Object>(p1, merge, 1.0);
//            Tuple3<OutputVertex, InputVertex, Object> e2 = new Tuple3<OutputVertex, InputVertex, Object>(p2, merge, 1.0);
//            Tuple3<OutputVertex, InputVertex, Object> e3 = new Tuple3<OutputVertex, InputVertex, Object>(merge, outlier, 1.0);
//            Tuple3<OutputVertex, InputVertex, Object> e4 = new Tuple3<OutputVertex, InputVertex, Object>(outlier, average, 0.5);
//            Tuple3<OutputVertex, InputVertex, Object> e5 = new Tuple3<OutputVertex, InputVertex, Object>(average, c, 1.0);
//
//            Set<Tuple3<OutputVertex, InputVertex, Object>> edges = new HashSet<>();
//            edges.add(e1);
//            edges.add(e2);
//            edges.add(e3);
//            edges.add(e4);
//            edges.add(e5);

            Query q = Query.apply("testsubstream" + i, vertices, edges, DURATION);
            queries.add(q);
        }
        Placement placement = Placement.withQueries(queries, 1);



        QueryCloudlet qCloudlet = QueryCloudlet.apply("cl", placement,
				//AltDynOpScheduleStrategy.apply(UniformAllocationStrategy.apply()), 10);
                //DefaultOpScheduleStrategy.weighted(weights), 10);
                DynOpScheduleStrategy.apply(UniformAllocationStrategy.apply()), 1);
                //DefaultOpScheduleStrategy.weighted(weights));
               // RRDynOpScheduleStrategy.apply(WeightedAllocationStrategy.apply(weights), 1));


        CepQueryCloudlet cloudlet = new CepQueryCloudlet(1, qCloudlet, 2, false);
        cloudlet.setUserId(brokerId);

        cloudlets.add(cloudlet);

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
			datacenter = new CepSimDatacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, SIM_INTERVAL);
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
	private static DatacenterBroker createBroker() {
		DatacenterBroker broker = null;
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
