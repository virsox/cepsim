package ca.uwo.eng.sel.cepsim.example;

import ca.uwo.eng.sel.cepsim.QueryCloudlet;
import ca.uwo.eng.sel.cepsim.gen.Generator;
import ca.uwo.eng.sel.cepsim.gen.UniformGenerator;
import ca.uwo.eng.sel.cepsim.integr.CepQueryCloudlet;
import ca.uwo.eng.sel.cepsim.integr.CepQueryCloudletScheduler;
import ca.uwo.eng.sel.cepsim.integr.CepSimBroker;
import ca.uwo.eng.sel.cepsim.integr.CepSimDatacenter;
import ca.uwo.eng.sel.cepsim.metric.LatencyMetric;
import ca.uwo.eng.sel.cepsim.metric.ThroughputMetric;
import ca.uwo.eng.sel.cepsim.placement.Placement;
import ca.uwo.eng.sel.cepsim.query.*;
import ca.uwo.eng.sel.cepsim.sched.DefaultOpScheduleStrategy;
import ca.uwo.eng.sel.cepsim.sched.RRDynOpScheduleStrategy;
import ca.uwo.eng.sel.cepsim.sched.RROpScheduleStrategy;
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


public class CepSimWordCount2 {

    private static final Double SIM_INTERVAL = 0.01;
    private static final Long DURATION = 10L;

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
		Log.printLine("Starting CepSimWordCount2...");

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
			DatacenterBroker broker = createBroker();
			int brokerId = broker.getId();

			// Fourth step: Create one virtual machine
			vmlist = new ArrayList<Vm>();

			// VM description
			int vmid = 1;
			int mips = 2500;
			long size = 10000; // image size (MB)
			int ram = 512; // vm memory (MB)
			long bw = 1000;
			int pesNumber = 1; // number of cpus
			String vmm = "Xen"; // VMM name

			// create VM
			Vm vm = new Vm(vmid, brokerId, mips, pesNumber, ram, bw, size, vmm, new CepQueryCloudletScheduler());

			// add the VM to the vmList
			vmlist.add(vm);

			// submit vm list to the broker
			broker.submitVmList(vmlist);

			// Fifth step: Create one Cloudlet
			cloudletList = new ArrayList<Cloudlet>();

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
                Query q = cepCl.getQueries().iterator().next();

                System.out.println("Throughput: " + ThroughputMetric.calculate(q, q.duration()));

                if (cepCl.getCloudletId() == 1) {
                    System.out.println("Latency   : " + LatencyMetric.calculate(q, cepCl.getExecutionHistory()));
                }
			}

			Log.printLine("CloudSimExample1 finished!");
		} catch (Exception e) {
			e.printStackTrace();
			Log.printLine("Unwanted errors happen");
		}
	}



	private static Set<Cloudlet> createCloudlets(int brokerId) {
		// 100_000_000 I / interval
		// 100 events / interval

        /*
          This is a multiline comment
          ....
         */
        int x = 10;

        final int MAX_QUERIES = 1;
		Set<Cloudlet> cloudlets = new HashSet<>();

        for (int i = 1; i <= MAX_QUERIES; i++) {

            Set<Tuple3<OutputVertex, InputVertex, Object>> edges = new HashSet<>();
            Set<Vertex> vertices = new HashSet<>();
            Map<Vertex, Object> weights = new HashMap<>();

            // producers
            EventProducer[] producers = new EventProducer[3];
            for (int j = 0; j < producers.length; j++) {
                Generator gen = new UniformGenerator(1000, (long) Math.floor(SIM_INTERVAL * 1000));
                EventProducer producer = new EventProducer("spout" + j + "_" + i, 1000, gen, false);
                producers[j] = producer;

                weights.put(producer, 1.0);
                vertices.add(producer);
            }

            // splits
            Operator[] splits = new Operator[3];
            for (int j = 0; j < splits.length; j++) {
                Operator split = new Operator("split" + j + "_" + i, 25000, 1000000);
                splits[j] = split;

                weights.put(split, 1.0);
                vertices.add(split);
            }

            // counts
            Operator[] counts = new Operator[9];
            for (int j = 0; j < counts.length; j++) {
                Operator count = new Operator("count" + j + "_" + i, 12500, 1000000);
                counts[j] = count;

                weights.put(count, 1.667);
                vertices.add(count);
            }

            EventConsumer c = new EventConsumer("end_" + i, 5000, 1000000);
            weights.put(c, 15.0);
            vertices.add(c);

            // connecting producers and splits
            double edgeWeight = 1.0 / splits.length;
            for (int j = 0; j < producers.length; j++) {
                for (int k = 0; k < splits.length; k++) {
                    Tuple3<OutputVertex, InputVertex, Object> edge = new Tuple3<OutputVertex, InputVertex, Object>(
                            producers[j], splits[k], edgeWeight);
                    edges.add(edge);
                }
            }

            // connecting splits and counts
            edgeWeight = 5.0 / counts.length;
            for (int j = 0; j < splits.length; j++) {
                for (int k = 0; k < counts.length; k++) {
                    Tuple3<OutputVertex, InputVertex, Object> edge = new Tuple3<OutputVertex, InputVertex, Object>(
                            splits[j], counts[k], edgeWeight);
                    edges.add(edge);
                }
            }
            
            // connecting counts and consumer
            edgeWeight = 1.0;
            for (int j = 0; j < counts.length; j++) {
                Tuple3<OutputVertex, InputVertex, Object> edge = new Tuple3<OutputVertex, InputVertex, Object>(
                        counts[j], c, edgeWeight);
                edges.add(edge);
            }

            Query q = Query.apply("wordcount" + i, vertices, edges, DURATION);
            Set<Query> queries = new HashSet<Query>();
            queries.add(q);

            Placement placement = Placement.withQueries(queries, 1);
            QueryCloudlet qCloudlet = new QueryCloudlet("cl" + i, placement,
                    RRDynOpScheduleStrategy.apply(WeightedAllocationStrategy.apply(weights), 50));

            CepQueryCloudlet cloudlet = new CepQueryCloudlet(i, qCloudlet, false, null);
            cloudlet.setUserId(brokerId);

            cloudlets.add(cloudlet);

        }
        //
        //long length = 400000;
        //long fileSize = 300;
        //long outputSize = 300;

        // overhead
//        UtilizationModel utilizationModel = new UtilizationModelFull();
//        for (int j = 1; j <= 7; j++) {
//            Cloudlet cloudlet = new Cloudlet(MAX_QUERIES + j,
//                    210 * DURATION, 1, 0, 0, utilizationModel, utilizationModel, utilizationModel);
//
//            cloudlet.setUserId(brokerId);
//            cloudlets.add(cloudlet);
//
//        }





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
        for (int i = 0; i < 8; i++) {
            peList.add(new Pe(i, new PeProvisionerSimple(mips))); // need to store Pe id and MIPS Rating
        }


		// 4. Create Host with its id and list of PEs and add them to the list
		// of machines
		int hostId = 0;
		int ram = 16384; // host memory (MB)
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