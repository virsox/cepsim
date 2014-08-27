package ca.uwo.eng.sel.cepsim.integr;

import java.util.ArrayList;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.ResCloudlet;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CepQueryCloudletSchedulerTest {

	
	@Test
	public void testSubmitCloudlet() throws Exception {
		CepQueryCloudletScheduler scheduler = new CepQueryCloudletScheduler();
		
		CepQueryCloudlet cloudlet = mock(CepQueryCloudlet.class);
		when(cloudlet.getDuration()).thenReturn(100L);
		
		double timeToFinish = scheduler.cloudletSubmit(cloudlet);		
		assertEquals(100.0, timeToFinish, 0.0);
		
		List<? extends ResCloudlet> execList = scheduler.getCloudletExecList();
		assertEquals(1, execList.size());
		
		ResCloudlet resCloudlet = execList.get(0); 
		assertTrue(resCloudlet instanceof CepQueryResCloudlet);
		verify(cloudlet).setCloudletStatus(Cloudlet.INEXEC);		
	}
	
	
	@Test
	public void testUpdateVmProcessing() {
		CepQueryCloudlet cloudlet = mock(CepQueryCloudlet.class);
		when(cloudlet.getDuration()).thenReturn(100L);
		when(cloudlet.getNumberOfPes()).thenReturn(1);

		// submit the cloudlet - needed to setup the scheduler
		CepQueryCloudletScheduler scheduler = new CepQueryCloudletScheduler();
		scheduler.cloudletSubmit(cloudlet);		

		
		List<Double> mipsShare = new ArrayList<>();
		mipsShare.add(500.0);
		scheduler.updateVmProcessing(0.1, mipsShare);
		
		// 500 MI per second / 100 ms it's one tenth of this
		verify(cloudlet).updateQuery(50_000_000, 0.1, 0, 500);
		
		
	}
	
}
