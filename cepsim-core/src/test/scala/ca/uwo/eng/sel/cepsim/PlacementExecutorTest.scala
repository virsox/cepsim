package ca.uwo.eng.sel.cepsim

import ca.uwo.eng.sel.cepsim.event.EventSet
import ca.uwo.eng.sel.cepsim.history.{Consumed, Generated, Produced}
import ca.uwo.eng.sel.cepsim.network.NetworkInterface
import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query._
import ca.uwo.eng.sel.cepsim.sched.{EnqueueAction, ExecuteAction, OpScheduleStrategy}
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable.TreeSet


@RunWith(classOf[JUnitRunner])
class PlacementExecutorTest extends FlatSpec
	with Matchers
	with MockitoSugar {

  trait Fixture {
    val prod = mock[EventProducer]("prod")
    val f1 = mock[Operator]("f1")
    val f2 = mock[Operator]("f2")
    val cons = mock[EventConsumer]("cons")
    val q = mock[Query]("q")

    doReturn(Set(prod)).when(f1).predecessors
    doReturn(Set(f1)).when(f2).predecessors
    doReturn(Set(f2)).when(cons).predecessors

    doReturn(Set(f1)).when(prod).successors
    doReturn(Set(f2)).when(f1).successors
    doReturn(Set(cons)).when(f2).successors

    val placement = mock[Placement]
    doReturn(1).when(placement).vmId
    doReturn(Set(prod)).when(placement).producers
    doReturn(Set(prod, f1, f2, cons)).when(placement).vertices
    doReturn(Set.empty).when(placement).successorsNotInPlacement(anyObject[Vertex]())
    doReturn(Set(f1  )).when(placement).successorsInPlacement(prod)
    doReturn(Set(f2  )).when(placement).successorsInPlacement(f1  )
    doReturn(Set(cons)).when(placement).successorsInPlacement(f2  )
    doReturn(Set.empty).when(placement).successorsInPlacement(cons)

    var opSchedule = mock[OpScheduleStrategy]
    doReturn(Iterator(
              ExecuteAction(prod, 1000.0, 1100.0, 100000.0),
              ExecuteAction(f1, 1100.0, 1500.0, 400000.0),
              ExecuteAction(f2, 1500.0, 1900.0, 400000.0),
              ExecuteAction(cons, 1900.0, 2000.0, 100000.0))).
      when(opSchedule).
      allocate(1000000, 1000.0, 1, placement, TreeSet.empty)
  }

  trait Fixture1 extends Fixture {
    doReturn(Some(Generated(prod, 0.0, 1000, EventSet(100.0, 1000, 0, Map(prod -> 100.0))))).when(prod).generate(0.0, 1000)

    doReturn(List(Produced(prod, 1000.0, 1100.0, EventSet(100.0, 1100.0,  100.0, prod -> 100.0)))).when(prod).run(100000, 1000.0, 1100.0)
    doReturn(List(Produced(f1,   1100.0, 1500.0, EventSet(100.0, 1500.0,  500.0, prod -> 100.0)))).when(f1  ).run(400000, 1100.0, 1500.0)
    doReturn(List(Produced(f2,   1500.0, 1900.0, EventSet(100.0, 1900.0,  900.0, prod -> 100.0)))).when(f2  ).run(400000, 1500.0, 1900.0)
    doReturn(List(Consumed(cons, 1900.0, 2000.0, EventSet(100.0, 2000.0, 1000.0, prod -> 100.0)))).when(cons).run(100000, 1900.0, 2000.0)
  }

  "A PlacementExecutor" should "correctly initialize all operators" in new Fixture {
    val executor = PlacementExecutor("c1", placement, opSchedule)
    executor.init(0.0)

    verify(prod).init(0.0)
    verify(f1).init(0.0)
    verify(f2).init(0.0)
    verify(cons).init(0.0)
  }

  it should "correctly enqueue events received from the network" in new Fixture {

    val executor = PlacementExecutor("c1", placement, opSchedule)
    val enqueuedEs = EventSet(1000, 50.0, 10.0, prod -> 100.0)

    executor.enqueue(100.0, prod, f1, enqueuedEs)

    executor.pendingActions should have size (1)
    executor.pendingActions should contain (EnqueueAction(f1, prod, 100.0, enqueuedEs))
  }

  // --------------------------------------------------

  it should "correctly run all operators" in new Fixture1 {
    val executor = PlacementExecutor("c1", placement, opSchedule) //, 0.0)
    executor.init(0.0)

    doReturn(100.0).when(prod).outputQueues(f1)
    doReturn(100.0).when(f1).outputQueues(f2)
    doReturn(100.0).when(f2).outputQueues(cons)

    // the cloudlet should run all operators
    val history = executor run(1000000, 1000.0, 1)

    verify(prod).generate(0.0, 1000.0)
    verify(prod).run(100000, 1000.0, 1100.0)
    verify(f1  ).run(400000, 1100.0, 1500.0)
    verify(f2  ).run(400000, 1500.0, 1900.0)
    verify(cons).run(100000, 1900.0, 2000.0)

    history should have size (5)
    history.toList should contain theSameElementsInOrderAs (List(
      Generated(prod,    0.0, 1000.0, EventSet(100.0, 1000.0,    0.0, prod -> 100.0)),
      Produced (prod, 1000.0, 1100.0, EventSet(100.0, 1100.0,  100.0, prod -> 100.0)),
      Produced (f1,   1100.0, 1500.0, EventSet(100.0, 1500.0,  500.0, prod -> 100.0)),
      Produced (f2,   1500.0, 1900.0, EventSet(100.0, 1900.0,  900.0, prod -> 100.0)),
      Consumed (cons, 1900.0, 2000.0, EventSet(100.0, 2000.0, 1000.0, prod -> 100.0))
    ))
  }


  it should "send events to operators that are in a different Placement" in new Fixture1 {
    val network = mock[NetworkInterface]
    val executor = PlacementExecutor("c1", placement, opSchedule, 1, network)
    executor.init(0.0)

    // create new operators
    val f3 = mock[Operator]
    val cons2 = mock[EventConsumer]

    doReturn(Set(f2)).when(f3).predecessors
    doReturn(Set(f2)).when(cons).predecessors
    doReturn(Set(f3)).when(cons2).predecessors

    doReturn(Set(f3, cons)).when(f2).successors
    doReturn(Set(cons2)).when(f3).successors

    doReturn(Set(f3)  ).when(placement).successorsNotInPlacement(f2)
    doReturn(Set.empty).when(placement).successorsNotInPlacement(prod)
    doReturn(Set.empty).when(placement).successorsNotInPlacement(f1)
    doReturn(Set.empty).when(placement).successorsNotInPlacement(cons)


    when(prod.outputQueues(anyObject[Vertex]())).thenReturn(0.0)
    when(f1.outputQueues(anyObject[Vertex]())).thenReturn(0.0)
    when(f2.outputQueues(cons)).thenReturn(100.0)
    when(f2.outputQueues(  f3)).thenReturn(100.0)

    val es = EventSet(100.0, 1900.0, 900.0, prod -> 100.0)
    when(f2.dequeueFromOutput(f3, 100.0)).thenReturn(es)

    // the cloudlet should run all operators
    val history = executor run(1000000, 1000.0, 1)

    verify(prod).generate(0.0, 1000.0)
    verify(prod).run(100000, 1000.0, 1100.0)
    verify(f1  ).run(400000, 1100.0, 1500.0)
    verify(f2  ).run(400000, 1500.0, 1900.0)
    verify(cons).run(100000, 1900.0, 2000.0)

    // these operators shouldn't run
    verify(f3, never()).run(anyDouble(), anyDouble(), anyDouble())
    verify(cons2, never()).run(anyDouble(), anyDouble(), anyDouble())

    // network interface object should be invoked
    verify(network).sendMessage(1900.0, f2, f3, es)

    val entries = history.from(f2)
    entries should have size (1)
    entries should be (List(Produced(f2, 1500.0, 1900.0, EventSet(100.0, 1900.0, 900.0, prod -> 100.0))))
  }

  // -------------------------------------------------

  it should "correctly split available instructions into iterations" in new Fixture {

    // redefine schedule strategy
    opSchedule = mock[OpScheduleStrategy]

    // first iteration
    doReturn(Iterator(
        ExecuteAction(prod, 500.0,  550.0,  50000.0), ExecuteAction(f1,   550.0,  750.0, 200000.0),
        ExecuteAction(f2,   750.0,  950.0, 200000.0), ExecuteAction(cons, 950.0, 1000.0,  50000.0))).
      when(opSchedule).
      allocate(500000, 500.0, 1, placement, TreeSet.empty)

    // second iteration
    doReturn(Iterator(
        ExecuteAction(prod, 1000.0, 1050.0,  50000.0), ExecuteAction(f1,   1050.0, 1250.0, 200000.0),
        ExecuteAction(f2,   1250.0, 1450.0, 200000.0), ExecuteAction(cons, 1450.0, 1500.0,  50000.0))).
      when(opSchedule).
      allocate(500000, 1000.0, 1, placement, TreeSet.empty)


    // 1st iteration
    doReturn(Some(Generated(prod,    0.0, 500.0, EventSet(50.0,  500.0,   0.0, prod -> 50.0)))).when(prod).generate(0.0, 500.0)
    doReturn(List(Produced (prod, 500.0,  550.0, EventSet(50.0,  550.0,  50.0, prod -> 50.0)))).when(prod).run( 50000, 500.0,  550.0)
    doReturn(List(Produced (f1,   550.0,  750.0, EventSet(50.0,  750.0, 250.0, prod -> 50.0)))).when(f1  ).run(200000, 550.0,  750.0)
    doReturn(List(Produced (f2,   750.0,  950.0, EventSet(50.0,  950.0, 450.0, prod -> 50.0)))).when(f2  ).run(200000, 750.0,  950.0)
    doReturn(List(Consumed (cons, 950.0, 1000.0, EventSet(50.0, 1000.0, 500.0, prod -> 50.0)))).when(cons).run( 50000, 950.0, 1000.0)

    // 2nd iteration
    doReturn(Some(Generated(prod,  500.0, 1000.0, EventSet(50.0, 1000.0,  50.0, prod -> 50.0)))).when(prod).generate(500.0, 1000.0)
    doReturn(List(Produced (prod, 1000.0, 1050.0, EventSet(50.0, 1050.0,  50.0, prod -> 50.0)))).when(prod).run( 50000, 1000.0, 1050.0)
    doReturn(List(Produced (f1,   1050.0, 1250.0, EventSet(50.0, 1250.0, 250.0, prod -> 50.0)))).when(f1  ).run(200000, 1050.0, 1250.0)
    doReturn(List(Produced (f2,   1250.0, 1450.0, EventSet(50.0, 1450.0, 450.0, prod -> 50.0)))).when(f2  ).run(200000, 1250.0, 1450.0)
    doReturn(List(Consumed (cons, 1450.0, 1500.0, EventSet(50.0, 1500.0, 500.0, prod -> 50.0)))).when(cons).run(50000,  1450.0, 1500.0)

    // 2 iterations
    val executor = PlacementExecutor("c1", placement, opSchedule, 2)
    executor.init(0.0)

    when(prod.outputQueues(anyObject[Vertex]())).thenReturn(50.0)
    when(  f1.outputQueues(anyObject[Vertex]())).thenReturn(50.0)
    when(  f2.outputQueues(anyObject[Vertex]())).thenReturn(50.0)

    // the cloudlet should run all operators
    executor run(1000000, 500.0, 1)  // 1 million instructions @ 1 MIPS = 1 second

    // two iterations of 500,000 instructions each
    verify(prod).generate(  0.0,  500.0)
    verify(prod).generate(500.0, 1000.0)

    verify(prod).run( 50000,  500.0,  550.0)
    verify(prod).run( 50000, 1000.0, 1050.0)
    verify(f1  ).run(200000,  550.0,  750.0)
    verify(f1  ).run(200000, 1050.0, 1250.0)
    verify(f2  ).run(200000,  750.0,  950.0)
    verify(f2  ).run(200000, 1250.0, 1450.0)
    verify(cons).run( 50000,  950.0, 1000.0)
    verify(cons).run( 50000, 1450.0, 1500.0)
  }


  it should "consider all pending actions" in new Fixture {

    val f3 = mock[Operator]("f3")
    doReturn(Set(f1, f3)).when(f2).predecessors

    val enqueueEs1 = EventSet(100.0, 1000.0, 20.0, prod -> 100.0)
    val enqueueAction1 = EnqueueAction(f1, f3, 1200.0, enqueueEs1)

    // this action should not be scheduled because it is out of the iteration time bounds
    val enqueueEs2 = EventSet(100.0, 2100.0, 20.0, prod -> 100.0)
    val enqueueAction2 = EnqueueAction(f1, f3, 2200.0, enqueueEs2)

    doReturn(Iterator(
        ExecuteAction(prod, 1000.0, 1100.0, 100000.0),
        ExecuteAction(f1,   1100.0, 1200.0, 100000.0),
        enqueueAction1,
        ExecuteAction(f1,   1200.0, 1500.0, 300000.0),
        ExecuteAction(f2,   1500.0, 1900.0, 400000.0),
        ExecuteAction(cons, 1900.0, 2000.0, 100000.0))).
      when(opSchedule).
      allocate(1000000, 1000.0, 1, placement, TreeSet(enqueueAction1))

    doReturn(Some(Generated(prod, 0.0, 1000, EventSet(100.0, 1000, 0, Map(prod -> 100.0))))).when(prod).generate(0.0, 1000)
    doReturn(List(Produced(prod, 1000.0, 1100.0, EventSet(100.0, 1100.0,  100.0, prod -> 100.0)))).when(prod).run(100000, 1000.0, 1100.0)
    doReturn(List(Produced(f1,   1100.0, 1200.0, EventSet( 25.0, 1200.0,  200.0, prod ->  25.0)))).when(f1  ).run(100000, 1100.0, 1200.0)
    doReturn(List(Produced(f1,   1200.0, 1500.0, EventSet( 75.0, 1500.0,  500.0, prod ->  75.0)))).when(f1  ).run(300000, 1200.0, 1500.0)
    doReturn(List(Produced(f2,   1500.0, 1900.0, EventSet(100.0, 1900.0,  900.0, prod -> 100.0)))).when(f2  ).run(400000, 1500.0, 1900.0)
    doReturn(List(Consumed(cons, 1900.0, 2000.0, EventSet(100.0, 2000.0, 1000.0, prod -> 100.0)))).when(cons).run(100000, 1900.0, 2000.0)

    val executor = PlacementExecutor("c1", placement, opSchedule)
    executor.init(0.0)
    executor.enqueue(1200.0, f3, f1, enqueueEs1)
    executor.enqueue(2200.0, f3, f1, enqueueEs2)

    doReturn(100.0).when(prod).outputQueues(f1)
    doReturn(100.0).when(f1).outputQueues(f2)
    doReturn(100.0).when(f2).outputQueues(cons)

    // the cloudlet should run all operators
    val history = executor run(1000000, 1000.0, 1)

    val inOrder = Mockito.inOrder(prod, f1, f2, cons)
    inOrder.verify(prod).generate(0.0, 1000.0)
    inOrder.verify(prod).run(100000, 1000.0, 1100.0)
    inOrder.verify(f1  ).run(100000, 1100.0, 1200.0)
    inOrder.verify(f1  ).enqueueIntoInput(f3, enqueueEs1)
    inOrder.verify(f1  ).run(300000, 1200.0, 1500.0)
    inOrder.verify(f2  ).run(400000, 1500.0, 1900.0)
    inOrder.verify(cons).run(100000, 1900.0, 2000.0)

    history should have size (6)
    history.toList should contain theSameElementsInOrderAs (List(
      Generated(prod,    0.0, 1000.0, EventSet(100.0, 1000.0,    0.0, prod -> 100.0)),
      Produced (prod, 1000.0, 1100.0, EventSet(100.0, 1100.0,  100.0, prod -> 100.0)),
      Produced (f1,   1100.0, 1200.0, EventSet( 25.0, 1200.0,  200.0, prod ->  25.0)),
      Produced (f1,   1200.0, 1500.0, EventSet( 75.0, 1500.0,  500.0, prod ->  75.0)),
      Produced (f2,   1500.0, 1900.0, EventSet(100.0, 1900.0,  900.0, prod -> 100.0)),
      Consumed (cons, 1900.0, 2000.0, EventSet(100.0, 2000.0, 1000.0, prod -> 100.0))
    ))

    // enqueue action 2 is not scheduled
    executor.pendingActions should have size (1)
    executor.pendingActions should contain theSameElementsAs(Set(enqueueAction2))
  }



}