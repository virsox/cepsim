package ca.uwo.eng.sel.cepsim

import ca.uwo.eng.sel.cepsim.history.{Consumed, Generated, Produced}
import ca.uwo.eng.sel.cepsim.metric.EventSet
import ca.uwo.eng.sel.cepsim.placement.Placement
import ca.uwo.eng.sel.cepsim.query._
import ca.uwo.eng.sel.cepsim.sched.{ExecuteAction, OpScheduleStrategy}
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}


@RunWith(classOf[JUnitRunner])
class QueryCloudletTest extends FlatSpec
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

    var opSchedule = mock[OpScheduleStrategy]
    doReturn(Iterator(
              ExecuteAction(prod, 1000.0, 1100.0, 100000.0),
              ExecuteAction(f1, 1100.0, 1500.0, 400000.0),
              ExecuteAction(f2, 1500.0, 1900.0, 400000.0),
              ExecuteAction(cons, 1900.0, 2000.0, 100000.0))).
      when(opSchedule).
      allocate(1000000, 1000.0, 1, placement)
  }

  trait Fixture1 extends Fixture {
    doReturn(Some(Generated(prod, 0.0, 1000, EventSet(100.0, 1000, 0, Map(prod -> 100.0))))).when(prod).generate(0.0, 1000)

    doReturn(List(Produced(prod, 1000.0, 1100.0, EventSet(100.0, 1100.0,  100.0, prod -> 100.0)))).when(prod).run(100000, 1000.0, 1100.0)
    doReturn(List(Produced(f1,   1100.0, 1500.0, EventSet(100.0, 1500.0,  500.0, prod -> 100.0)))).when(f1  ).run(400000, 1100.0, 1500.0)
    doReturn(List(Produced(f2,   1500.0, 1900.0, EventSet(100.0, 1900.0,  900.0, prod -> 100.0)))).when(f2  ).run(400000, 1500.0, 1900.0)
    doReturn(List(Consumed(cons, 1900.0, 2000.0, EventSet(100.0, 2000.0, 1000.0, prod -> 100.0)))).when(cons).run(100000, 1900.0, 2000.0)
  }

  "A QueryCloudlet" should "correctly initialize all operators" in new Fixture {
    val cloudlet = QueryCloudlet.apply("c1", placement, opSchedule)
    cloudlet.init(0.0)

    verify(prod).init(0.0)
    verify(f1).init(0.0)
    verify(f2).init(0.0)
    verify(cons).init(0.0)
  }

  it should "correctly enqueue events received from the network" in new Fixture {
    // TODO fix this test after working on networked queries
    val cloudlet = QueryCloudlet.apply("c1", placement, opSchedule)
    val history = cloudlet.enqueue(100.0, f1, prod, 1000)

//    verify(f1).enqueueIntoInput(prod, 1000)


    val entries = history.from(f1)
    entries should have size (0)
    //entries should contain theSameElementsInOrderAs ())
  }

  // --------------------------------------------------

  it should "correctly run all operators" in new Fixture1 {
    val cloudlet = QueryCloudlet("c1", placement, opSchedule) //, 0.0)
    cloudlet.init(0.0)

    doReturn(100.0).when(prod).outputQueues(f1)
    doReturn(100.0).when(f1).outputQueues(f2)
    doReturn(100.0).when(f2).outputQueues(cons)

    // the cloudlet should run all operators
    val history = cloudlet run(1000000, 1000.0, 1)

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


  it should "not run operators that are in a different Placement" in new Fixture1 {
    val cloudlet = QueryCloudlet("c1", placement, opSchedule)
    cloudlet.init(0.0)

    // create new operators
    val f3 = mock[Operator]
    val cons2 = mock[EventConsumer]

    doReturn(Set(f2)).when(f3).predecessors
    doReturn(Set(f2)).when(cons).predecessors
    doReturn(Set(f3)).when(cons2).predecessors

    doReturn(Set(f3, cons)).when(f2).successors
    doReturn(Set(cons2)).when(f3).successors

    when(prod.outputQueues(anyObject[Vertex]())).thenReturn(0.0)
    when(f1.outputQueues(anyObject[Vertex]())).thenReturn(0.0)
    when(f2.outputQueues(cons)).thenReturn(100.0)
    when(f2.outputQueues(  f3)).thenReturn(100.0)

    // the cloudlet should run all operators
    val history = cloudlet run(1000000, 1000.0, 1)

    verify(prod).generate(0.0, 1000.0)
    verify(prod).run(100000, 1000.0, 1100.0)
    verify(f1  ).run(400000, 1100.0, 1500.0)
    verify(f2  ).run(400000, 1500.0, 1900.0)
    verify(cons).run(100000, 1900.0, 2000.0)

    // these operators shouldn't run
    verify(f3, never()).run(anyDouble(), anyDouble(), anyDouble())
    verify(cons2, never()).run(anyDouble(), anyDouble(), anyDouble())


    // TODO fix this test after working on networked queries
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
      allocate(500000, 500.0, 1, placement)

    // second iteration
    doReturn(Iterator(
        ExecuteAction(prod, 1000.0, 1050.0,  50000.0), ExecuteAction(f1,   1050.0, 1250.0, 200000.0),
        ExecuteAction(f2,   1250.0, 1450.0, 200000.0), ExecuteAction(cons, 1450.0, 1500.0,  50000.0))).
      when(opSchedule).
      allocate(500000, 1000.0, 1, placement)


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
    val cloudlet = QueryCloudlet.apply("c1", placement, opSchedule, 2)
    cloudlet.init(0.0)

    when(prod.outputQueues(anyObject[Vertex]())).thenReturn(50.0)
    when(  f1.outputQueues(anyObject[Vertex]())).thenReturn(50.0)
    when(  f2.outputQueues(anyObject[Vertex]())).thenReturn(50.0)

    // the cloudlet should run all operators
    cloudlet run(1000000, 500.0, 1)  // 1 million instructions @ 1 MIPS = 1 second

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

}