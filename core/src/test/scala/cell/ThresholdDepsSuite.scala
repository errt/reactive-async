package cell

import org.scalatest.FunSuite
import java.util.concurrent.CountDownLatch

import scala.util.{Failure, Success}
import scala.concurrent.Await
import scala.concurrent.duration._
import lattice.{NaturalNumberKey, _}

class ThresholdDepsSuite extends FunSuite {
  test("whenNext: triggered with threshold value") {
    val latch = new CountDownLatch(1)

    implicit val pool: HandlerPool = new HandlerPool
    implicit val lattice: NaturalNumberLattice = new NaturalNumberLattice

    val completer1 = CellCompleter[NaturalNumberKey.type, Int](NaturalNumberKey, _ => NoOutcome)
    val cell1 = completer1.cell
    cell1.onComplete { _ => latch.countDown() }
    val completer2 = CellCompleter[NaturalNumberKey.type, Int](NaturalNumberKey, _ => NoOutcome)
    val cell2 = completer2.cell

    cell1.whenNext(cell2, 5, FinalOutcome(_))
    completer2.putNext(5)

    latch.await()

    pool.shutdown()
  }

  test("whenNext: triggered with higher value") {
    val latch = new CountDownLatch(1)

    implicit val pool: HandlerPool = new HandlerPool
    implicit val lattice: NaturalNumberLattice = new NaturalNumberLattice

    val completer1 = CellCompleter[NaturalNumberKey.type, Int](NaturalNumberKey, _ => NoOutcome)
    val cell1 = completer1.cell
    cell1.onComplete { _ => latch.countDown() }
    val completer2 = CellCompleter[NaturalNumberKey.type, Int](NaturalNumberKey, _ => NoOutcome)
    val cell2 = completer2.cell

    cell1.whenNext(cell2, 5, FinalOutcome(_))
    completer2.putNext(6)

    latch.await()

    pool.shutdown()
  }

  test("whenNext: not triggered with lower value") {
    val latch = new CountDownLatch(1)

    implicit val pool: HandlerPool = new HandlerPool
    implicit val lattice: NaturalNumberLattice = new NaturalNumberLattice

    val completer1 = CellCompleter[NaturalNumberKey.type, Int](NaturalNumberKey, _ => NoOutcome)
    val cell1 = completer1.cell
    cell1.onComplete {
      // this should only be called, when the cell is resolved by its key
      case Success(v) =>
        assert(v === 2)
        latch.countDown()
      case Failure(_) =>
        fail()
        latch.countDown()
    }
    val completer2 = CellCompleter[NaturalNumberKey.type, Int](NaturalNumberKey, _ => NoOutcome)
    val cell2 = completer2.cell

    cell1.trigger() //otherwise, cell would not get resolved
    completer1.putNext(2) // cell1 will be resolved with 10 via the NaturalNumberKey
    cell1.whenNext(cell2, 5, FinalOutcome(_)) // add a thresholded dependency
    completer2.putNext(4) // this put should not trigger the dependency callback

    val fut = pool.quiescentResolveCell // resolve the cells
    Await.ready(fut, 2.seconds)

    latch.await()

    assert(cell1.getResult() === 2)
    assert(cell2.getResult() === 4)

    pool.shutdown()
  }

  test("whenNext: not triggered twice") {
    val latch = new CountDownLatch(1)
    val latch2 = new CountDownLatch(1)

    implicit val pool: HandlerPool = new HandlerPool
    implicit val lattice: NaturalNumberLattice = new NaturalNumberLattice

    val completer1 = CellCompleter[NaturalNumberKey.type, Int](NaturalNumberKey, _ => NoOutcome)
    val cell1 = completer1.cell
    cell1.onComplete {
      // this should only be called, when the cell is resolved by its key
      case Success(v) =>
        assert(v === 6)
        latch.countDown()
      case Failure(_) =>
        fail()
        latch.countDown()
    }
    cell1.onNext {
      // this should only be called once via the dep (and a second time via the key)
      case Success(v) =>
        assert(v === 6)
        latch2.countDown()
      case Failure(_) =>
        fail()
        latch2.countDown()
    }
    val completer2 = CellCompleter[NaturalNumberKey.type, Int](NaturalNumberKey, _ => NoOutcome)
    val cell2 = completer2.cell

    cell1.trigger() //otherwise, cell would not get resolved
    cell1.whenNext(cell2, 5, FinalOutcome(_)) // add a thresholded dependency
    completer2.putNext(6) // this put should trigger the dependency callback
    latch2.await() // wait for the first callback to take effect
    completer2.putNext(7) // this put should not trigger the dependency callback any more

    val fut = pool.quiescentResolveCell // resolve the cells
    Await.ready(fut, 2.seconds)

    latch.await()

    assert(cell1.getResult() === 6)
    assert(cell2.getResult() === 7)

    pool.shutdown()
  }


}
