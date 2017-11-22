package cell

import java.util.concurrent.CountDownLatch

import lattice.{DefaultKey, Lattice, StringIntKey, StringIntLattice}
import org.scalatest.FunSuite

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

class OnDemand extends FunSuite {

  implicit val stringIntLattice: Lattice[Int] = new StringIntLattice

  test("lazy init") {
    val latch = new CountDownLatch(1)
    val pool = new HandlerPool()
    val cell = pool.createCell[StringIntKey, Int]("cell", () => {
      FinalOutcome(1)
    })
    cell.onComplete(_ => latch.countDown())

    assert(!cell.isComplete)
    pool.triggerExecution(cell)

    latch.await()

    assert(cell.isComplete)
    assert(cell.getResult() == 1)

    pool.shutdown()
  }

  test("trigger dependees") {
    val latch = new CountDownLatch(2)
    val pool = new HandlerPool()

    var cell1: Cell[StringIntKey, Int] = null
    var cell2: Cell[StringIntKey, Int] = null

    cell1 = pool.createCell[StringIntKey, Int]("cell1", () => {
      FinalOutcome(1)
    })

    cell2 = pool.createCell[StringIntKey, Int]("cell2", () => {
      cell2.whenComplete(cell1, _ => {
        FinalOutcome(3)
      })
      NextOutcome(2)
    })

    cell1.onComplete(_ => latch.countDown())
    cell2.onComplete(_ => latch.countDown())

    assert(!cell1.isComplete)
    assert(!cell2.isComplete)
    pool.triggerExecution(cell2)

    latch.await()

    assert(cell1.isComplete)
    assert(cell1.getResult() == 1)

    assert(cell2.isComplete)
    assert(cell2.getResult() == 3)

    pool.shutdown()
  }

  test("do not trigger unneeded cells") {
    val latch = new CountDownLatch(1)
    val pool = new HandlerPool()

    var cell1: Cell[StringIntKey, Int] = null
    var cell2: Cell[StringIntKey, Int] = null

    cell1 = pool.createCell[StringIntKey, Int]("cell1", () => {
      assert(false)
      FinalOutcome(-11)
    })

    cell2 = pool.createCell[StringIntKey, Int]("cell2", () => {
      FinalOutcome(2)
    })

    cell2.onComplete(_ => latch.countDown())

    pool.triggerExecution(cell2)

    latch.await()

    assert(!cell1.isComplete)
    assert(cell1.getResult() == 0)

    pool.shutdown()
  }

  test("cycle deps") {
    val latch1 = new CountDownLatch(2)
    val latch2 = new CountDownLatch(2)
    val pool = new HandlerPool()

    var cell1: Cell[StringIntKey, Int] = null
    var cell2: Cell[StringIntKey, Int] = null

    cell1 = pool.createCell[StringIntKey, Int]("cell1", () => {
      cell1.whenComplete(cell2, _ => {
        NextOutcome(3) // TODO Use FinalOutcome, if cycleDeps are removed correctly!
      })
      NextOutcome(1)
    })

    cell2 = pool.createCell[StringIntKey, Int]("cell2", () => {
      cell2.whenComplete(cell1, _ => {
        NextOutcome(3) // TODO Use FinalOutcome, if cycleDeps are removed correctly!
      })
      NextOutcome(2)
    })

    cell1.onNext(_ => latch1.countDown())
    cell2.onNext(_ => latch1.countDown())

    cell1.onComplete(_ => latch2.countDown())
    cell2.onComplete(_ => latch2.countDown())

    pool.triggerExecution(cell2)
    latch1.await()

    pool.whileQuiescentResolveCell
    val fut = pool.quiescentResolveCycles
    val ready = Await.ready(fut, 2.seconds)

    latch2.await()

    assert(cell1.isComplete)
    assert(cell1.getResult() == 0)

    assert(cell2.isComplete)
    assert(cell2.getResult() == 0)

    pool.shutdown()
  }

  test("cycle deps with outgoing dep") {
    val theKey = new DefaultKey[Int]()

    val latch1 = new CountDownLatch(2)
    val latch2 = new CountDownLatch(3)
    val pool = new HandlerPool()

    var cell1: Cell[theKey.type, Int] = null
    var cell2: Cell[theKey.type, Int] = null
    var cell3: Cell[theKey.type, Int] = null

    cell1 = pool.createCell[theKey.type, Int](theKey, () => {
      cell1.whenComplete(cell2, _ => {
        NextOutcome(-1)
      })
      NextOutcome(1)
    })

    cell2 = pool.createCell[theKey.type, Int](theKey, () => {
      cell2.whenComplete(cell1, _ => {
        NextOutcome(-1)
      })
      NextOutcome(1)
    })

    cell3 = pool.createCell[theKey.type, Int](theKey, () => {
      cell3.whenComplete(cell1, _ => {
        FinalOutcome(10)
      })
      NextOutcome(-1)
    })

    cell1.onNext(_ => latch1.countDown())
    cell2.onNext(_ => latch1.countDown())

    cell1.onComplete(_ => latch2.countDown())
    cell2.onComplete(_ => latch2.countDown())
    cell3.onComplete(_ => latch2.countDown())

    assert(!cell1.isComplete)
    assert(!cell2.isComplete)

    pool.triggerExecution(cell3)
    latch1.await()

    pool.whileQuiescentResolveCell
    val fut = pool.quiescentResolveCycles
    val ready = Await.ready(fut, 2.seconds)

    latch2.await()

    assert(cell3.isComplete)
    assert(cell3.getResult() === 10)

    pool.shutdown()
  }

  test("cycle deps with outgoing dep, resolve cycle first") {
    val theKey = new DefaultKey[Int]()

    val latch1 = new CountDownLatch(2)
    val latch2 = new CountDownLatch(2)
    val latch3 = new CountDownLatch(1)
    val pool = new HandlerPool()

    var cell1: Cell[theKey.type, Int] = null
    var cell2: Cell[theKey.type, Int] = null
    var cell3: Cell[theKey.type, Int] = null

    cell1 = pool.createCell[theKey.type, Int](theKey, () => {
      cell1.whenComplete(cell2, _ => {
        NextOutcome(-1)
      })
      NextOutcome(1)
    })

    cell2 = pool.createCell[theKey.type, Int](theKey, () => {
      cell2.whenComplete(cell1, _ => {
        NextOutcome(-1)
      })
      NextOutcome(1)
    })

    cell1.onNext(_ => latch1.countDown())
    cell2.onNext(_ => latch1.countDown())

    cell1.onComplete(_ => latch2.countDown())
    cell2.onComplete(_ => latch2.countDown())

    pool.triggerExecution(cell2)
    latch1.await()

    pool.whileQuiescentResolveCell
    val fut = pool.quiescentResolveCycles
    val ready = Await.ready(fut, 2.seconds)

    latch2.await()

    cell3 = pool.createCell[theKey.type, Int](theKey, () => {
      cell3.whenComplete(cell1, _ => {
        FinalOutcome(10)
      })
      NextOutcome(-1)
    })

    cell3.onComplete(_ => latch3.countDown())
    pool.triggerExecution(cell3)

    latch3.await()

    assert(cell3.isComplete)
    assert(cell3.getResult() === 10)

    pool.shutdown()
  }

  test("cycle does not get resolved, if not triggered") {
    val pool = new HandlerPool()
    var cell1: Cell[StringIntKey, Int] = null
    var cell2: Cell[StringIntKey, Int] = null
    cell1 = pool.createCell[StringIntKey, Int]("cell1", () => {
      cell1.whenNext(cell2, _ => FinalOutcome(-2))
      FinalOutcome(-1)
    })
    cell2 = pool.createCell[StringIntKey, Int]("cell1", () => {
      cell2.whenNext(cell1, _ => FinalOutcome(-2))
      FinalOutcome(-1)
    })

    val fut2 = pool.quiescentResolveCell
    Await.ready(fut2, 2.seconds)

    assert(cell1.getResult() == 0)
    assert(!cell1.isComplete)
    assert(cell2.getResult() == 0)
    assert(!cell2.isComplete)

    pool.shutdown()
  }

  test("cell does not get resolved, if not triggered") {
    val pool = new HandlerPool()
    val cell = pool.createCell[StringIntKey, Int]("cell1", () => FinalOutcome(-1))

    val fut2 = pool.quiescentResolveCell
    Await.ready(fut2, 2.seconds)

    assert(cell.getResult() == 0)
    assert(!cell.isComplete)

    pool.shutdown()
  }

  test("cell gets resolved, if triggered") {
    val pool = new HandlerPool()
    val cell = pool.createCell[StringIntKey, Int]("cell1", () => NextOutcome(-1))
    pool.triggerExecution(cell)

    val fut2 = pool.quiescentResolveCell
    Await.ready(fut2, 2.seconds)

    assert(cell.getResult() == 1)
    assert(cell.isComplete)

    pool.shutdown()
  }
}
