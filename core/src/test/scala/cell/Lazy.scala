package cell

import java.util.concurrent.CountDownLatch

import lattice.{DefaultKey, Lattice, StringIntKey, StringIntLattice}
import org.scalatest.FunSuite

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Success

class Lazy extends FunSuite {

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

  test("init after cell completed") {
    val latch = new CountDownLatch(1)

    val pool = new HandlerPool()

    val completed = pool.createCompletedCell[Int](5)
    var cell1: Cell[DefaultKey[Int], Int] = null
    cell1 = pool.createCell(new DefaultKey[Int], () => {

      cell1.onComplete(_ => latch.countDown())
      cell1.whenComplete(completed, _ => FinalOutcome(10))

      latch.await() // meanwhile, cell1 gets final value `10` via `whenComplete`
      NextOutcome(-1)
    })

    pool.triggerExecution(cell1)

    latch.await()

    pool.shutdown()

    assert(cell1.getResult() == 10)
    assert(completed.getResult() == 5)
  }

  test("cycle deps") {
    val latch1 = new CountDownLatch(2)
    val latch2 = new CountDownLatch(2)
    val pool = new HandlerPool()

    var cell1: Cell[StringIntKey, Int] = null
    var cell2: Cell[StringIntKey, Int] = null

    cell1 = pool.createCell[StringIntKey, Int]("cell1", () => {
      cell1.whenComplete(cell2, _ => {
        FinalOutcome(3)
      })
      NextOutcome(1)
    })

    cell2 = pool.createCell[StringIntKey, Int]("cell2", () => {
      cell2.whenComplete(cell1, _ => {
        FinalOutcome(3)
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
      cell1.whenComplete(cell2, _ => NextOutcome(-1))
      NextOutcome(101)
    })

    cell2 = pool.createCell[theKey.type, Int](theKey, () => {
      cell2.whenComplete(cell1, _ => NextOutcome(-1))
      NextOutcome(102)
    })

    cell3 = pool.createCell[theKey.type, Int](theKey, () => {
      cell3.whenComplete(cell1, _ => FinalOutcome(103))
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

    //pool.whileQuiescentResolveCell
    val fut = pool.quiescentResolveCycles
    val ready = Await.ready(fut, 2.seconds)

    latch2.await()

    assert(cell3.isComplete)
    assert(cell3.getResult() === 103)

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
        NextOutcome(-111)
      })
      NextOutcome(11)
    })

    cell2 = pool.createCell[theKey.type, Int](theKey, () => {
      cell2.whenComplete(cell1, _ => {
        NextOutcome(-222)
      })
      NextOutcome(22)
    })

    cell1.onNext(_ => latch1.countDown())
    cell2.onNext(_ => latch1.countDown())

    cell1.onComplete(_ => latch2.countDown())
    cell2.onComplete(_ => latch2.countDown())

    pool.triggerExecution(cell2)
    latch1.await()

    val fut = pool.quiescentResolveCycles
    val ready = Await.ready(fut, 2.seconds)

    latch2.await()

    cell3 = pool.createCell[theKey.type, Int](theKey, () => {
      cell3.whenComplete(cell1, _ => {
        FinalOutcome(333)
      })
      NextOutcome(-3)
    })

    cell3.onComplete(_ => latch3.countDown())
    pool.triggerExecution(cell3)

    latch3.await()

    assert(cell3.isComplete)
    assert(cell3.getResult() === 333)

    pool.shutdown()
  }

  test("cycle does not get resolved, if not triggered") {
    val pool = new HandlerPool()
    var c1: Cell[StringIntKey, Int] = null
    var c2: Cell[StringIntKey, Int] = null
    c1 = pool.createCell[StringIntKey, Int]("cell1", () => {
      c1.whenNext(c2, _ => FinalOutcome(-2))
      FinalOutcome(-1)
    })
    c2 = pool.createCell[StringIntKey, Int]("cell2", () => {
      c2.whenNext(c1, _ => FinalOutcome(-2))
      FinalOutcome(-1)
    })

    val fut2 = pool.quiescentResolveCell
    Await.ready(fut2, 2.seconds)

    assert(c1.getResult() == 0)
    assert(!c1.isComplete)
    assert(c2.getResult() == 0)
    assert(!c2.isComplete)

    pool.shutdown()
  }

  test("cell does not get resolved, if not triggered") {
    val pool = new HandlerPool()
    val c = pool.createCell[StringIntKey, Int]("cell1", () => FinalOutcome(-1))

    val fut2 = pool.quiescentResolveCell
    Await.ready(fut2, 2.seconds)

    assert(c.getResult() == 0)
    assert(!c.isComplete)

    pool.shutdown()
  }

  test("cell gets resolved, if triggered") {
    val pool = new HandlerPool()
    val cell = pool.createCell[StringIntKey, Int]("cell1", () => {
      NextOutcome(-1)
    })
    pool.triggerExecution(cell)

    val fut2 = pool.quiescentResolveCell
    Await.ready(fut2, 2.seconds)

    assert(cell.isComplete) // cell should be completed with a fallback value
    assert(cell.getResult() == 1) // StringIntKey sets cell to fallback value `1`.

    pool.shutdown()
  }

  test("prio") {

    val pool = new HandlerPool()

    //var cell0, cell1, cell2: Cell[StringIntKey, Int] = null
    var cells = Map.empty[Int, Cell[StringIntKey, Int]]


    var i = 0

    for(i <- 0 to 2) {
      var c: Cell[StringIntKey, Int] = null
      c = pool.createCell[StringIntKey, Int](s"cell$i", () => {
        println(s"Init root cell$i")
        var cell = c
        var other: Cell[StringIntKey, Int] = pool.createCell[StringIntKey, Int](s"cell$i chain", () => {
//          println(s"init first cell that cell$i depends on")
          NextOutcome(1)
        })
        for (p <- 1 to 1000) {
          val last = p == 999
          cell.whenComplete(other, (x: Int) => {
//            println(s"triggering a whenNext ($p) in the chain of cell$i")
            Thread.sleep(scala.util.Random.nextInt(1))
            FinalOutcome(x + 1)
          }, i)
          cell = other
          other = pool.createCell[StringIntKey, Int]("cell", () => {
//            println(s"init a cell that cell$i depends on ($p). FinalOutcome==$last")
            Outcome(p,  last)
          })
        }
        NextOutcome(0)
      })
      cells = cells + (i -> c)
    }

    val latch = new CountDownLatch(3)

    for(i <- List(2,1,0)) {
      val c = cells(i)
      c.onComplete({case Success(x) => {
        println(s"completed cell$i with value $x")
        latch.countDown()
      }}, i)
      pool.triggerExecution(c, i)
    }

    latch.await()
    pool.shutdown()
  }
}
