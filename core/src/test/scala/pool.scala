package cell

import java.util.concurrent.ConcurrentHashMap

import java.util.concurrent.CountDownLatch

import org.scalatest.FunSuite

import scala.concurrent.{ Await, Promise }
import scala.concurrent.duration._
import lattice.{ Lattice, StringIntKey, StringIntUpdater, Updater }

class PoolSuite extends FunSuite {
  test("onQuiescent") {
    val pool = new HandlerPool

    var i = 0
    while (i < 10000) {
      val p1 = Promise[Boolean]()
      val p2 = Promise[Boolean]()
      pool.execute({ () => { p1.success(true) }: Unit }, 0)
      pool.onQuiescent { () => p2.success(true) }
      try {
        Await.result(p2.future, 1.seconds)
      } catch {
        case t: Throwable =>
          assert(false, s"failure after $i iterations")
      }
      i += 1
    }

    pool.shutdown()
  }

  test("register cells concurrently") {
    implicit val stringIntUpdater: Updater[Int] = new StringIntUpdater

    implicit val pool = new HandlerPool()
    var regCells = new ConcurrentHashMap[Cell[StringIntKey, Int], Cell[StringIntKey, Int]]()
    for (_ <- 1 to 1000) {
      pool.execute(() => {
        val completer = CellCompleter[StringIntKey, Int]("somekey")
        completer.cell.trigger()
        regCells.put(completer.cell, completer.cell)
        ()
      }, 0) // TODO check priority
    }
    val fut = pool.quiescentResolveDefaults // set all (registered) cells to 1 via key.fallback
    Await.ready(fut, 5.seconds)

    regCells.values().removeIf(_.getResult() != 0)
    assert(regCells.size === 0)
  }

  test("register cells concurrently 2") {
    implicit val stringIntUpdater: Updater[Int] = new StringIntUpdater

    implicit val pool = new HandlerPool()
    var regCells = new ConcurrentHashMap[Cell[StringIntKey, Int], Cell[StringIntKey, Int]]()
    for (_ <- 1 to 1000) {
      pool.execute(() => {
        val completer = CellCompleter[StringIntKey, Int]("somekey")
        regCells.put(completer.cell, completer.cell)
        ()
      }, 0) // TODO check priority
    }
    val fut = pool.quiescentResolveDefaults // set all (registered) cells to 1 via key.fallback
    Await.ready(fut, 5.seconds)

    assert(regCells.size === 1000)
  }


  test("prio") {
    // this tests demonstrates, that high priority tasks are executed with high priority:
    // The output should be roughly sorted.
    val n = 99
    val pool = new HandlerPool(2)
    val latch = new CountDownLatch(n)

    // generate a random list of priorities
    val ps = scala.util.Random.shuffle((-n to n).toList)

    // create a task for each priority
    ps.foreach(p => {
      pool.execute(new Runnable {
        override def run(): Unit = {
          println(p + "\t" + Thread.currentThread().getName)
          latch.countDown()
        }
      }, p)
    })
    latch.await()
    pool.shutdown()
    println(ps)
    /*
    An actual test could make use of the inversion of ps the inversion of the output o.
    If priorities are handled correctly, inv(o) <= inv(ps)


    def inv(p):
      s = 0
      n = len(p)
      for i in range(n‐1):
          for j in range(i+1):
              if p[i] > p[j]:
                  s += 1
      return s
     */
  }
}
