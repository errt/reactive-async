package cell

import java.util.concurrent.CountDownLatch

import org.scalatest.FunSuite

import scala.concurrent.{ Await, Promise }
import scala.concurrent.duration._

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

  test("prio") {
    // this tests demonstrates, that high priority tasks are executed with high priority
    val n = 99
    val pool = new HandlerPool(1)
    val latch = new CountDownLatch(n)
    val l = 1 to n
    val ll = l.toList
    val ps = scala.util.Random.shuffle(ll)
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
  }
}
