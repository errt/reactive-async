package com.phaller.rasync
package test

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ CountDownLatch, TimeUnit }

import com.phaller.rasync.lattice._
import com.phaller.rasync.lattice.lattices.{ NaturalNumberKey, NaturalNumberLattice, PowerSetLattice }
import com.phaller.rasync.test.lattice._
import org.scalatest.FunSuite

import scala.concurrent.duration._
import scala.concurrent.{ Await, Promise }
import scala.util.{ Failure, Success }

class WhenMultiSuite extends FunSuite {

  implicit val stringIntUpdater: Updater[Int] = new StringIntUpdater

  test("when: values passed to callback 1") {
    val latch1 = new CountDownLatch(1)
    val latch2 = new CountDownLatch(1)

    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("somekey")
    val completer2 = CellCompleter[StringIntKey, Int]("someotherkey")

    val cell1 = completer1.cell
    cell1.whenMulti(List(completer2.cell), _ => {
      Outcome(completer2.cell.getResult(), completer2.cell.isComplete) // complete, if completer2 is completed
    })

    assert(cell1.numNextDependencies == 1)
    assert(cell1.numTotalDependencies == 1)

    cell1.onNext {
      case Success(x) =>
        assert((x === 8 && !cell1.isComplete) || x === 10)
        latch1.countDown()
      case Failure(e) =>
        assert(false)
        latch1.countDown()
    }

    cell1.onComplete {
      case Success(x) =>
        assert(x === 10)
        latch2.countDown()
      case Failure(e) =>
        assert(false)
        latch2.countDown()
    }

    completer1.putNext(8)
    latch1.await()

    assert(!cell1.isComplete)

    completer2.putFinal(10)
    latch2.await()

    assert(cell1.isComplete)

    pool.onQuiescenceShutdown()
  }

  test("whenSequential: values passed to callback 1") {
    val latch1 = new CountDownLatch(1)
    val latch2 = new CountDownLatch(1)

    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("somekey")
    val completer2 = CellCompleter[StringIntKey, Int]("someotherkey")

    val cell1 = completer1.cell
    cell1.whenMultiSequential(List(completer2.cell), _ => {
      Outcome(completer2.cell.getResult(), completer2.cell.isComplete) // complete, if completer2 is completed
    })

    assert(cell1.numNextDependencies == 1)
    assert(cell1.numTotalDependencies == 1)

    cell1.onNext {
      case Success(x) =>
        assert((x === 8 && !cell1.isComplete) || x === 10)
        latch1.countDown()
      case Failure(e) =>
        assert(false)
        latch1.countDown()
    }

    cell1.onComplete {
      case Success(x) =>
        assert(x === 10)
        latch2.countDown()
      case Failure(e) =>
        assert(false)
        latch2.countDown()
    }

    completer1.putNext(8)
    latch1.await()

    assert(!cell1.isComplete)

    completer2.putFinal(10)
    latch2.await()

    assert(cell1.isComplete)

    pool.onQuiescenceShutdown()
  }

  test("whenMulti: values passed to callback 2") {
    val latch1 = new CountDownLatch(1)

    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("somekey")
    val completer20 = CellCompleter[StringIntKey, Int]("someotherkey")
    val completer21 = CellCompleter[StringIntKey, Int]("someotherkey")
    val completer22 = CellCompleter[StringIntKey, Int]("someotherkey")

    val cell1 = completer1.cell
    cell1.whenMulti(List(completer20.cell, completer22.cell, completer21.cell), {
      case completer20.cell =>
        assert(completer20.cell.getResult() === 7)
        NoOutcome
      case completer21.cell =>
        assert(completer21.cell.getResult() === 9)
        NoOutcome
      case completer22.cell =>
        assert(completer22.cell.getResult() === 10)
        FinalOutcome(10)
    })

    assert(cell1.numTotalDependencies == 3)

    cell1.onComplete {
      case Success(x) =>
        assert(x === 10)
        latch1.countDown()
      case Failure(e) =>
        assert(false)
        latch1.countDown()
    }

    completer20.putNext(7)
    completer21.putNext(9)
    completer22.putNext(10)
    latch1.await()

    assert(cell1.isComplete)

    pool.onQuiescenceShutdown()
  }

  test("whenMultiSequenial: values passed to callback 2") {
    val latch1 = new CountDownLatch(1)

    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("somekey")
    val completer20 = CellCompleter[StringIntKey, Int]("someotherkey")
    val completer21 = CellCompleter[StringIntKey, Int]("someotherkey")
    val completer22 = CellCompleter[StringIntKey, Int]("someotherkey")

    val cell1 = completer1.cell
    cell1.whenMultiSequential(List(completer20.cell, completer22.cell, completer21.cell), {
      case completer20.cell =>
        assert(completer20.cell.getResult() === 7)
        NoOutcome
      case completer21.cell =>
        assert(completer21.cell.getResult() === 9)
        NoOutcome
      case completer22.cell =>
        assert(completer22.cell.getResult() === 10)
        FinalOutcome(10)
    })

    assert(cell1.numTotalDependencies == 3)

    cell1.onComplete {
      case Success(x) =>
        assert(x === 10)
        latch1.countDown()
      case Failure(e) =>
        assert(false)
        latch1.countDown()
    }

    completer20.putNext(7)
    completer21.putNext(9)
    completer22.putNext(10)
    latch1.await()

    assert(cell1.isComplete)

    pool.onQuiescenceShutdown()
  }

  test("whenMulti: concurrent puts") {
    for (i <- (1 to 1000).par) {
      val latch1 = new CountDownLatch(1)

      implicit val pool: HandlerPool = new HandlerPool
      implicit val theLattice: PowerSetLattice[Int] = new PowerSetLattice[Int]
      val theKey = new DefaultKey[Set[Int]]
      val completer1 = CellCompleter[theKey.type, Set[Int]](theKey)
      val completer20 = CellCompleter[theKey.type, Set[Int]](theKey)
      val completer21 = CellCompleter[theKey.type, Set[Int]](theKey)
      val completer22 = CellCompleter[theKey.type, Set[Int]](theKey)
      val completer3 = CellCompleter[theKey.type, Set[Int]](theKey)

      val cell1 = completer1.cell
      cell1.whenMulti(List(completer20.cell, completer22.cell, completer21.cell), c => {
        NextOutcome(c.getResult())
      })

      val cell3 = completer3.cell
      cell1.whenNext(cell3, FinalOutcome(_))
      cell3.whenNext(cell1, v => if (v.size == 3) FinalOutcome(Set(11)) else NoOutcome)

      assert(cell1.numTotalDependencies == 4)

      cell1.onComplete {
        case Success(x) =>
          assert(x === Set(7, 9, 10, 11))
          latch1.countDown()
        case Failure(e) =>
          assert(false)
          latch1.countDown()
      }

      pool.execute(() => completer20.putNext(Set(7)))
      pool.execute(() => completer21.putNext(Set(9)))
      pool.execute(() => completer22.putNext(Set(10)))

      latch1.await()

      assert(cell1.isComplete)

      pool.onQuiescenceShutdown()
    }
  }

  test("whenMultiSequential: concurrent puts") {
    for (i <- (1 to 1000).par) {
      val latch1 = new CountDownLatch(1)

      implicit val pool: HandlerPool = new HandlerPool
      implicit val theLattice: PowerSetLattice[Int] = new PowerSetLattice[Int]
      val theKey = new DefaultKey[Set[Int]]
      val completer1 = CellCompleter[theKey.type, Set[Int]](theKey)
      val completer20 = CellCompleter[theKey.type, Set[Int]](theKey)
      val completer21 = CellCompleter[theKey.type, Set[Int]](theKey)
      val completer22 = CellCompleter[theKey.type, Set[Int]](theKey)
      val completer3 = CellCompleter[theKey.type, Set[Int]](theKey)

      val cell1 = completer1.cell
      cell1.whenMultiSequential(List(completer20.cell, completer22.cell, completer21.cell), c => {
        NextOutcome(c.getResult())
      })

      val cell3 = completer3.cell
      cell1.whenNextSequential(cell3, FinalOutcome(_))
      cell3.whenNext(cell1, v => if (v.size == 3) FinalOutcome(Set(11)) else NoOutcome)

      assert(cell1.numTotalDependencies == 4)

      cell1.onComplete {
        case Success(x) =>
          assert(x === Set(7, 9, 10, 11))
          latch1.countDown()
        case Failure(e) =>
          assert(false)
          latch1.countDown()
      }

      pool.execute(() => completer20.putNext(Set(7)))
      pool.execute(() => completer21.putNext(Set(9)))
      pool.execute(() => completer22.putNext(Set(10)))

      latch1.await()

      assert(cell1.isComplete)

      pool.onQuiescenceShutdown()
    }
  }

  test("DefaultKey.resolve") {
    implicit val pool = new HandlerPool
    val k = new DefaultKey[Int]
    val completer1 = CellCompleter[DefaultKey[Int], Int](k)
    val completer2 = CellCompleter[DefaultKey[Int], Int](k)
    completer1.cell.whenMulti(List(completer2.cell), _ => NextOutcome(completer2.cell.getResult()))
    completer2.cell.whenMulti(List(completer1.cell), _ => NextOutcome(completer1.cell.getResult()))
    completer1.putNext(5)
    Await.ready(pool.quiescentResolveCycles, 2.seconds)
    assert(completer1.cell.isComplete)
    assert(completer2.cell.isComplete)
    assert(completer1.cell.getResult() == 5)
    assert(completer2.cell.getResult() == 5)
    pool.shutdown()
  }

  test("quiescent incomplete cells") {
    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("key1")
    val completer2 = CellCompleter[StringIntKey, Int]("key2")
    val cell1 = completer1.cell
    val cell2 = completer2.cell
    cell1.whenMulti(List(cell2), _ => NoOutcome)
    cell2.whenMulti(List(cell1), _ => NoOutcome)
    val incompleteFut = pool.quiescentIncompleteCells
    val cells = Await.result(incompleteFut, 2.seconds)
    assert(cells.map(_.key).toList.toString == "List(key1, key2)")
  }

  test("quiescent resolve cycle") {
    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[StringIntKey, Int]("key1")
    val completer2 = CellCompleter[StringIntKey, Int]("key2")
    val cell1 = completer1.cell
    val cell2 = completer2.cell
    cell1.whenMulti(List(cell2), _ => NoOutcome)
    cell2.whenMulti(List(cell1), _ => NoOutcome)
    val qfut = pool.quiescentResolveCell
    Await.ready(qfut, 2.seconds)
    val incompleteFut = pool.quiescentIncompleteCells
    val cells = Await.result(incompleteFut, 2.seconds)
    assert(cells.isEmpty)
  }

  test("whenComplete: cycle with additional incoming dep") {
    sealed trait Value
    case object Bottom extends Value
    case object Resolved extends Value
    case object Fallback extends Value
    case object OK extends Value
    case object ShouldNotHappen extends Value

    implicit object ValueUpdater extends Updater[Value] {
      override def update(v1: Value, v2: Value): Value = v2
      override val initial: Value = Bottom
    }

    object TheKey extends DefaultKey[Value] {
      override def resolve[K <: Key[Value]](cells: Iterable[Cell[K, Value]]): Iterable[(Cell[K, Value], Value)] = {
        cells.map(cell => (cell, Resolved))
      }
      override def fallback[K <: Key[Value]](cells: Iterable[Cell[K, Value]]): Iterable[(Cell[K, Value], Value)] = {
        cells.map(cell => (cell, Fallback))
      }
    }

    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[TheKey.type, Value](TheKey)
    val completer2 = CellCompleter[TheKey.type, Value](TheKey)
    val cell1 = completer1.cell
    val cell2 = completer2.cell
    val in = CellCompleter[TheKey.type, Value](TheKey)

    // let `cell1` and `cell2` form a cycle
    cell1.whenMulti(List(cell2), _ => NextOutcome(ShouldNotHappen))
    cell2.whenMulti(List(cell1), _ => NextOutcome(ShouldNotHappen))

    // the cycle is dependent on incoming information from `in`
    cell2.whenComplete(in.cell, v => { NextOutcome(ShouldNotHappen) })

    // resolve the independent cell `in` and the cycle
    val fut = pool.quiescentResolveCell
    Await.ready(fut, 1.minutes)

    pool.onQuiescenceShutdown()

    assert(cell1.getResult() != ShouldNotHappen)
    assert(cell2.getResult() != ShouldNotHappen)
    assert(in.cell.getResult() == Fallback)
  }

  test("whenSequential: calling sequentially") {
    val n = 1000

    val runningCallbacks = new AtomicInteger(0)
    val latch = new CountDownLatch(1)
    val random = new scala.util.Random()

    implicit val pool = new HandlerPool
    val completer1 = CellCompleter[NaturalNumberKey.type, Int](NaturalNumberKey)(Updater.latticeToUpdater(new NaturalNumberLattice), pool)
    val completer2 = CellCompleter[NaturalNumberKey.type, Int](NaturalNumberKey)(Updater.latticeToUpdater(new NaturalNumberLattice), pool)

    val cell1 = completer1.cell
    cell1.whenMultiSequential(List(completer2.cell), _ => {
      assert(runningCallbacks.incrementAndGet() == 1)
      val x = completer2.cell.getResult()
      Thread.`yield`()
      try {
        Thread.sleep(random.nextInt(3))
      } catch {
        case _: InterruptedException => /* ignore */
      }
      assert(runningCallbacks.decrementAndGet() == 0)
      Outcome(x * n, x == n)
    })

    cell1.onComplete(_ => {
      latch.countDown()
    })

    for (i <- 1 to n)
      pool.execute(() => completer2.putNext(i))

    latch.await()

    assert(cell1.getResult() == n * n)
    assert(completer2.cell.getResult() == n)

    pool.onQuiescenceShutdown()
  }

  test("whenSequential: state") {
    // cell1 has deps to 1000 cells. All callbacks
    // share a counter (i.e. state) that must not be
    // incremented concurrently
    val n = 1000
    var count = Set[Int]()

    class PowerSetLattice[T] extends Lattice[Set[T]] {

      def join(left: Set[T], right: Set[T]): Set[T] =
        left ++ right

      val bottom: Set[T] =
        Set[T]()

    }

    val theUpdater = Updater.latticeToUpdater(new PowerSetLattice[Int])

    val latch = new CountDownLatch(1)
    val random = new scala.util.Random()

    implicit val pool = new HandlerPool
    val theKey = new DefaultKey[Set[Int]]
    val completer1 = CellCompleter[DefaultKey[Set[Int]], Set[Int]](theKey)(theUpdater, pool)
    val cell1 = completer1.cell

    cell1.onComplete(_ => {
      latch.countDown()
    })

    for (i <- 1 to n) {
      val completer2 = CellCompleter[DefaultKey[Set[Int]], Set[Int]](theKey)(theUpdater, pool)
      val completer3 = CellCompleter[DefaultKey[Set[Int]], Set[Int]](theKey)(theUpdater, pool)
      cell1.whenMultiSequential(List(completer2.cell, completer3.cell), _ => {
        count = count ++ Set(count.size)
        Thread.`yield`()
        try {
          Thread.sleep(random.nextInt(3))
        } catch {
          case _: InterruptedException => /* ignore */
        }
        Outcome(count, count.size == 2 * n)
      })
      pool.execute(() => completer2.putNext(Set(2 * i)))
      pool.execute(() => completer3.putNext(Set(2 * i + 1)))
    }

    latch.await()

    assert(cell1.getResult().size == 2 * n)

    pool.onQuiescenceShutdown()
  }

  test("whenCompleteSequential: discard callbacks on completion") {
    val latch1 = new CountDownLatch(1)
    val latch2 = new CountDownLatch(1)

    implicit val pool = new HandlerPool

    val completer1 = CellCompleter[NaturalNumberKey.type, Int](NaturalNumberKey)
    val completer2 = CellCompleter[NaturalNumberKey.type, Int](NaturalNumberKey)
    val completer3 = CellCompleter[NaturalNumberKey.type, Int](NaturalNumberKey)
    val cell1 = completer1.cell
    val cell2 = completer2.cell
    val cell3 = completer3.cell
    cell1.trigger()

    cell1.whenMultiSequential(List(cell2), _ => {
      latch1.await() // wait for some puts/triggers
      FinalOutcome(10)
    })
    cell1.whenMultiSequential(List(cell3), _ => NextOutcome(cell3.getResult()))

    completer2.putFinal(3)
    completer3.putNext(2)
    completer3.putNext(3)
    latch1.countDown()

    pool.onQuiescent(() => {
      pool.onQuiescenceShutdown()
      latch2.countDown()
    })
    // pool needs to reach quiescence, even if cell1 is completed early:
    latch2.await()

    assert(cell1.getResult() == 10)
    assert(cell2.getResult() == 3)
    assert(cell3.getResult() == 3)
    assert(cell1.isComplete)
    assert(cell2.isComplete)
    assert(!cell3.isComplete)
  }

}
