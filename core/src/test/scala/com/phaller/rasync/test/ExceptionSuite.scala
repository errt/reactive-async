package com.phaller.rasync
package test

import scala.language.implicitConversions

import java.util.concurrent.{ CountDownLatch, TimeUnit }

import scala.concurrent.duration._
import com.phaller.rasync.lattice._
import org.scalatest.FunSuite

import scala.concurrent.Await
import scala.util.{ Failure, Success }

class ExceptionSuite extends FunSuite {
  /*
   * This Suite contains test cases, where exceptions are thrown
   * - when initializing cells,
   * - in dependency callbacks
   * - in a Key's methods (fallback, resolve)
   *
   * It also tests, whether completed cells (with Success or Failure)
   * behave correclty wrt. exceptions in dependencies and keys.
   */

  implicit val naturalNumberUpdater: Updater[Int] = Updater.latticeToUpdater(new NaturalNumberLattice)
  implicit def strToIntKey(s: String): NaturalNumberKey.type = NaturalNumberKey

  test("exception in init") {
    // If the init method throws an exception e,
    // the cell is completed with Failure(e)
    val latch = new CountDownLatch(1)
    val pool = new HandlerPool()
    val cell = pool.mkCell[NaturalNumberKey.type, Int](NaturalNumberKey, _ => {
      throw new Exception("foo")
    })

    // cell should be completed as a consequence
    cell.onComplete(_ => latch.countDown())
    cell.trigger()

    assert(!cell.isComplete)

    // wait for cell to be completed
    latch.await()

    assert(cell.isComplete)
    // check, if cell has been completed with an exception
    cell.getResultTry() match {
      case Success(_) => assert(false)
      case Failure(e) => assert(e.getMessage == "foo")
    }

    pool.shutdown()
  }

  test("exception in whenNext callback") {
    // If the callback thrown an exception e,
    // the dependent cell is completed with Failure e
    val latch = new CountDownLatch(1)
    implicit val pool: HandlerPool = new HandlerPool()
    val c0 = CellCompleter[NaturalNumberKey.type, Int](NaturalNumberKey)
    val cell = pool.mkCell[NaturalNumberKey.type, Int](NaturalNumberKey, c => {
      // build up dependency, throw exeption, if c0's value changes
      c.whenNext(c0.cell, _ => throw new Exception("foo"))
      NoOutcome
    })

    // cell should be completed as a consequence
    cell.onComplete(_ => latch.countDown())
    cell.trigger()

    assert(!cell.isComplete)

    // trigger dependency, s.t. callback is called
    c0.putFinal(1)

    // wait for cell to be completed
    latch.await()

    assert(cell.isComplete)
    // check, if cell has been completed with an exception
    cell.getResultTry() match {
      case Success(_) => assert(false)
      case Failure(e) => assert(e.getMessage == "foo")
    }

    pool.shutdown()
  }

  test("exception in whenComplete callback") {
    // If the callback thrown an exception e,
    // the dependent cell is completed with Failure e
    val latch = new CountDownLatch(1)
    implicit val pool: HandlerPool = new HandlerPool()
    val c0 = CellCompleter[NaturalNumberKey.type, Int](NaturalNumberKey)
    val cell = pool.mkCell[NaturalNumberKey.type, Int](NaturalNumberKey, c => {
      // build up dependency, throw exeption, if c0's value changes
      c.whenComplete(c0.cell, _ => throw new Exception("foo"))
      NoOutcome
    })

    // cell should be completed as a consequence
    cell.onComplete(_ => latch.countDown())
    cell.trigger()

    assert(!cell.isComplete)

    // trigger dependency, s.t. callback is called
    c0.putFinal(1)

    // wait for cell to be completed
    latch.await()

    assert(cell.isComplete)
    // check, if cell has been completed with an exception
    cell.getResultTry() match {
      case Success(_) => assert(false)
      case Failure(e) => assert(e.getMessage == "foo")
    }

    pool.shutdown()
  }

  test("exception in when callback") {
    // If the callback thrown an exception e,
    // the dependent cell is completed with Failure e
    val latch = new CountDownLatch(1)
    implicit val pool: HandlerPool = new HandlerPool()
    val c0 = CellCompleter[NaturalNumberKey.type, Int](NaturalNumberKey)
    val cell = pool.mkCell[NaturalNumberKey.type, Int](NaturalNumberKey, c => {
      // build up dependency, throw exeption, if c0's value changes
      c.when(c0.cell, (_, _) => throw new Exception("foo"))
      NoOutcome
    })

    // cell should be completed as a consequence
    cell.onComplete(_ => latch.countDown())
    cell.trigger()

    assert(!cell.isComplete)

    // trigger dependency, s.t. callback is called
    c0.putFinal(1)

    // wait for cell to be completed
    latch.await()

    assert(cell.isComplete)
    // check, if cell has been completed with an exception
    cell.getResultTry() match {
      case Success(_) => assert(false)
      case Failure(e) => assert(e.getMessage == "foo")
    }

    pool.shutdown()
  }

  test("exception in whenNextSequential callback") {
    // If the callback thrown an exception e,
    // the dependent cell is completed with Failure e
    val latch = new CountDownLatch(1)
    implicit val pool: HandlerPool = new HandlerPool()
    val c0 = CellCompleter[NaturalNumberKey.type, Int](NaturalNumberKey)
    val cell = pool.mkCell[NaturalNumberKey.type, Int](NaturalNumberKey, c => {
      // build up dependency, throw exeption, if c0's value changes
      c.whenNextSequential(c0.cell, _ => throw new Exception("foo"))
      NoOutcome
    })

    // cell should be completed as a consequence
    cell.onComplete(_ => latch.countDown())
    cell.trigger()

    assert(!cell.isComplete)

    // trigger dependency, s.t. callback is called
    c0.putFinal(1)

    // wait for cell to be completed
    latch.await()

    assert(cell.isComplete)
    // check, if cell has been completed with an exception
    cell.getResultTry() match {
      case Success(_) => assert(false)
      case Failure(e) => assert(e.getMessage == "foo")
    }

    pool.shutdown()
  }

  test("exception in whenCompleteSequential callback") {
    // If the callback thrown an exception e,
    // the dependent cell is completed with Failure e
    val latch = new CountDownLatch(1)
    implicit val pool: HandlerPool = new HandlerPool()
    val c0 = CellCompleter[NaturalNumberKey.type, Int](NaturalNumberKey)
    val cell = pool.mkCell[NaturalNumberKey.type, Int](NaturalNumberKey, c => {
      // build up dependency, throw exeption, if c0's value changes
      c.whenCompleteSequential(c0.cell, _ => throw new Exception("foo"))
      NoOutcome
    })

    // cell should be completed as a consequence
    cell.onComplete(_ => latch.countDown())
    cell.trigger()

    assert(!cell.isComplete)

    // trigger dependency, s.t. callback is called
    c0.putFinal(1)

    // wait for cell to be completed
    latch.await()

    assert(cell.isComplete)
    // check, if cell has been completed with an exception
    cell.getResultTry() match {
      case Success(_) => assert(false)
      case Failure(e) => assert(e.getMessage == "foo")
    }

    pool.shutdown()
  }

  test("exception in whenSequential callback") {
    // If the callback thrown an exception e,
    // the dependent cell is completed with Failure e
    val latch = new CountDownLatch(1)
    implicit val pool: HandlerPool = new HandlerPool()
    val c0 = CellCompleter[NaturalNumberKey.type, Int](NaturalNumberKey)
    val cell = pool.mkCell[NaturalNumberKey.type, Int](NaturalNumberKey, c => {
      // build up dependency, throw exeption, if c0's value changes
      c.whenSequential(c0.cell, (_, _) => throw new Exception("foo"))
      NoOutcome
    })

    // cell should be completed as a consequence
    cell.onComplete(_ => latch.countDown())
    cell.trigger()

    assert(!cell.isComplete)

    // trigger dependency, s.t. callback is called
    c0.putFinal(1)

    // wait for cell to be completed
    latch.await()

    assert(cell.isComplete)
    // check, if cell has been completed with an exception
    cell.getResultTry() match {
      case Success(_) => assert(false)
      case Failure(e) => assert(e.getMessage == "foo")
    }

    pool.shutdown()
  }

  test("exception in Key.resolve") {
    // If Key.resolved is called for cSSC of cells c
    // and throws an exception e, all cells c are completed
    // with Failure(e).

    // Define a key that throws exceptions
    object ExceptionKey extends Key[Int] {
      override def resolve[K <: Key[Int]](cells: Iterable[Cell[K, Int]]): Iterable[(Cell[K, Int], Int)] =
        throw new Exception("foo")

      override def fallback[K <: Key[Int]](cells: Iterable[Cell[K, Int]]): Iterable[(Cell[K, Int], Int)] =
        throw new Exception("bar")
    }

    implicit val pool: HandlerPool = new HandlerPool()
    val c0 = CellCompleter[ExceptionKey.type, Int](ExceptionKey)
    val c1 = CellCompleter[ExceptionKey.type, Int](ExceptionKey)
    val c2 = CellCompleter[ExceptionKey.type, Int](ExceptionKey)
    val c3 = CellCompleter[ExceptionKey.type, Int](ExceptionKey)
    val c4 = CellCompleter[ExceptionKey.type, Int](ExceptionKey)

    // Create a cSSC
    c1.cell.whenNext(c0.cell, _ => NoOutcome)
    c2.cell.whenNextSequential(c1.cell, _ => NoOutcome)
    c3.cell.whenComplete(c1.cell, _ => NoOutcome)
    c4.cell.whenCompleteSequential(c2.cell, _ => NoOutcome)
    c4.cell.when(c3.cell, (_, _) => NoOutcome)
    c0.cell.whenSequential(c4.cell, (_, _) => NoOutcome)

    // wait for the cycle to be resolved by ExceptionKey.resolve
    Await.ready(pool.quiescentResolveCell, 2.seconds)

    pool.onQuiescenceShutdown()

    // check for exceptions in all cells of the cycle
    for (c ← List(c0, c1, c2, c3, c4))
      c.cell.getResultTry() match {
        case Success(_) => assert(false)
        case Failure(e) => assert(e.getMessage == "foo")
      }
  }

  test("exception in Key.fallback") {
    // If Key.fallback is called for a cell c
    // and throws an exception e, c is completed
    // with Failure(e).

    // Define a key that throws exceptions
    object ExceptionKey extends Key[Int] {
      override def resolve[K <: Key[Int]](cells: Iterable[Cell[K, Int]]): Iterable[(Cell[K, Int], Int)] =
        throw new Exception("foo")

      override def fallback[K <: Key[Int]](cells: Iterable[Cell[K, Int]]): Iterable[(Cell[K, Int], Int)] =
        throw new Exception("bar")
    }

    implicit val pool: HandlerPool = new HandlerPool()
    val c0 = CellCompleter[ExceptionKey.type, Int](ExceptionKey)
    val c1 = CellCompleter[ExceptionKey.type, Int](ExceptionKey)

    // Create a dependency, c1 "recover" from the exception in c0 by completing with 10
    c1.cell.whenNext(c0.cell, _ => FinalOutcome(10))

    // wait for c0 to be resolved by ExceptionKey.fallback
    Await.ready(pool.quiescentResolveCell, 2.seconds)

    pool.onQuiescenceShutdown()

    // c0 should have been resolved with Failure("bar")
    c0.cell.getResultTry() match {
      case Success(_) => assert(false)
      case Failure(e) => assert(e.getMessage == "bar")
    }
    // c1 should have ignored this failure and contain 10
    assert(c1.cell.isComplete)
    assert(c1.cell.getResult() == 10)

  }

  test("exception after freeze") {
    // after a cell has been completed, an exception in one
    // of its callbacks should be ignored
    val latch = new CountDownLatch(1)
    implicit val pool: HandlerPool = new HandlerPool()
    val c0 = CellCompleter[NaturalNumberKey.type, Int](NaturalNumberKey)
    val c1 = CellCompleter[NaturalNumberKey.type, Int](NaturalNumberKey)
    val c2 = CellCompleter[NaturalNumberKey.type, Int](NaturalNumberKey)

    // Create a dependency, c1 "recover" from the exception in c0 by completing with 10
    c2.cell.whenNext(c0.cell, _ => FinalOutcome(10))
    c2.cell.whenNext(c1.cell, _ => throw new Exception("BOOM"))
    c2.cell.onComplete(_ => latch.countDown())

    // trigger completion of cell2
    c0.putFinal(0)
    // wait form completion of cell2
    latch.await(2, TimeUnit.SECONDS)
    // trigger an exception-throwing callback (this should be ignored)
    c1.putFinal(1)

    pool.onQuiescent(() => {
      // c2 should have been completed after c0.putFinal(…),
      // so the exception should not matter
      assert(c2.cell.isComplete)
      assert(c2.cell.getResult() == 10)

      pool.shutdown()
    })
  }

  test("put after completion with exception") {
    // after a cell has been completed with an exception,
    // any subsequent put should be ignored.
    val latch = new CountDownLatch(1)
    implicit val pool: HandlerPool = new HandlerPool()
    val c0 = CellCompleter[NaturalNumberKey.type, Int](NaturalNumberKey)
    val c1 = CellCompleter[NaturalNumberKey.type, Int](NaturalNumberKey)
    val c2 = CellCompleter[NaturalNumberKey.type, Int](NaturalNumberKey)

    // Create dependencies
    c2.cell.whenNext(c0.cell, _ => FinalOutcome(10))
    c2.cell.whenNext(c1.cell, _ => throw new Exception("foo"))
    c2.cell.onComplete(_ => latch.countDown())

    c1.putFinal(1)
    latch.await(2, TimeUnit.SECONDS)
    c0.putFinal(0)

    pool.onQuiescent(() => {

      // c2 should have been completed after c1.putFinal(…),
      // so the FinalOutcome(10) should be ignored
      assert(c2.cell.isComplete)
      c2.cell.getResultTry() match {
        case Success(_) => assert(false)
        case Failure(e) => assert(e.getMessage == "foo")
      }

      pool.shutdown()
    })

  }

  test("do not catch fatal exception") {
    // This test throws an error that will be printed out.
    //
    // If an instance of Error is thrown,
    // this will not be used as value for
    // the respective cell.
    val latch = new CountDownLatch(1)
    implicit val pool: HandlerPool = new HandlerPool()
    val c0 = CellCompleter[NaturalNumberKey.type, Int](NaturalNumberKey)
    val cell = pool.mkCell[NaturalNumberKey.type, Int](NaturalNumberKey, c => {
      // build up dependency, throw error, if c0's value changes
      c.whenNext(c0.cell, _ => throw new Error("It's OK, if I am not caught. See description"))
      NoOutcome
    })

    // cell should be completed as a consequence
    cell.onComplete(_ => latch.countDown())
    cell.trigger()

    assert(!cell.isComplete)

    // trigger dependency, s.t. callback is called
    // this causes the error to be thrown.
    // This should not be handled internally.
    c0.putFinal(1)

    cell.completer.putFinal(10)

    // wait for cell to be completed
    latch.await()

    assert(cell.isComplete)
    // check, if cell has been completed with an exception
    assert(cell.getResult() == 10)

    pool.shutdown()
  }

}
