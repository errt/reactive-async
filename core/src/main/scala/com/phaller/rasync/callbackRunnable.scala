package com.phaller.rasync

import lattice.Key

import scala.concurrent.OnCompleteRunnable
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }

/**
 * Run a callback in a handler pool, if a value in a cell changes.
 * Call execute() to add the callback to the given HandlerPool.
 */
private[rasync] trait CallbackRunnable[K <: Key[V], V] extends Runnable with OnCompleteRunnable {
  /** The handler pool that runs the callback function. */
  val pool: HandlerPool

  /** The cell that awaits this callback. */
  val dependentCell: Cell[K, V]

  /** The cell that triggers the callback. */
  val otherCell: Cell[K, V]

  /** The callback to run. */
  val callback: Any

  val sequential: Boolean

  /** Add this CallbackRunnable to its handler pool. */
  def execute(): Unit =
    try pool.execute(this)
    catch { case NonFatal(t) => pool reportFailure t }

  /** Essentially, call the callback. */
  override def run(): Unit
}

/**
 * Run a callback concurrently, if a value in a cell changes.
 * Call execute() to add the callback to the given HandlerPool.
 */
private[rasync] trait ConcurrentCallbackRunnable[K <: Key[V], V] extends CallbackRunnable[K, V] {
  override val sequential: Boolean = false
}

/**
 * Run a callback sequentially (for a dependent cell), if a value in another cell changes.
 * Call execute() to add the callback to the given HandlerPool.
 */
private[rasync] trait SequentialCallbackRunnable[K <: Key[V], V] extends CallbackRunnable[K, V] {
  override val sequential: Boolean = true
  val dependentCell: Cell[K, V]
}

/**
 * A dependency between to cells consisting of a dependent cell(completer),
 * an other cell and the callback to calculate new values for the dependent cell.
 */
private[rasync] trait Dependency[K <: Key[V], V] {
  val dependentCompleter: CellCompleter[K, V]
  val otherCell: Cell[K, V]
  val valueCallback: Any
}

/**
 * A dependency between to cells consisting of a dependent cell(completer),
 * an other cell and the callback to calculate new values for the dependent cell.
 */
private[rasync] trait SingleDependency[K <: Key[V], V] extends Dependency[K, V] {
  override val valueCallback: V => Outcome[V]
}

/**
 * A dependency between to cells consisting of a dependent cell(completer),
 * an other cell and the callback to calculate new values for the dependent cell.
 */
private[rasync] trait CombinedDependency[K <: Key[V], V] extends Dependency[K, V] {
  override val valueCallback: (V, Boolean) => Outcome[V]
}

/**
 * To be run when `otherCell` gets its final update.
 * @param pool          The handler pool that runs the callback function
 * @param dependentCell The cell, that depends on `otherCell`.
 * @param otherCell     Cell that triggers this callback.
 * @param callback      Callback function that is triggered on an onNext event
 */
private[rasync] abstract class CompleteCallbackRunnable[K <: Key[V], V](
  override val pool: HandlerPool,
  override val dependentCell: Cell[K, V], // needed to not call whenNext callback, if whenComplete callback exists.
  override val otherCell: Cell[K, V],
  val callback: Try[V] => Any)
  extends CallbackRunnable[K, V] {

  // must be filled in before running it
  var started: Boolean = false

  def run(): Unit = {
    require(!started) // can't complete it twice
    started = true
    if (sequential) {
      dependentCell.synchronized {
        callback(Success(otherCell.getResult()))
      }
    } else {
      callback(Success(otherCell.getResult()))
    }
  }
}

private[rasync] class CompleteConcurrentCallbackRunnable[K <: Key[V], V](override val pool: HandlerPool, override val dependentCell: Cell[K, V], override val otherCell: Cell[K, V], override val callback: Try[V] => Any)
  extends CompleteCallbackRunnable[K, V](pool, dependentCell, otherCell, callback) with ConcurrentCallbackRunnable[K, V]

/**
 * Dependency between `dependentCompleter` and `otherCell`.
 *
 * @param pool               The handler pool that runs the callback function
 * @param dependentCompleter The (completer) of the cell, that depends on `otherCell`.
 * @param otherCell          The cell that `dependentCompleter` depends on.
 * @param valueCallback      Called to retrieve the new value for the dependent cell.
 */
private[rasync] abstract class CompleteDepRunnable[K <: Key[V], V](
  override val pool: HandlerPool,
  override val dependentCompleter: CellCompleter[K, V],
  override val otherCell: Cell[K, V],
  override val valueCallback: V => Outcome[V]) extends CompleteCallbackRunnable[K, V](pool, dependentCompleter.cell, otherCell, {
  case Success(x) =>
    valueCallback(x) match {
      case FinalOutcome(v) =>
        dependentCompleter.putFinal(v) // deps will be removed by putFinal()
      case NextOutcome(v) =>
        dependentCompleter.putNext(v)
        dependentCompleter.removeDep(otherCell)
        dependentCompleter.removeNextDep(otherCell)
        dependentCompleter.removeCombinedDep(otherCell)
      case NoOutcome =>
        dependentCompleter.removeDep(otherCell)
        dependentCompleter.removeNextDep(otherCell)
        dependentCompleter.removeCombinedDep(otherCell)
    }
  case Failure(_) =>
    dependentCompleter.removeDep(otherCell)
    dependentCompleter.removeNextDep(otherCell)
    dependentCompleter.removeCombinedDep(otherCell)
}) with SingleDependency[K, V]

/**
 * Dependency between `dependentCompleter` and `otherCell`.
 *
 * @param pool               The handler pool that runs the callback function
 * @param dependentCompleter The (completer) of the cell, that depends on `otherCell`.
 * @param otherCell          The cell that `dependentCompleter` depends on.
 * @param valueCallback   Called to retrieve the new value for the dependent cell.
 */
private[rasync] class CompleteConcurrentDepRunnable[K <: Key[V], V](
  override val pool: HandlerPool,
  override val dependentCompleter: CellCompleter[K, V],
  override val otherCell: Cell[K, V],
  override val valueCallback: V => Outcome[V]) extends CompleteDepRunnable[K, V](pool, dependentCompleter, otherCell, valueCallback) with ConcurrentCallbackRunnable[K, V] {
}

/**
 * Dependency between `dependentCompleter` and `otherCell`.
 *
 * @param pool               The handler pool that runs the callback function
 * @param dependentCompleter The (completer) of the cell, that depends on `otherCell`.
 * @param otherCell          The cell that `dependentCompleter` depends on.
 * @param valueCallback   Called to retrieve the new value for the dependent cell.
 */
private[rasync] class CompleteSequentialDepRunnable[K <: Key[V], V](
  override val pool: HandlerPool,
  override val dependentCompleter: CellCompleter[K, V],
  override val otherCell: Cell[K, V],
  override val valueCallback: V => Outcome[V]) extends CompleteDepRunnable[K, V](pool, dependentCompleter, otherCell, valueCallback) with SequentialCallbackRunnable[K, V] {
}

/**
 * To be run when `otherCell` gets a final update.
 * @param pool          The handler pool that runs the callback function
 * @param dependentCell The cell, that depends on `otherCell`.
 * @param otherCell     Cell that triggers this callback.
 * @param callback      Callback function that is triggered on an onNext event
 */
private[rasync] abstract class NextCallbackRunnable[K <: Key[V], V](
  override val pool: HandlerPool,
  override val dependentCell: Cell[K, V], // needed to not call whenNext callback, if whenComplete callback exists.
  override val otherCell: Cell[K, V],
  val callback: Try[V] => Any)
  extends CallbackRunnable[K, V] {

  def run(): Unit = {
    if (sequential) {
      dependentCell.synchronized {
        callback(Success(otherCell.getResult()))
      }
    } else {
      callback(Success(otherCell.getResult()))
    }
  }
}

/**
 * @param pool          The handler pool that runs the callback function
 * @param dependentCell The cell, that depends on `otherCell`.
 * @param otherCell     Cell that triggers this callback.
 * @param callback      Callback function that is triggered on an onNext event
 */
private[rasync] class NextConcurrentCallbackRunnable[K <: Key[V], V](
  override val pool: HandlerPool,
  override val dependentCell: Cell[K, V],
  override val otherCell: Cell[K, V],
  override val callback: Try[V] => Any)
  extends NextCallbackRunnable[K, V](pool, dependentCell, otherCell, callback) with ConcurrentCallbackRunnable[K, V]

/**
 * Dependency between `dependentCompleter` and `otherCell`.
 *
 * @param pool               The handler pool that runs the callback function
 * @param dependentCompleter The (completer) of the cell, that depends on `otherCell`.
 * @param otherCell          The cell that `dependentCompleter` depends on.
 * @param valueCallback   Called to retrieve the new value for the dependent cell.
 */
private[rasync] abstract class NextDepRunnable[K <: Key[V], V](
  override val pool: HandlerPool,
  override val dependentCompleter: CellCompleter[K, V],
  override val otherCell: Cell[K, V],
  override val valueCallback: V => Outcome[V]) extends NextCallbackRunnable[K, V](pool, dependentCompleter.cell, otherCell, t => {
  t match {
    case Success(_) =>
      valueCallback(otherCell.getResult()) match {
        case NextOutcome(v) =>
          dependentCompleter.putNext(v)
        case FinalOutcome(v) =>
          dependentCompleter.putFinal(v)
        case _ => /* do nothing */
      }
    case Failure(_) => /* do nothing */
  }

  // Remove the dependency, if `otherCell` is complete.
  // There is no need to removeCompleteDeps, because if those existed,
  // a CompleteDepRunnable would have been called and removed the dep
  if (otherCell.isComplete) dependentCompleter.removeNextDep(otherCell)
}) with SingleDependency[K, V]

/**
 * Dependency between `dependentCompleter` and `otherCell`.
 *
 * @param pool               The handler pool that runs the callback function
 * @param dependentCompleter The (completer) of the cell, that depends on `otherCell`.
 * @param otherCell          The cell that `dependentCompleter` depends on.
 * @param valueCallback   Called to retrieve the new value for the dependent cell.
 */
private[rasync] class NextConcurrentDepRunnable[K <: Key[V], V](
  override val pool: HandlerPool,
  override val dependentCompleter: CellCompleter[K, V],
  override val otherCell: Cell[K, V],
  override val valueCallback: V => Outcome[V]) extends NextDepRunnable[K, V](pool, dependentCompleter, otherCell, valueCallback) with ConcurrentCallbackRunnable[K, V]

/**
 * Dependency between `dependentCompleter` and `otherCell`.
 *
 * @param pool               The handler pool that runs the callback function
 * @param dependentCompleter The (completer) of the cell, that depends on `otherCell`.
 * @param otherCell          The cell that `dependentCompleter` depends on.
 * @param valueCallback   Called to retrieve the new value for the dependent cell.
 */
private[rasync] class NextSequentialDepRunnable[K <: Key[V], V](
  override val pool: HandlerPool,
  override val dependentCompleter: CellCompleter[K, V],
  override val otherCell: Cell[K, V],
  override val valueCallback: V => Outcome[V]) extends NextDepRunnable[K, V](pool, dependentCompleter, otherCell, valueCallback) with SequentialCallbackRunnable[K, V]

/**
 * To be run when `otherCell` gets a final update.
 * @param pool          The handler pool that runs the callback function
 * @param dependentCell The cell, that depends on `otherCell`.
 * @param otherCell     Cell that triggers this callback.
 * @param callback      Callback function that is triggered on an onNext event
 */
private[rasync] abstract class CombinedCallbackRunnable[K <: Key[V], V](
  override val pool: HandlerPool,
  override val dependentCell: Cell[K, V], // needed to not call whenNext callback, if whenComplete callback exists.
  override val otherCell: Cell[K, V],
  val callback: (Try[V], Boolean) => Any)
  extends CallbackRunnable[K, V] {

  def run(): Unit = {
    // It is important to call isComplete first
    // otherwise, isComplete could return true, while
    // `value` is outdated.
    val isComplete = otherCell.isComplete
    val value = otherCell.getResult()

    if (sequential) {
      dependentCell.synchronized {
        callback(Success(value), isComplete)
      }
    } else {
      callback(Success(value), isComplete)
    }
  }
}

/**
 * @param pool          The handler pool that runs the callback function
 * @param dependentCell The cell, that depends on `otherCell`.
 * @param otherCell     Cell that triggers this callback.
 * @param callback      Callback function that is triggered on an onNext event
 */
private[rasync] class CombinedConcurrentCallbackRunnable[K <: Key[V], V](
  override val pool: HandlerPool,
  override val dependentCell: Cell[K, V],
  override val otherCell: Cell[K, V],
  override val callback: (Try[V], Boolean) => Any)
  extends CombinedCallbackRunnable[K, V](pool, dependentCell, otherCell, callback) with ConcurrentCallbackRunnable[K, V] {
}

/**
 * Dependency between `dependentCompleter` and `otherCell`.
 *
 * @param pool               The handler pool that runs the callback function
 * @param dependentCompleter The (completer) of the cell, that depends on `otherCell`.
 * @param otherCell          The cell that `dependentCompleter` depends on.
 * @param valueCallback   Called to retrieve the new value for the dependent cell.
 */
private[rasync] abstract class CombinedDepRunnable[K <: Key[V], V](
  override val pool: HandlerPool,
  override val dependentCompleter: CellCompleter[K, V],
  override val otherCell: Cell[K, V],
  override val valueCallback: (V, Boolean) => Outcome[V]) extends CombinedCallbackRunnable[K, V](pool, dependentCompleter.cell, otherCell, (t, isFinal) => {
  if (isFinal) t match {
    case Success(x) =>
      valueCallback(x, true) match {
        case FinalOutcome(v) =>
          dependentCompleter.putFinal(v) // deps will be removed by putFinal()
        case NextOutcome(v) =>
          dependentCompleter.putNext(v)
          dependentCompleter.removeDep(otherCell)
          dependentCompleter.removeNextDep(otherCell)
          dependentCompleter.removeCombinedDep(otherCell)
        case NoOutcome =>
          dependentCompleter.removeDep(otherCell)
          dependentCompleter.removeNextDep(otherCell)
          dependentCompleter.removeCombinedDep(otherCell)
      }
    case Failure(_) =>
      dependentCompleter.removeDep(otherCell)
      dependentCompleter.removeNextDep(otherCell)
      dependentCompleter.removeCombinedDep(otherCell)
  }
  else {
    // Copied from NextDepRunnable
    t match {
      case Success(_) =>
        valueCallback(otherCell.getResult(), false) match {
          case NextOutcome(v) =>
            dependentCompleter.putNext(v)
          case FinalOutcome(v) =>
            dependentCompleter.putFinal(v)
          case _ => /* do nothing */
        }
      case Failure(_) => /* do nothing */
    }

    // Remove the dependency, if `otherCell` is complete.
    // There is no need to removeCompleteDeps, because if those existed,
    // a CompleteDepRunnable would have been called and removed the dep
    if (otherCell.isComplete) dependentCompleter.removeNextDep(otherCell)
  }
}) with CombinedDependency[K, V]

/**
 * Dependency between `dependentCompleter` and `otherCell`.
 *
 * @param pool               The handler pool that runs the callback function
 * @param dependentCompleter The (completer) of the cell, that depends on `otherCell`.
 * @param otherCell          The cell that `dependentCompleter` depends on.
 * @param valueCallback   Called to retrieve the new value for the dependent cell.
 */
private[rasync] class CombinedConcurrentDepRunnable[K <: Key[V], V](
  override val pool: HandlerPool,
  override val dependentCompleter: CellCompleter[K, V],
  override val otherCell: Cell[K, V],
  override val valueCallback: (V, Boolean) => Outcome[V]) extends CombinedDepRunnable[K, V](pool, dependentCompleter, otherCell, valueCallback) with ConcurrentCallbackRunnable[K, V] {
}

/**
 * Dependency between `dependentCompleter` and `otherCell`.
 *
 * @param pool               The handler pool that runs the callback function
 * @param dependentCompleter The (completer) of the cell, that depends on `otherCell`.
 * @param otherCell          The cell that `dependentCompleter` depends on.
 * @param valueCallback   Called to retrieve the new value for the dependent cell.
 */
private[rasync] class CombinedSequentialDepRunnable[K <: Key[V], V](
  override val pool: HandlerPool,
  override val dependentCompleter: CellCompleter[K, V],
  override val otherCell: Cell[K, V],
  override val valueCallback: (V, Boolean) => Outcome[V]) extends CombinedDepRunnable[K, V](pool, dependentCompleter, otherCell, valueCallback) with SequentialCallbackRunnable[K, V] {
}

