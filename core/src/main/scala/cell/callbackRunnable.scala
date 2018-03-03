package cell

import java.util.concurrent.atomic.AtomicBoolean

import lattice.{ Key, Lattice }

import scala.concurrent.OnCompleteRunnable
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }

/**
 * Run a callback in a handler pool, if a value in a cell changes.
 * Call execute() to add the callback to the given HandlerPool.
 */
private[cell] trait CallbackRunnable[K <: Key[V], V] extends Runnable with OnCompleteRunnable {
  /** The handler pool that runs the callback function. */
  val pool: HandlerPool

  /** The cell that awaits this callback. */
  val dependentCell: Cell[K, V]

  /** The cell that triggers the callback. */
  val otherCell: Cell[K, V]

  val callback: Try[V] => Any

  /**
   * Add this CallbackRunnable to its handler pool.
   *
   * @return true iff the callback is still valid after it has been executed.
   */
  def execute(): Boolean

  /** Essentially, call the callback. */
  override def run(): Unit
}

private[cell] trait MultiShotRunnalbe[K <: Key[V], V] extends CallbackRunnable[K, V] {
  def run(): Unit = {
    callback(Success(otherCell.getResult()))
  }
}

private[cell] trait SingleShotRunnable[K <: Key[V], V] extends CallbackRunnable[K, V] {
  val started = new AtomicBoolean(false)

  def run(): Unit = {
    if (started.compareAndSet(false, true))
      callback(Success(otherCell.getResult()))
  }
}

private[cell] trait Threshold[K <: Key[V], V] extends SingleShotRunnable[K, V] {
  val threshold: V
  val lattice: Lattice[V]
}

private[cell] trait UnconditionalRunnable[K <: Key[V], V] extends CallbackRunnable[K, V]

/**
 * Run a callback concurrently, if a value in a cell changes.
 * Call execute() to add the callback to the given HandlerPool.
 */
private[cell] trait ConcurrentCallbackRunnable[K <: Key[V], V] extends CallbackRunnable[K, V]

/**
 * Run a callback concurrently, if a value in a cell changes.
 * Call execute() to add the callback to the given HandlerPool.
 */
private[cell] trait ConcurrentUnconditionalCallbackRunnable[K <: Key[V], V] extends ConcurrentCallbackRunnable[K, V] with UnconditionalRunnable[K, V] {
  /** Add this CallbackRunnable to its handler pool such that it is run concurrently. */
  def execute(): Boolean = {
    try pool.execute(this)
    catch {
      case NonFatal(t) => pool reportFailure t
    }
    true
  }
}

/**
 * Run a callback concurrently, if a value in a cell changes.
 * Call execute() to add the callback to the given HandlerPool.
 */
private[cell] trait ConcurrentThresholdCallbackRunnable[K <: Key[V], V] extends ConcurrentCallbackRunnable[K, V] with Threshold[K, V] {
  /** Add this CallbackRunnable to its handler pool such that it is run concurrently. */
  def execute(): Boolean = {
    if (lattice.gteq(otherCell.getResult(), threshold)) {
      try pool.execute(this)
      catch {
        case NonFatal(t) => pool reportFailure t
      }
      true
    } else false
  }
}

/**
 * Run a callback sequentially (for a dependent cell), if a value in another cell changes.
 * Call execute() to add the callback to the given HandlerPool.
 */
private[cell] trait SequentialCallbackRunnable[K <: Key[V], V] extends CallbackRunnable[K, V]

/**
 * Run a callback sequentially (for a dependent cell), if a value in another cell changes.
 * Call execute() to add the callback to the given HandlerPool.
 */
private[cell] trait SequentialUnconditionalCallbackRunnable[K <: Key[V], V] extends SequentialCallbackRunnable[K, V] with UnconditionalRunnable[K, V] {

  /**
   * Add this CallbackRunnable to its handler pool such that it is run sequentially.
   * All SequentialCallbackRunnables with the same `dependentCell` are executed sequentially.
   */
  def execute(): Boolean = {
    pool.scheduleSequentialCallback(this)
    true
  }
}

private[cell] trait SequentialThresholdCallbackRunnable[K <: Key[V], V] extends SequentialCallbackRunnable[K, V] with Threshold[K, V] {

  /**
   * Add this CallbackRunnable to its handler pool such that it is run sequentially.
   * All SequentialCallbackRunnables with the same `dependentCell` are executed sequentially.
   */
  def execute(): Boolean = {
    if (lattice.gteq(otherCell.getResult(), threshold)) {
      pool.scheduleSequentialCallback(this)
      true
    } else false
  }
}

/**
 * A dependency between to cells consisting of a dependent cell(completer),
 * an other cell and the callback to calculate new values for the dependent cell.
 */
private[cell] trait Dependency[K <: Key[V], V] {
  val dependentCompleter: CellCompleter[K, V]
  val otherCell: Cell[K, V]
  val valueCallback: V => Outcome[V]
}

/**
 * To be run when `otherCell` gets its final update.
 *
 * @param pool          The handler pool that runs the callback function
 * @param dependentCell The cell, that depends on `otherCell`.
 * @param otherCell     Cell that triggers this callback.
 * @param callback      Callback function that is triggered on an onNext event
 */
private[cell] abstract class CompleteCallbackRunnable[K <: Key[V], V](
  override val pool: HandlerPool,
  override val dependentCell: Cell[K, V], // needed to not call whenNext callback, if whenComplete callback exists.
  override val otherCell: Cell[K, V],
  val callback: Try[V] => Any)
  extends SingleShotRunnable[K, V] {}

private[cell] class CompleteConcurrentCallbackRunnable[K <: Key[V], V](override val pool: HandlerPool, override val dependentCell: Cell[K, V], override val otherCell: Cell[K, V], override val callback: Try[V] => Any)
  extends CompleteCallbackRunnable[K, V](pool, dependentCell, otherCell, callback) with ConcurrentUnconditionalCallbackRunnable[K, V] {
}

/**
 * Dependency between `dependentCompleter` and `otherCell`.
 *
 * @param pool               The handler pool that runs the callback function
 * @param dependentCompleter The (completer) of the cell, that depends on `otherCell`.
 * @param otherCell          The cell that `dependentCompleter` depends on.
 * @param valueCallback      Called to retrieve the new value for the dependent cell.
 */
private[cell] abstract class CompleteDepRunnable[K <: Key[V], V](
  override val pool: HandlerPool,
  override val dependentCompleter: CellCompleter[K, V],
  override val otherCell: Cell[K, V],
  override val valueCallback: V => Outcome[V])
  extends CompleteCallbackRunnable[K, V](pool, dependentCompleter.cell, otherCell, {
    case Success(x) =>
      valueCallback(x) match {
        case FinalOutcome(v) =>
          dependentCompleter.putFinal(v) // deps will be removed by putFinal()
        case NextOutcome(v) =>
          dependentCompleter.putNext(v)
          dependentCompleter.removeDep(otherCell)
          dependentCompleter.removeNextDep(otherCell)
        case NoOutcome =>
          dependentCompleter.removeDep(otherCell)
          dependentCompleter.removeNextDep(otherCell)
      }
    case Failure(_) =>
      dependentCompleter.removeDep(otherCell)
      dependentCompleter.removeNextDep(otherCell)
  }) with Dependency[K, V]

/**
 * Dependency between `dependentCompleter` and `otherCell`.
 *
 * @param pool               The handler pool that runs the callback function
 * @param dependentCompleter The (completer) of the cell, that depends on `otherCell`.
 * @param otherCell          The cell that `dependentCompleter` depends on.
 * @param valueCallback      Called to retrieve the new value for the dependent cell.
 */
private[cell] class CompleteConcurrentDepRunnable[K <: Key[V], V](
  override val pool: HandlerPool,
  override val dependentCompleter: CellCompleter[K, V],
  override val otherCell: Cell[K, V],
  override val valueCallback: V => Outcome[V])
  extends CompleteDepRunnable[K, V](pool, dependentCompleter, otherCell, valueCallback)
  with ConcurrentUnconditionalCallbackRunnable[K, V] {
}

/**
 * Dependency between `dependentCompleter` and `otherCell`.
 *
 * @param pool               The handler pool that runs the callback function
 * @param dependentCompleter The (completer) of the cell, that depends on `otherCell`.
 * @param otherCell          The cell that `dependentCompleter` depends on.
 * @param valueCallback      Called to retrieve the new value for the dependent cell.
 */
private[cell] class CompleteSequentialDepRunnable[K <: Key[V], V](
  override val pool: HandlerPool,
  override val dependentCompleter: CellCompleter[K, V],
  override val otherCell: Cell[K, V],
  override val valueCallback: V => Outcome[V])
  extends CompleteDepRunnable[K, V](pool, dependentCompleter, otherCell, valueCallback)
  with SequentialUnconditionalCallbackRunnable[K, V] {
}

/**
 * To be run when `otherCell` gets a final update.
 *
 * @param pool          The handler pool that runs the callback function
 * @param dependentCell The cell, that depends on `otherCell`.
 * @param otherCell     Cell that triggers this callback.
 * @param callback      Callback function that is triggered on an onNext event
 */
private[cell] abstract class NextCallbackRunnable[K <: Key[V], V](
  override val pool: HandlerPool,
  override val dependentCell: Cell[K, V], // needed to not call whenNext callback, if whenComplete callback exists.
  override val otherCell: Cell[K, V],
  val callback: Try[V] => Any)
  extends CallbackRunnable[K, V] {
}

private[cell] abstract class UnconditionalNextCallbackRunnable[K <: Key[V], V](
  override val pool: HandlerPool,
  override val dependentCell: Cell[K, V], // needed to not call whenNext callback, if whenComplete callback exists.
  override val otherCell: Cell[K, V],
  override val callback: Try[V] => Any)
  extends NextCallbackRunnable(pool, dependentCell, otherCell, callback) with MultiShotRunnalbe[K, V] with UnconditionalRunnable[K, V] {
}

/**
 * @param pool          The handler pool that runs the callback function
 * @param dependentCell The cell, that depends on `otherCell`.
 * @param otherCell     Cell that triggers this callback.
 * @param callback      Callback function that is triggered on an onNext event
 */
private[cell] class NextConcurrentUnconditionalCallbackRunnable[K <: Key[V], V](
  override val pool: HandlerPool,
  override val dependentCell: Cell[K, V],
  override val otherCell: Cell[K, V],
  override val callback: Try[V] => Any)
  extends UnconditionalNextCallbackRunnable[K, V](pool, dependentCell, otherCell, callback) with ConcurrentUnconditionalCallbackRunnable[K, V] {
}

/**
 * Dependency between `dependentCompleter` and `otherCell`.
 *
 * @param pool               The handler pool that runs the callback function
 * @param dependentCompleter The (completer) of the cell, that depends on `otherCell`.
 * @param otherCell          The cell that `dependentCompleter` depends on.
 * @param valueCallback      Called to retrieve the new value for the dependent cell.
 */
private[cell] abstract class NextDepRunnable[K <: Key[V], V](
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
}) with Dependency[K, V]

/**
 * Dependency between `dependentCompleter` and `otherCell`.
 *
 * @param pool               The handler pool that runs the callback function
 * @param dependentCompleter The (completer) of the cell, that depends on `otherCell`.
 * @param otherCell          The cell that `dependentCompleter` depends on.
 * @param valueCallback      Called to retrieve the new value for the dependent cell.
 */
private[cell] class UnconditionalNextConcurrentDepRunnable[K <: Key[V], V](
  override val pool: HandlerPool,
  override val dependentCompleter: CellCompleter[K, V],
  override val otherCell: Cell[K, V],
  override val valueCallback: V => Outcome[V]) extends NextDepRunnable[K, V](pool, dependentCompleter, otherCell, valueCallback) with ConcurrentUnconditionalCallbackRunnable[K, V] with MultiShotRunnalbe[K, V] {
}

/**
 * Dependency between `dependentCompleter` and `otherCell`.
 *
 * @param pool               The handler pool that runs the callback function
 * @param dependentCompleter The (completer) of the cell, that depends on `otherCell`.
 * @param otherCell          The cell that `dependentCompleter` depends on.
 * @param valueCallback      Called to retrieve the new value for the dependent cell.
 */
private[cell] class UnconditionalNextSequentialDepRunnable[K <: Key[V], V](
  override val pool: HandlerPool,
  override val dependentCompleter: CellCompleter[K, V],
  override val otherCell: Cell[K, V],
  override val valueCallback: V => Outcome[V]) extends NextDepRunnable[K, V](pool, dependentCompleter, otherCell, valueCallback) with SequentialUnconditionalCallbackRunnable[K, V] with MultiShotRunnalbe[K, V] {
}

private[cell] class ThresholdNextConcurrentDepRunnalbe[K <: Key[V], V](
  override val pool: HandlerPool,
  override val dependentCompleter: CellCompleter[K, V],
  override val otherCell: Cell[K, V],
  override val threshold: V,
  override val valueCallback: V => Outcome[V])(implicit override val lattice: Lattice[V])
  extends NextDepRunnable[K, V](pool, dependentCompleter, otherCell, valueCallback) with ConcurrentThresholdCallbackRunnable[K, V]

private[cell] class ThresholdNextSequentialDepRunnalbe[K <: Key[V], V](
  override val pool: HandlerPool,
  override val dependentCompleter: CellCompleter[K, V],
  override val otherCell: Cell[K, V],
  override val threshold: V,
  override val valueCallback: V => Outcome[V])(implicit override val lattice: Lattice[V])
  extends NextDepRunnable[K, V](pool, dependentCompleter, otherCell, valueCallback) with SequentialThresholdCallbackRunnable[K, V]
