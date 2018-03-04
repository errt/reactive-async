package cell

/*
 * Cells call a CallbackRunnable's execute() method when they get updated.
 * Then the CallbackRunnnable checks, if its conditions are met (for example,
 * if it has not been called before or an associated threshold is crossed).
 * In this case, it calls it submit() method that registered itself to be
 * run in the handler pool. Eventually, the handler pool will then call run()
 * on the CallbackRunnable that in turn calls the actual callback. The value passed
 * to the callback is determined via the value() method.
 *
 * The classes in this file reflect different types of (runnable) callbacks that
 * are uses by `onnext`, `oncomplete`, `whenNext`, `whenComplete`, `whenNextSequential`
 * and `whenCompleteSequential`.
 *
 * Those callbacks differ in the following parameters:
 * (1) dependecy callback (when*)  vs.   event handler callback (onnext, oncomplete)
 * (2) called at most once         vs.   called repeatedly
 * (3) called concurrently         vs.   called sequentially
 * (4) called unconditionlly       vs.   called if threshold is crossed
 * (5) called for every update     vs.   called on completion only
 *
 * The implementation of those differences is as follows:
 *
 * (1) *DepRunnable classes are subclasses of the respective *CallbackRunnable classes,
 * as they have more specialized callbacks. (A fixed return type and a predefined semantics
 * regarding the dependet cell.) *DepRunnables mix in the Dependency trait that define
 * those callbacks and make use of a completer to alter the depending cell.
 *
 * (2) The traits SingleShotRunnable and MultiShotRunnable define a run() method
 * that does/does not stop the callback from being invoked twice. One of those traits
 * need to be mixed in.
 *
 * (3) The traits ConcurrentCallbackRunnable and SequentialCallbackRunnable define submit() methods
 * that either add the Runnable to the handler pool directly or schedule it for sequential execution.
 * One of those traits need to be mixed in.
 *
 * (4) The traits ThresholdRunnable vs. UnconditionalRunnable define execute() methods that either
 * check if an associated threshold is crossed or if the new value is at least greater than bottom.
 * In the first case, the runnable stores a value that will later be passed to the callback. (Remember
 * that thresholds are a way to guarantee determinstic reads by not passing the "latest" value
 * of a cell but "only" the threshold that is crossed.)
 * ThresholdRunnables are SingleShotRunnables.
 *
 * (5) Cells handle CompleteCallbackRunnables and NextCallbackRunnables differently in that
 * they only call CompleteCallRunnables on completion. The traits here are mostly for
 * type safety and documentation purpose. Plus: CompleteCallbackRunnables mix in SingleShotRunnable.
 *
 */

import java.util.concurrent.atomic.AtomicBoolean

import lattice.Key

import scala.concurrent.OnCompleteRunnable
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

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
   * Inform this callback that `otherCell` has been an updated to value v.
   *
   * @return true iff the callback is still valid after it has been executed.
   */
  def execute(): Boolean

  /** Returns the value that is passed to the callback. */
  protected def value(): V

  /** Add this CallbackRunnable to its handler pool. */
  protected def submit(): Unit

  /**
    * Essentially, call the callback. Do not call this method directly. It is inteded
    * to be called by the handler pool.
    */
  override def run(): Unit
}

/*
 * (2) called at most once vs. called repeatedly
 */

private[cell] trait MultiShotRunnalbe[K <: Key[V], V] extends CallbackRunnable[K, V] {
  def run(): Unit = {
    callback(Success(value()))
  }
}

private[cell] trait SingleShotRunnable[K <: Key[V], V] extends CallbackRunnable[K, V] {
  val started = new AtomicBoolean(false)

  def run(): Unit = {
    if (started.compareAndSet(false, true))
      callback(Success(value()))
  }
}

/*
 * (4) called unconditionlly vs. called if threshold is crossed
 */
private trait Threshold[K <: Key[V], V] extends SingleShotRunnable[K, V] {
  val thresholds: Set[V]
  val ordering: PartialOrdering[V]

  var theValue: Option[V] = _

  def value(): V = theValue.get

  def execute(): Boolean = {
    val crossed = thresholds.find(ordering.lteq(_, otherCell.getResult()))
    crossed.foreach(t => {
      theValue = Some(t)
      submit()
    })
    // if one of the thresholds has been crossed,
    // the callback should not be called again,
    // i.e. return false
    crossed.isEmpty
  }
}

private[cell] trait UnconditionalRunnable[K <: Key[V], V] extends CallbackRunnable[K, V] {
  def value(): V = otherCell.getResult()

  def execute(): Boolean = {
    submit()
    true // If SingleShot, this could return false. But this seems not to be used.
  }
}

/*
 * (3) called concurrently vs. called sequentially
 */

/**
 * Run a callback concurrently, if a value in a cell changes.
 * Call execute() to add the callback to the given HandlerPool.
 */
private[cell] trait ConcurrentCallbackRunnable[K <: Key[V], V] extends CallbackRunnable[K, V] {
  /** Add this CallbackRunnable to its handler pool such that it is run concurrently. */
  override def submit(): Unit = {
    try pool.execute(this)
    catch {
      case NonFatal(t) => pool reportFailure t
    }
  }
}

/**
 * Run a callback sequentially (for a dependent cell), if a value in another cell changes.
 * Call execute() to add the callback to the given HandlerPool.
 */
private[cell] trait SequentialCallbackRunnable[K <: Key[V], V] extends CallbackRunnable[K, V] {
  /**
    * Add this CallbackRunnable to its handler pool such that it is run sequentially.
    * All SequentialCallbackRunnables with the same `dependentCell` are executed sequentially.
    */
  override def submit(): Unit = {
    pool.scheduleSequentialCallback(this)
  }
}

/*
 * (1) dependecy callback (when*) vs. event handler callback (onnext, oncomplete)
 */

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

private[cell] class CompleteConcurrentCallbackRunnable[K <: Key[V], V](
    override val pool: HandlerPool,
    override val dependentCell: Cell[K, V],
    override val otherCell: Cell[K, V],
    override val callback: Try[V] => Any)
  extends CompleteCallbackRunnable[K, V](pool, dependentCell, otherCell, callback) with ConcurrentCallbackRunnable[K, V] with UnconditionalRunnable[K, V]

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
  with ConcurrentCallbackRunnable[K, V] with UnconditionalRunnable[K, V]  {
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
  with SequentialCallbackRunnable[K, V] with UnconditionalRunnable[K, V] {
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
  extends NextCallbackRunnable[K, V](pool, dependentCell, otherCell, callback)
    with UnconditionalRunnable[K, V]
    with MultiShotRunnalbe[K, V]
    with ConcurrentCallbackRunnable[K, V] {
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
    case Success(x) =>
      valueCallback(x) match {
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
  override val valueCallback: V => Outcome[V])
  extends NextDepRunnable[K, V](pool, dependentCompleter, otherCell, valueCallback)
  with ConcurrentCallbackRunnable[K, V]
  with UnconditionalRunnable[K, V]
  with MultiShotRunnalbe[K, V] {
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
  override val valueCallback: V => Outcome[V])
  extends NextDepRunnable[K, V](pool, dependentCompleter, otherCell, valueCallback)
    with SequentialCallbackRunnable[K, V]
    with UnconditionalRunnable[K, V]
    with MultiShotRunnalbe[K, V] {
}

private[cell] class ThresholdNextConcurrentDepRunnalbe[K <: Key[V], V](
  override val pool: HandlerPool,
  override val dependentCompleter: CellCompleter[K, V],
  override val otherCell: Cell[K, V],
  override val thresholds: Set[V],
  override val valueCallback: V => Outcome[V])(implicit override val ordering: PartialOrdering[V])
  extends NextDepRunnable[K, V](pool, dependentCompleter, otherCell, valueCallback)
  with ConcurrentCallbackRunnable[K, V]
  with Threshold[K, V]

private[cell] class ThresholdNextSequentialDepRunnalbe[K <: Key[V], V](
  override val pool: HandlerPool,
  override val dependentCompleter: CellCompleter[K, V],
  override val otherCell: Cell[K, V],
  override val thresholds: Set[V],
  override val valueCallback: V => Outcome[V])(implicit override val ordering: PartialOrdering[V])
  extends NextDepRunnable[K, V](pool, dependentCompleter, otherCell, valueCallback)
    with SequentialCallbackRunnable[K, V]
  with Threshold[K, V]
