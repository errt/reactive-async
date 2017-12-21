package cell

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{ CountDownLatch, ExecutionException }

import scala.annotation.tailrec

import scala.concurrent.OnCompleteRunnable
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }

import lattice.{ Lattice, LatticeViolationException, Key, DefaultKey }

trait Cell[K <: Key[V], V] {

  def key: K

  /**
   * Returns the current value of `this` `Cell`.
   *
   * Note that this method may return non-deterministic values. To ensure
   * deterministic executions use the quiescence API of class `HandlerPool`.
   */
  def getResult(): V

  def isComplete: Boolean

  /**
   * Adds a dependency on some `other` cell.
   *
   * Example:
   * {{{
   *   whenComplete(cell, {                   // when `cell` is completed
   *     case Impure => FinalOutcome(Impure)  // if the final value of `cell` is `Impure`, `this` cell is completed with value `Impure`
   *     case _ => NoOutcome
   *   })
   * }}}
   *
   * @param other  Cell that `this` Cell depends on.
   * @param valueCallback  Callback that retrieves the final value of `other` and returns an Outcome for `this` cell.
   */
  def whenComplete(other: Cell[K, V], valueCallback: V => Outcome[V]): Unit

  /**
   * Adds a dependency on some `other` cell.
   *
   * Example:
   * {{{
   *   whenNext(cell, {                       // when the next value is put into `cell`
   *     case Impure => FinalOutcome(Impure)  // if the next value of `cell` is `Impure`, `this` cell is completed with value `Impure`
   *     case _ => NoOutcome
   *   })
   * }}}
   *
   * @param other  Cell that `this` Cell depends on.
   * @param valueCallback  Callback that receives the new value of `other` and returns an `Outcome` for `this` cell.
   */
  def whenNext(other: Cell[K, V], valueCallback: V => Outcome[V]): Unit

  //  def zipFinal(that: Cell[K, V]): Cell[DefaultKey[(V, V)], (V, V)]

  // internal API

  // Schedules execution of `callback` when next intermediate result is available.
  private[cell] def onNext[U](callback: Try[V] => U): Unit //(implicit context: ExecutionContext): Unit

  // Schedules execution of `callback` when completed with final result.
  private[cell] def onComplete[U](callback: Try[V] => U): Unit

  // Only used in tests.
  private[cell] def waitUntilNoDeps(): Unit

  // Only used in tests.
  private[cell] def waitUntilNoNextDeps(): Unit

  private[cell] def isRunning: Boolean
  private[cell] def markAsRunning(): Boolean

  private[cell] def numTotalDependencies: Int
  private[cell] def numNextDependencies: Int
  private[cell] def numCompleteDependencies: Int

  private[cell] def numNextCallbacks: Int
  private[cell] def numCompleteCallbacks: Int

  private[cell] def addCallback[U](callback: Try[V] => U, cell: Cell[K, V]): Unit
  private[cell] def addNextCallback[U](callback: Try[V] => U, cell: Cell[K, V]): Unit

  private[cell] def resolveWithValue(value: V): Unit
  private[cell] def cellDependencies: Seq[Cell[K, V]]
  private[cell] def totalCellDependencies: Seq[Cell[K, V]]

  private[cell] def removeCompleteCallbacks(cell: Cell[K, V]): Unit
  private[cell] def removeNextCallbacks(cell: Cell[K, V]): Unit
}

/* State of a cell that is not yet completed.
 *
 * This is not a case class, since it is important that equality is by-reference.
 *
 * @param res       current intermediate result (optional)
 * @param deps      dependencies on other cells
 * @param callbacks list of registered call-back runnables
 */
private class State[K <: Key[V], V](
  val res: V,
  val running: Boolean,
  val completeDeps: Map[Cell[K, V], List[CompleteDepCallback[K, V]]],
  val completeCallbacks: Map[Cell[K, V], List[CompleteCallbackRunnable[K, V]]],
  val nextDeps: Map[Cell[K, V], List[NextDepCallback[K, V]]],
  val nextCallbacks: Map[Cell[K, V], List[NextCallbackRunnable[K, V]]])

private object State {
  def empty[K <: Key[V], V](lattice: Lattice[V]): State[K, V] =
    new State[K, V](lattice.empty, false, Map(), Map(), Map(), Map())
}

class CellImpl[K <: Key[V], V](pool: HandlerPool, val key: K, lattice: Lattice[V], val init: () => Outcome[V]) extends Cell[K, V] with CellCompleter[K, V] {

  private val nodepslatch = new CountDownLatch(1)
  private val nonextdepslatch = new CountDownLatch(1)

  /* Contains a value either of type
   * (a) `Try[V]`      for the final result, or
   * (b) `State[K,V]`  for an incomplete state.
   *
   * Assumes that dependencies need to be kept until a final result is known.
   */
  private val state = new AtomicReference[AnyRef](State.empty[K, V](lattice))

  // `CellCompleter` and corresponding `Cell` are the same run-time object.
  override def cell: Cell[K, V] = this

  override def getResult(): V = state.get() match {
    case finalRes: Try[V] =>
      finalRes match {
        case Success(result) => result
        case Failure(err) => throw new IllegalStateException(err)
      }
    case raw: State[K, V] => raw.res
  }

  override def isComplete: Boolean = state.get match {
    case _: Try[_] => true
    case _ => false
  }

  override def putFinal(x: V): Unit = {
    val res = tryComplete(Success(x))
    if (!res)
      throw new IllegalStateException("Cell already completed.")
  }

  override def putNext(x: V): Unit = {
    val res = tryNewState(x)
    if (!res)
      throw new IllegalStateException("Cell already completed.")
  }

  override def put(x: V, isFinal: Boolean): Unit = {
    if (isFinal) putFinal(x)
    else putNext(x)
  }

  //  def zipFinal(that: Cell[K, V]): Cell[DefaultKey[(V, V)], (V, V)] = {
  //    implicit val theLattice: Lattice[V] = lattice
  //    val completer =
  //      CellCompleter[DefaultKey[(V, V)], (V, V)](pool, new DefaultKey[(V, V)])
  //    this.onComplete {
  //      case Success(x) =>
  //        that.onComplete {
  //          case Success(y) =>
  //            completer.putFinal((x, y))
  //          case f @ Failure(_) =>
  //            completer.tryComplete(f.asInstanceOf[Try[(V, V)]])
  //        }
  //      case f @ Failure(_) =>
  //        completer.tryComplete(f.asInstanceOf[Try[(V, V)]])
  //    }
  //    completer.cell
  //  }

  private[this] def currentState(): State[K, V] =
    state.get() match {
      case finalRes: Try[_] => // completed with final result
        null
      case pre: State[_, _] => // not completed
        pre.asInstanceOf[State[K, V]]
    }

  override private[cell] def numCompleteDependencies: Int = {
    val current = currentState()
    if (current == null) 0
    else current.completeDeps.values.flatten.size
  }

  override private[cell] def numNextDependencies: Int = {
    val current = currentState()
    if (current == null) 0
    else current.nextDeps.values.flatten.size
  }

  override private[cell] def numTotalDependencies: Int = {
    val current = currentState()
    if (current == null) 0
    (current.completeDeps.values.flatten ++ current.nextDeps.values.flatten).size
  }

  override private[cell] def cellDependencies: Seq[Cell[K, V]] = {
    state.get() match {
      case finalRes: Try[_] => // completed with final result
        Seq[Cell[K, V]]()
      case pre: State[_, _] => // not completed
        val current = pre.asInstanceOf[State[K, V]]
        current.completeDeps.keys.toSeq
    }
  }

  override def totalCellDependencies: Seq[Cell[K, V]] = {
    state.get() match {
      case finalRes: Try[_] => // completed with final result
        Seq[Cell[K, V]]()
      case pre: State[_, _] => // not completed
        val current = pre.asInstanceOf[State[K, V]]
        current.completeDeps.keys.toSeq ++ current.nextDeps.keys.toSeq
    }
  }

  override def numNextCallbacks: Int = {
    state.get() match {
      case finalRes: Try[_] => // completed with final result
        0
      case pre: State[_, _] => // not completed
        val current = pre.asInstanceOf[State[K, V]]
        current.nextCallbacks.values.size
    }
  }
  override def numCompleteCallbacks: Int = {
    state.get() match {
      case finalRes: Try[_] => // completed with final result
        0
      case pre: State[_, _] => // not completed
        val current = pre.asInstanceOf[State[K, V]]
        current.completeCallbacks.values.size
    }
  }

  override private[cell] def resolveWithValue(value: V): Unit = {
    this.putFinal(value)
  }

  /**
   * Adds dependency on `other` cell: when `other` cell receives an intermediate result by using
   *  `putNext`, evaluate `pred` with the result of `other`. If this evaluation yields `WhenNext`
   *  or `WhenNextComplete`, `this` cell receives an intermediate or a final result `v`
   *  respectively. To calculate `v`, the `valueCallback` function is called with the result of `other`.
   *
   *  If `v` is `Some(v)`, then the shortcut value is `v`. Otherwise if `value` is `None`,
   *  the cell is not updated.
   *
   *  The thereby introduced dependency is removed when `this` cell
   *  is completed (either prior or after an invocation of `whenNext`).
   */
  override def whenNext(other: Cell[K, V], valueCallback: V => Outcome[V]): Unit = {
    var success = false
    while (!success) {
      state.get() match {
        case finalRes: Try[_] => // completed with final result
          // do not add dependency
          // in fact, do nothing
          success = true

        case raw: State[_, _] => // not completed
          val newDep = new NextDepCallback(pool, other, valueCallback, this)
          // TODO: it looks like `newDep` is wrapped into a CallbackRunnable by `onComplete` -> bad

          val current = raw.asInstanceOf[State[K, V]]
          val newState = current.nextDeps.contains(other) match {
            case true => new State(current.res, current.running, current.completeDeps, current.completeCallbacks, current.nextDeps + (other -> (newDep :: current.nextDeps(other))), current.nextCallbacks)
            case false => new State(current.res, current.running, current.completeDeps, current.completeCallbacks, current.nextDeps + (other -> List(newDep)), current.nextCallbacks)
          }
          if (state.compareAndSet(current, newState)) {
            success = true
            other.addNextCallback(newDep, this)
            pool.triggerExecution(other, pool.getSchedulingStrategy.calcPriority(this, other)) // Check, if strategy(this, other) or strategy(other, this) is correct.  (also in whenNext)
          }
      }
    }
  }
  /**
   * Adds dependency on `other` cell: when `other` cell is completed, evaluate `pred`
   *  with the result of `other`. If this evaluation yields true, complete `this` cell
   *  with what the function `valueCallback` returns.
   *
   *  The thereby introduced dependency is removed when `this` cell
   *  is completed (either prior or after an invocation of `whenComplete`).
   */
  override def whenComplete(other: Cell[K, V], valueCallback: V => Outcome[V]): Unit = {
    state.get() match {
      case finalRes: Try[_] => // completed with final result
      // do not add dependency
      // in fact, do nothing

      case raw: State[_, _] => // not completed
        val newDep = new CompleteDepCallback(pool, other, valueCallback, this)
        // TODO: it looks like `newDep` is wrapped into a CallbackRunnable by `onComplete` -> bad
        other.addCallback(newDep, this)

        val current = raw.asInstanceOf[State[K, V]]
        val newState = current.completeDeps.contains(other) match {
          case true => new State(current.res, current.running, current.completeDeps + (other -> (newDep :: current.completeDeps(other))), current.completeCallbacks, current.nextDeps, current.nextCallbacks)
          case false => new State(current.res, current.running, current.completeDeps + (other -> List(newDep)), current.completeCallbacks, current.nextDeps, current.nextCallbacks)
        }
        state.compareAndSet(current, newState)
        pool.triggerExecution(other, pool.getSchedulingStrategy.calcPriority(this, other)) // Check, if strategy(this, other) or strategy(other, this) is correct. (also in whenNext)
    }
  }

  override private[cell] def addCallback[U](callback: Try[V] => U, cell: Cell[K, V]): Unit = {
    val runnable = new CompleteCallbackRunnable[K, V](pool, callback, this, cell)
    dispatchOrAddCallback(runnable)
  }

  override private[cell] def addNextCallback[U](callback: Try[V] => U, cell: Cell[K, V]): Unit = {
    val runnable = new NextCallbackRunnable[K, V](pool, callback, this, cell)
    dispatchOrAddNextCallback(runnable)
  }

  /**
   * Called by 'putNext' and 'putFinal'. It will try to join the current state
   * with the new value by using the given lattice and return the new value.
   * If 'current == v' then it will return 'v'.
   */
  private def tryJoin(current: V, next: V): V = {
    try {
      lattice.join(current, next)
    } catch {
      case LatticeViolationException(c, n) => current
    }
  }

  /**
   * Called by 'putNext' which will try creating a new state with some new value
   * and then set the new state. The function returns 'true' if it succeeds, 'false'
   * if it fails.
   */
  @tailrec
  private[cell] final def tryNewState(value: V): Boolean = {
    state.get() match {
      case finalRes: Try[_] => // completed with final result already
        val finalResult = finalRes.asInstanceOf[Try[V]].get
        val newVal = tryJoin(finalResult, value)
        val res = finalRes == Success(newVal)
        if (!res) {
          println(s"problem with $this; existing value: $finalRes, new value: $newVal")
        }
        res
      case raw: State[_, _] => // not completed
        val current = raw.asInstanceOf[State[K, V]]
        val newVal = tryJoin(current.res, value)
        if (current.res != newVal) {
          val newState = new State(newVal, current.running, current.completeDeps, current.completeCallbacks, current.nextDeps, current.nextCallbacks)
          if (!state.compareAndSet(current, newState)) {
            tryNewState(value)
          } else {
            // CAS was successful, so there was a point in time where `newVal` was in the cell
            current.nextCallbacks.values.foreach { callbacks =>
              callbacks.foreach(callback => callback.executeWithValue(Success(newVal)))
            }
            true
          }
        } else true
    }
  }

  /**
   * Called by `tryComplete` to store the resolved value and get the current state
   *  or `null` if it is already completed.
   */
  // TODO: take care of compressing root (as in impl.Promise.DefaultPromise)
  @tailrec
  private def tryCompleteAndGetState(v: Try[V]): AnyRef = {
    state.get() match {
      case current: State[_, _] =>
        val currentState = current.asInstanceOf[State[K, V]]
        val newVal = Success(tryJoin(currentState.res, v.get))
        if (state.compareAndSet(current, newVal))
          (currentState, newVal)
        else
          tryCompleteAndGetState(v)

      case finalRes: Try[_] => finalRes
    }
  }

  override def tryComplete(value: Try[V]): Boolean = {
    val resolved: Try[V] = resolveTry(value)

    // the only call to `tryCompleteAndGetState`
    val res = tryCompleteAndGetState(resolved) match {
      case finalRes: Try[_] => // was already complete
        val res = finalRes == value // FIXME: should compare to joined value
        res

      case (pre: State[K, V], newVal: Try[V]) =>
        val depsCells = pre.completeDeps.keys
        val nextDepsCells = pre.nextDeps.keys
        if (pre.completeCallbacks.isEmpty) {
          pre.nextCallbacks.values.foreach { callbacks =>
            callbacks.foreach(callback => callback.executeWithValue(newVal))
          }
        } else {
          // onNext callbacks with these cells should not be triggered, because they have
          // onComplete callbacks which are triggered instead.
          pre.completeCallbacks.values.foreach { callbacks =>
            callbacks.foreach(callback => callback.executeWithValue(newVal))
          }
          pre.nextCallbacks.values.foreach { callbacks =>
            callbacks.foreach { callback =>
              if (!pre.completeCallbacks.contains(callback.otherCell))
                callback.executeWithValue(newVal)
            }
          }
        }

        if (depsCells.nonEmpty)
          depsCells.foreach(_.removeCompleteCallbacks(this))
        if (nextDepsCells.nonEmpty)
          nextDepsCells.foreach(_.removeNextCallbacks(this))

        true
    }
    if (res) {
      pool.deregister(this)
    }
    res
  }

  @tailrec
  override private[cell] final def removeDep(cell: Cell[K, V]): Unit = {
    state.get() match {
      case pre: State[_, _] =>
        val current = pre.asInstanceOf[State[K, V]]
        val newDeps = current.completeDeps - cell

        val newState = new State(current.res, current.running, newDeps, current.completeCallbacks, current.nextDeps, current.nextCallbacks)
        if (!state.compareAndSet(current, newState))
          removeDep(cell)
        else if (newDeps.isEmpty)
          nodepslatch.countDown()

      case _ => /* do nothing */
    }
  }

  @tailrec
  override private[cell] final def removeNextDep(cell: Cell[K, V]): Unit = {
    state.get() match {
      case pre: State[_, _] =>
        val current = pre.asInstanceOf[State[K, V]]
        val newNextDeps = current.nextDeps - cell

        val newState = new State(current.res, current.running, current.completeDeps, current.completeCallbacks, newNextDeps, current.nextCallbacks)
        if (!state.compareAndSet(current, newState))
          removeNextDep(cell)
        else if (newNextDeps.isEmpty)
          nonextdepslatch.countDown()

      case _ => /* do nothing */
    }
  }

  @tailrec
  override private[cell] final def removeCompleteCallbacks(cell: Cell[K, V]): Unit = {
    state.get() match {
      case pre: State[_, _] =>
        val current = pre.asInstanceOf[State[K, V]]
        val newCompleteCallbacks = current.completeCallbacks - cell

        val newState = new State(current.res, current.running, current.completeDeps, newCompleteCallbacks, current.nextDeps, current.nextCallbacks)
        if (!state.compareAndSet(current, newState))
          removeCompleteCallbacks(cell)
      case _ => /* do nothing */
    }
  }

  @tailrec
  override private[cell] final def removeNextCallbacks(cell: Cell[K, V]): Unit = {
    state.get() match {
      case pre: State[_, _] =>
        val current = pre.asInstanceOf[State[K, V]]
        val newNextCallbacks = current.nextCallbacks - cell

        val newState = new State(current.res, current.running, current.completeDeps, current.completeCallbacks, current.nextDeps, newNextCallbacks)
        if (!state.compareAndSet(current, newState))
          removeNextCallbacks(cell)
      case _ => /* do nothing */
    }
  }

  override private[cell] def waitUntilNoDeps(): Unit = {
    nodepslatch.await()
  }

  override private[cell] def waitUntilNoNextDeps(): Unit = {
    nonextdepslatch.await()
  }

  override private[cell] def isRunning = {
    state.get() match {
      case _: Try[_] => false
      case s: State[_, _] => s.running
    }
  }

  /**
   * Mark this cell as "running".
   *
   * @return Returns true, iff the cell's status changed (i.e. it had not been running before).
   */
  @tailrec
  override private[cell] final def markAsRunning(): Boolean = {
    state.get() match {
      case pre: State[_, _] =>
        val current = pre.asInstanceOf[State[K, V]]
        val newState = new State(current.res, true, current.completeDeps, current.completeCallbacks, current.nextDeps, current.nextCallbacks)
        if (!state.compareAndSet(current, newState)) markAsRunning()
        else !pre.running
      case _ => false
    }
  }

  // Schedules execution of `callback` when next intermediate result is available.
  override private[cell] def onNext[U](callback: Try[V] => U): Unit = {
    val runnable = new NextCallbackRunnable[K, V](pool, callback, this, this)
    dispatchOrAddNextCallback(runnable)
  }

  // Schedules execution of `callback` when completed with final result.
  override private[cell] def onComplete[U](callback: Try[V] => U): Unit = {
    val runnable = new CompleteCallbackRunnable[K, V](pool, callback, this, this)
    dispatchOrAddCallback(runnable)
  }

  /**
   * Tries to add the callback, if already completed, it dispatches the callback to be executed.
   *  Used by `onComplete()` to add callbacks to a promise and by `link()` to transfer callbacks
   *  to the root promise when linking two promises together.
   */
  @tailrec
  private def dispatchOrAddCallback(runnable: CompleteCallbackRunnable[K, V]): Unit = {
    state.get() match {
      case r: Try[V] => runnable.executeWithValue(r)
      // case _: DefaultPromise[_] => compressedRoot().dispatchOrAddCallback(runnable)
      case pre: State[_, _] =>
        // assemble new state
        val current = pre.asInstanceOf[State[K, V]]
        val newState = current.completeCallbacks.contains(runnable.otherCell) match {
          case true => new State(current.res, current.running, current.completeDeps, current.completeCallbacks + (runnable.otherCell -> (runnable :: current.completeCallbacks(runnable.otherCell))), current.nextDeps, current.nextCallbacks)
          case false => new State(current.res, current.running, current.completeDeps, current.completeCallbacks + (runnable.otherCell -> List(runnable)), current.nextDeps, current.nextCallbacks)
        }
        if (!state.compareAndSet(pre, newState)) dispatchOrAddCallback(runnable)
    }
  }

  /**
   * Tries to add the callback, if already completed, it dispatches the callback to be executed.
   *  Used by `onNext()` to add callbacks to a promise and by `link()` to transfer callbacks
   *  to the root promise when linking two promises together.
   */
  @tailrec
  private def dispatchOrAddNextCallback(runnable: NextCallbackRunnable[K, V]): Unit = {
    state.get() match {
      case r: Try[V] => runnable.executeWithValue(r)
      /* Cell is completed, do nothing emit an onNext callback */
      // case _: DefaultPromise[_] => compressedRoot().dispatchOrAddCallback(runnable)
      case pre: State[_, _] =>
        // assemble new state
        val current = pre.asInstanceOf[State[K, V]]
        val newState = current.nextCallbacks.contains(runnable.otherCell) match {
          case true => new State(current.res, current.running, current.completeDeps, current.completeCallbacks, current.nextDeps, current.nextCallbacks + (runnable.otherCell -> (runnable :: current.nextCallbacks(runnable.otherCell))))
          case false => new State(current.res, current.running, current.completeDeps, current.completeCallbacks, current.nextDeps, current.nextCallbacks + (runnable.otherCell -> List(runnable)))
        }
        if (!state.compareAndSet(pre, newState)) dispatchOrAddNextCallback(runnable)
    }
  }

  // copied from object `impl.Promise`
  private def resolveTry[T](source: Try[T]): Try[T] = source match {
    case Failure(t) => resolver(t)
    case _ => source
  }

  // copied from object `impl.Promise`
  private def resolver[T](throwable: Throwable): Try[T] = throwable match {
    case t: scala.runtime.NonLocalReturnControl[_] => Success(t.value.asInstanceOf[T])
    case t: scala.util.control.ControlThrowable => Failure(new ExecutionException("Boxed ControlThrowable", t))
    case t: InterruptedException => Failure(new ExecutionException("Boxed InterruptedException", t))
    case e: Error => Failure(new ExecutionException("Boxed Error", e))
    case t => Failure(t)
  }

}

/*
  This is called to inform this `completer`, that `otherCell` has been completed with a value.
 */
private class CompleteDepCallback[K <: Key[V], V](
  val pool: HandlerPool,
  val otherCell: Cell[K, V],
  val shortCutValueCallback: V => Outcome[V],
  val completer: CellCompleter[K, V])
  extends /*Runnable with OnCompleteRunnable with */ (Try[V] => Unit) {
  // must be filled in before running it
  var value: Try[V] = null

  override def apply(x: Try[V]): Unit = x match {
    case Success(v) =>
      shortCutValueCallback(v) match {
        case FinalOutcome(v) => completer.putFinal(v)
        case NextOutcome(v) => completer.putNext(v)
        case NoOutcome =>
          completer.removeDep(otherCell)
          completer.removeNextDep(otherCell)
      }
    case Failure(e) =>
      completer.removeDep(otherCell)
      completer.removeNextDep(otherCell)
  }

  /*override def run(): Unit = {
    try apply(value) catch { case NonFatal(e) => pool reportFailure e }
  }

  def executeWithValue(v: Try[V]): Unit = {
    value = v
    try pool.execute(this, 0/* FIXME Set a prio?! */) catch { case NonFatal(t) => pool reportFailure t }
  }*/
}

// copied from `impl.CallbackRunnable` in Scala core lib.
/**
 * @param executor   The thread that runs the callback function
 * @param onComplete Callback function that is triggered on an onComplete event
 * @param cell       The cell that depends on this callback
 */
private class CompleteCallbackRunnable[K <: Key[V], V](
  val executor: HandlerPool,
  val onComplete: Try[V] => Any,
  val thisCell: Cell[K, V],
  val otherCell: Cell[K, V])

  extends Runnable with OnCompleteRunnable {
  // must be filled in before running it
  var value: Try[V] = null

  override def run() = {
    require(value ne null) // must set value to non-null before running!
    try onComplete(value) catch { case NonFatal(e) => executor reportFailure e }
  }

  def executeWithValue(v: Try[V]): Unit = {
    require(value eq null) // can't complete it twice
    value = v
    // Note that we cannot prepare the ExecutionContext at this point, since we might
    // already be running on a different thread!
    try executor.execute(this, executor.getSchedulingStrategy.calcPriority(thisCell, otherCell)) catch { case NonFatal(t) => executor reportFailure t }
  }
}

/*
  This is called to inform this `completer`, that `otherCell` has been updated with a new value.
 */
private class NextDepCallback[K <: Key[V], V](
  val pool: HandlerPool,
  val otherCell: Cell[K, V], // otherCell
  val shortCutValueCallback: V => Outcome[V],
  val completer: CellCompleter[K, V]) // this
  extends /*Runnable with OnCompleteRunnable with*/ (Try[V] => Unit) {
  var value: Try[V] = null

  override def apply(x: Try[V]): Unit = {
    x match {
      case Success(v) =>
        shortCutValueCallback(v) match {
          case NextOutcome(v) =>
            completer.putNext(v)
          case FinalOutcome(v) =>
            completer.putFinal(v)
          case _ => /* do nothing */
        }
      case Failure(e) => /* do nothing */
    }

    if (otherCell.isComplete) completer.removeNextDep(otherCell)
  }

  /*override def run(): Unit = {
    try apply(value) catch { case NonFatal(e) => pool reportFailure e }
  }

  This us not used by any tests and appears to be not used at all, so I removed it.
  def executeWithValue(v: Try[V]): Unit = {
    value = v
    try pool.execute(this, 0 /* FIXME set a prio?! */) catch { case NonFatal(t) => pool reportFailure t }
  }*/
}

/**
 * @param executor The thread that runs the callback function
 * @param onNext   Callback function that is triggered on an onNext event
 * @param thisCell The cell that depends on this callback
 */
private class NextCallbackRunnable[K <: Key[V], V](
  val executor: HandlerPool,
  val onNext: Try[V] => Any,
  val thisCell: Cell[K, V],
  val otherCell: Cell[K, V]) {
  def executeWithValue(v: Try[V]): Unit = {
    // Note that we cannot prepare the ExecutionContext at this point, since we might
    // already be running on a different thread!
    try executor.execute(() => { onNext(v); () }, executor.getSchedulingStrategy.calcPriority(thisCell, otherCell)) catch { case NonFatal(t) => executor reportFailure t }
  }
}
