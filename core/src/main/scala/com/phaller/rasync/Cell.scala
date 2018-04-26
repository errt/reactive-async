package com.phaller.rasync

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{ CountDownLatch, ExecutionException }

import com.phaller.rasync.lattice.{ Key, NotMonotonicException, DefaultKey, Updater, PartialOrderingWithBottom }

import scala.annotation.tailrec
import scala.util.{ Failure, Success, Try }

trait Cell[K <: Key[V], V] {
  private[rasync] val completer: CellCompleter[K, V]

  def key: K

  /**
   * Returns the current value of `this` `Cell`.
   *
   * Note that this method may return non-deterministic values. To ensure
   * deterministic executions use the quiescence API of class `HandlerPool`.
   */
  def getResult(): V

  /**
   * Returns the current value of `this` `Cell` and its `completeness`.
   *
   * Note that this method may return non-deterministic values. To ensure
   * deterministic executions use the quiescence API of class `HandlerPool`.
   */
  def getState(): (V, Boolean)

  /** Start computations associated with this cell. */
  def trigger(): Unit

  def isComplete: Boolean

  /**
   * Adds a dependency on some `other` cell.
   *
   * Example:
   * {{{
   *   when(cell, (x, isFinal) => x match { // when the next value or final value is put into `cell`
   *     case (_, Impure) => FinalOutcome(Impure)  // if the next value of `cell` is `Impure`, `this` cell is completed with value `Impure`
   *     case (true, Pure) => FinalOutcome(Pure)// if the final value of `cell` is `Pure`, `this` cell is completed with `Pure`.
   *     case _ => NoOutcome
   *   })
   * }}}
   *
   * @param other  Cell that `this` Cell depends on.
   * @param valueCallback  Callback that receives the new value of `other` and returns an `Outcome` for `this` cell.
   */
  def when(other: Cell[K, V], valueCallback: (V, Boolean) => Outcome[V]): Unit
  def whenSequential(other: Cell[K, V], valueCallback: (V, Boolean) => Outcome[V]): Unit

//  def zipFinal(that: Cell[K, V]): Cell[DefaultKey[(V, V)], (V, V)]

  // internal API

  // Schedules execution of `callback` when next intermediate or final result is available.
  private[rasync] def on[U](callback: (Try[V], Boolean) => U): Unit

  // Only used in tests.
  private[rasync] def waitUntilNoDeps(): Unit

  private[rasync] def tasksActive(): Boolean
  private[rasync] def setTasksActive(): Boolean

  private[rasync] def numDependencies: Int

  private[rasync] def numCallbacks: Int

  private[rasync] def addCallback(callback: CombinedCallbackRunnable[K, V], cell: Cell[K, V]): Unit

  private[rasync] def resolveWithValue(value: V, dontCall: Iterable[Cell[K, V]]): Unit
  private[rasync] def resolveWithValue(value: V): Unit

  def totalCellDependencies: Seq[Cell[K, V]]
  def isIndependent(): Boolean

  private[rasync] def removeCallbacks(cell: Cell[K, V]): Unit
  private[rasync] def removeCallbacks(cells: Iterable[Cell[K, V]]): Unit

  def isADependee(): Boolean
}

object Cell {

  def completed[V](result: V)(implicit updater: Updater[V], pool: HandlerPool): Cell[DefaultKey[V], V] = {
    val completer = CellCompleter.completed(result)(updater, pool)
    completer.cell
  }

//  def sequence[K <: Key[V], V](in: List[Cell[K, V]])(implicit pool: HandlerPool): Cell[DefaultKey[List[V]], List[V]] = {
//    implicit val updater: Updater[List[V]] = Updater.partialOrderingToUpdater(PartialOrderingWithBottom.trivial[List[V]])
//    val completer =
//      CellCompleter[DefaultKey[List[V]], List[V]](new DefaultKey[List[V]])
//    in match {
//      case List(c) =>
//        c.onComplete {
//          case Success(x) =>
//            completer.putFinal(List(x))
//          case f @ Failure(_) =>
//            completer.tryComplete(f.asInstanceOf[Failure[List[V]]])
//        }
//      case c :: cs =>
//        val fst = in.head
//        fst.onComplete {
//          case Success(x) =>
//            val tailCell = sequence(in.tail)
//            tailCell.onComplete {
//              case Success(xs) =>
//                completer.putFinal(x :: xs)
//              case f @ Failure(_) =>
//                completer.tryComplete(f)
//            }
//          case f @ Failure(_) =>
//            completer.tryComplete(f.asInstanceOf[Failure[List[V]]])
//        }
//    }
//    completer.cell
//  }

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
  val tasksActive: Boolean,
  val deps: List[Cell[K, V]],
  val callbacks: Map[Cell[K, V], List[CombinedCallbackRunnable[K, V]]])

private object State {
  def empty[K <: Key[V], V](updater: Updater[V]): State[K, V] =
    new State[K, V](updater.initial, false, List(), Map())
}

private class CellImpl[K <: Key[V], V](pool: HandlerPool, val key: K, updater: Updater[V], val init: (Cell[K, V]) => Outcome[V]) extends Cell[K, V] with CellCompleter[K, V] {

  override val completer: CellCompleter[K, V] = this.asInstanceOf[CellCompleter[K, V]]

  implicit val ctx = pool

  private val nodepslatch = new CountDownLatch(1)

  /* Contains a value either of type
   * (a) `Try[V]`      for the final result, or
   * (b) `State[K,V]`  for an incomplete state.
   *
   * Assumes that dependencies need to be kept until a final result is known.
   */
  private val state = new AtomicReference[AnyRef](State.empty[K, V](updater))

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

  override def getState(): (V, Boolean) = state.get() match {
    case finalRes: Try[V] =>
      finalRes match {
        case Success(result) => (result, true)
        case Failure(err) => throw new IllegalStateException(err)
      }
    case raw: State[K, V] => (raw.res, false)
  }

  override def trigger(): Unit = {
    pool.triggerExecution(this)
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
//    implicit val theUpdater: Updater[V] = updater
//    val completer =
//      CellCompleter[DefaultKey[(V, V)], (V, V)](new DefaultKey[(V, V)])(Updater.pair(updater), pool)
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

  override private[rasync] def numDependencies: Int = {
    val current = currentState()
    if (current == null) 0

    else current.deps.toSet.size
  }

  override def totalCellDependencies: Seq[Cell[K, V]] = {
    state.get() match {
      case finalRes: Try[_] => // completed with final result
        Seq[Cell[K, V]]()
      case pre: State[_, _] => // not completed
        val current = pre.asInstanceOf[State[K, V]]
        current.deps
    }
  }

  override def isIndependent(): Boolean = {
    state.get() match {
      case finalRes: Try[_] => // completed with final result
        true
      case pre: State[_, _] => // not completed
        val current = pre.asInstanceOf[State[K, V]]
        current.deps.isEmpty
    }
  }

  override def numCallbacks: Int = {
    state.get() match {
      case finalRes: Try[_] => // completed with final result
        0
      case pre: State[_, _] => // not completed
        val current = pre.asInstanceOf[State[K, V]]
        current.callbacks.values.size
    }
  }

  override private[rasync] def resolveWithValue(value: V, dontCall: Iterable[Cell[K, V]]): Unit = {
    val res = tryComplete(Success(value), dontCall)
    if (!res)
      throw new IllegalStateException("Cell already completed.")
  }

  override private[rasync] def resolveWithValue(value: V): Unit = {
    this.putFinal(value)
  }

  override def when(other: Cell[K, V], valueCallback: (V, Boolean) => Outcome[V]): Unit = {
    this.when(other, valueCallback, sequential = false)
  }

  override def whenSequential(other: Cell[K, V], valueCallback: (V, Boolean) => Outcome[V]): Unit = {
    this.when(other, valueCallback, sequential = true)
  }

  private def when(other: Cell[K, V], valueCallback: (V, Boolean) => Outcome[V], sequential: Boolean): Unit = {
    var success = false
    while (!success) {
      state.get() match {
        case finalRes: Try[_] => // completed with final result
          // do not add dependency
          // in fact, do nothing
          success = true

        case raw: State[_, _] => // not completed
          val newDep: CombinedDepRunnable[K, V] =
            if (sequential) new CombinedSequentialDepRunnable(pool, this, other, valueCallback)
            else new CombinedConcurrentDepRunnable(pool, this, other, valueCallback)

          val current = raw.asInstanceOf[State[K, V]]
          val depRegistered =
            if (current.deps.contains(other)) true
            else {
              val newState = new State(current.res, current.tasksActive, other :: current.deps, current.callbacks)
              state.compareAndSet(current, newState)
            }
          if (depRegistered) {
            success = true
            other.addCallback(newDep, this)
            pool.triggerExecution(other)
          }
      }
    }
  }

  override private[rasync] def addCallback(callback: CombinedCallbackRunnable[K, V], cell: Cell[K, V]): Unit = {
    dispatchOrAddCallback(callback)
  }

  /**
   * Called by 'putNext' and 'putFinal'. It will try to join the current state
   * with the new value by using the given updater and return the new value.
   * If 'current == v' then it will return 'v'.
   */
  private def tryJoin(current: V, next: V): V = {
    updater.update(current, next)
  }

  /**
   * Called by 'putNext' which will try creating a new state with some new value
   * and then set the new state. The function returns 'true' if it succeeds, 'false'
   * if it fails.
   */
  @tailrec
  private[rasync] final def tryNewState(value: V): Boolean = {
    state.get() match {
      case finalRes: Try[_] => // completed with final result already
        if (!updater.ignoreIfFinal()) {
          // Check, if the incoming result would have changed the final result.
          try {
            val finalResult = finalRes.asInstanceOf[Try[V]].get
            val newVal = tryJoin(finalResult, value)
            val res = finalRes == Success(newVal)
            res
          } catch {
            case _: NotMonotonicException[_] => false
          }
        } else {
          // Do not check anything, if we are final already
          true
        }
      case raw: State[_, _] => // not completed
        val current = raw.asInstanceOf[State[K, V]]
        val newVal = tryJoin(current.res, value)
        if (current.res != newVal) {
          val newState = new State(newVal, current.tasksActive, current.deps, current.callbacks)
          if (!state.compareAndSet(current, newState)) {
            tryNewState(value)
          } else {
            // CAS was successful, so there was a point in time where `newVal` was in the cell
            current.callbacks.values.foreach { callbacks =>
              callbacks.foreach(callback => callback.execute())
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
          currentState
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

        if (!updater.ignoreIfFinal()) {
          // Check, if the incoming result would have changed the final result.
          try {
            val finalResult = finalRes.asInstanceOf[Try[V]].get
            val newVal = value.map(tryJoin(finalResult, _))
            val res = finalRes == newVal
            res
          } catch {
            case _: NotMonotonicException[_] => false
          }
        } else {
          // Do not check anything, if we are final already
          true
        }

      case pre: State[K, V] =>
        val callbacks = pre.callbacks

        if (callbacks.nonEmpty)
          callbacks.values.foreach { callbacks =>
            callbacks.foreach(callback => callback.execute())
          }

        val depsCells = pre.deps

        if (depsCells.nonEmpty)
          depsCells.foreach(_.removeCallbacks(this))

        true
    }
    res
  }

  private def tryComplete(value: Try[V], dontCall: Iterable[Cell[K, V]]): Boolean = {
    val resolved: Try[V] = resolveTry(value)

    val res = tryCompleteAndGetState(resolved) match {
      case finalRes: Try[_] => // was already complete
        val finalResult = finalRes.asInstanceOf[Try[V]].get
        val newVal = value.map(tryJoin(finalResult, _))
        val res = finalRes == newVal
        res

      case pre: State[K, V] =>
        val callbacks = pre.callbacks -- dontCall

        if (callbacks.nonEmpty)
          callbacks.values.foreach { callbacks =>
              callbacks.foreach(callback => callback.execute())
          }

        val deps = pre.deps

        if (deps.nonEmpty)
          deps.foreach(_.removeCallbacks(this))

        true
    }
    res
  }

  @tailrec
  override private[rasync] final def removeDep(cell: Cell[K, V]): Unit = {
    state.get() match {
      case pre: State[_, _] =>
        val current = pre.asInstanceOf[State[K, V]]
        val newDeps = current.deps.filterNot(_ == cell)

        val newState = new State(current.res, current.tasksActive, newDeps, current.callbacks)
        if (!state.compareAndSet(current, newState))
          removeDep(cell)
        else if (newDeps.isEmpty)
          nodepslatch.countDown()

      case _ => /* do nothing */
    }
  }


  @tailrec
  override final def removeCallbacks(cell: Cell[K, V]): Unit = {
    state.get() match {
      case pre: State[_, _] =>
        val current = pre.asInstanceOf[State[K, V]]
        val newCombinedCallbacks = current.callbacks - cell

        val newState = new State(current.res, current.tasksActive, current.deps, newCombinedCallbacks)
        if (!state.compareAndSet(current, newState))
          removeCallbacks(cell)
      case _ => /* do nothing */
    }
  }

  @tailrec
  override private[rasync] final def removeCallbacks(cells: Iterable[Cell[K, V]]): Unit = {
    state.get() match {
      case pre: State[_, _] =>
        val current = pre.asInstanceOf[State[K, V]]
        val newCallbacks = current.callbacks -- cells

        val newState = new State(current.res, current.tasksActive, current.deps, newCallbacks)

        if (!state.compareAndSet(current, newState))
          removeCallbacks(cells)
      case _ => /* do nothing */
    }
  }

  override private[rasync] def waitUntilNoDeps(): Unit = {
    nodepslatch.await()
  }

  override private[rasync] def tasksActive() = state.get() match {
    case _: Try[_] => false
    case s: State[_, _] => s.tasksActive
  }

  /**
   * Mark this cell as "running".
   *
   * @return Returns true, iff the cell's status changed (i.e. it had not been running before).
   */
  @tailrec
  override private[rasync] final def setTasksActive(): Boolean = state.get() match {
    case pre: State[_, _] =>
      if (pre.tasksActive)
        false
      else {
        val current = pre.asInstanceOf[State[K, V]]
        val newState = new State(current.res, true, current.deps, current.callbacks)
        if (!state.compareAndSet(current, newState)) setTasksActive()
        else !pre.tasksActive
      }
    case _ => false
  }

  // Schedules execution of `callback` when next intermediate result is available.
  override private[rasync] def on[U](callback: (Try[V],  Boolean) => U): Unit = {
    val runnable = new CombinedConcurrentCallbackRunnable[K, V](pool, null, this, callback) // NULL indicates that no cell is waiting for this callback.
    dispatchOrAddCallback(runnable)
  }

  /**
   * Tries to add the callback, if already completed, it dispatches the callback to be executed.
   *  Used by `onNext()` to add callbacks to a promise and by `link()` to transfer callbacks
   *  to the root promise when linking two promises together.
   */
  @tailrec
  private def dispatchOrAddCallback(runnable: CombinedCallbackRunnable[K, V]): Unit = {
    state.get() match {
      case r: Try[V] => runnable.execute()
      /* Cell is completed, do nothing emit an onNext callback */
      // case _: DefaultPromise[_] => compressedRoot().dispatchOrAddCallback(runnable)
      case pre: State[_, _] =>
        // assemble new state
        val current = pre.asInstanceOf[State[K, V]]

        val newState = current.callbacks.contains(runnable.dependentCell) match {
          case true => new State(current.res, current.tasksActive, current.deps, current.callbacks + (runnable.dependentCell -> (runnable :: current.callbacks(runnable.dependentCell))))
          case false => new State(current.res, current.tasksActive,current.deps, current.callbacks + (runnable.dependentCell -> List(runnable)))
        }
        if (!state.compareAndSet(pre, newState)) dispatchOrAddCallback(runnable)
        else if (current.res != updater.initial) runnable.execute()
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

  /**
   * Checks if this cell is a dependee of some other cells. This is true if some cells called
   * whenNext[Sequential / Complete](thisCell, f)
   * @return True if some cells depends on this one, false otherwise
   */
  override def isADependee(): Boolean = {
    numCallbacks > 0
  }

}
