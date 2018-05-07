package com.phaller.rasync

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{ CountDownLatch, ExecutionException }

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}
import lattice.{DefaultKey, Key, PartialOrderingWithBottom, Updater}

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

  /** Start computations associated with this cell. */
  def trigger(): Unit

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
   * @param valueCallback  Callback that receives the final value of `other` and returns an `Outcome` for `this` cell.
   */
  def whenComplete(other: Cell[K, V], valueCallback: V => Outcome[V]): Unit
  def whenCompleteSequential(other: Cell[K, V], valueCallback: V => Outcome[V]): Unit

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
  def whenNextSequential(other: Cell[K, V], valueCallback: V => Outcome[V]): Unit

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

  def zipFinal(that: Cell[K, V]): Cell[DefaultKey[(V, V)], (V, V)]

  // internal API

  // Schedules execution of `callback` when next intermediate result is available. (not thread safe!)
  private[rasync] def onNext[U](callback: Try[V] => U): Unit //(implicit context: ExecutionContext): Unit

  // Schedules execution of `callback` when completed with final result. (not thread safe!)
  private[rasync] def onComplete[U](callback: Try[V] => U): Unit

  // Only used in tests.
  private[rasync] def waitUntilNoDeps(): Unit

  // Only used in tests.
  private[rasync] def waitUntilNoNextDeps(): Unit

  private[rasync] def tasksActive(): Boolean
  private[rasync] def setTasksActive(): Boolean

  private[rasync] def numTotalDependencies: Int
  private[rasync] def numNextDependencies: Int
  private[rasync] def numCompleteDependencies: Int

  private[rasync] def numNextCallbacks: Int
  private[rasync] def numCompleteCallbacks: Int

  private[rasync] def addCompleteDependentCell(dependentCell: Cell[K, V]): Unit
  private[rasync] def addNextDependentCell(dependentCell: Cell[K, V]): Unit
  private[rasync] def addCombinedDependentCell(dependentCell: Cell[K, V]): Unit


  private[rasync] def removeDependentCell(dependentCell: Cell[K, V]): Unit

  private[rasync] def resolveWithValue(value: V): Unit
  def completeCellDependencies: Seq[Cell[K, V]]
  def totalCellDependencies: Seq[Cell[K, V]]
  def isIndependent(): Boolean

  def removeCompleteCallbacks(cell: Cell[K, V]): Unit
  def removeNextCallbacks(cell: Cell[K, V]): Unit

  /** Removes all callbacks that are called, when `cell` has been updated.
    * I.e., this method kills the dependency on the dependee's side.
    *
    * Use removeDependentCell to remove the dependency on the other side.
    */
  private[rasync] def removeAllCallbacks(cell: Cell[K, V]): Unit
  private[rasync] def removeAllCallbacks(cells: Seq[Cell[K, V]]): Unit

  private[rasync] def updateDeps(): Unit
  private[rasync] def getStagedValueFor(dependentCell: Cell[K, V]): Outcome[V]
  private[rasync] def hasStagedValueFor(dependentCell: Cell[K, V]): Outcome[V]

  def isADependee(): Boolean
}

object Cell {

  def completed[V](result: V)(implicit updater: Updater[V], pool: HandlerPool): Cell[DefaultKey[V], V] = {
    val completer = CellCompleter.completed(result)(updater, pool)
    completer.cell
  }

  def sequence[K <: Key[V], V](in: List[Cell[K, V]])(implicit pool: HandlerPool): Cell[DefaultKey[List[V]], List[V]] = {
    implicit val updater: Updater[List[V]] = Updater.partialOrderingToUpdater(PartialOrderingWithBottom.trivial[List[V]])
    val completer =
      CellCompleter[DefaultKey[List[V]], List[V]](new DefaultKey[List[V]])
    in match {
      case List(c) =>
        c.onComplete {
          case Success(x) =>
            completer.putFinal(List(x))
          case f @ Failure(_) =>
            completer.tryComplete(f.asInstanceOf[Failure[List[V]]])
        }
      case c :: cs =>
        val fst = in.head
        fst.onComplete {
          case Success(x) =>
            val tailCell = sequence(in.tail)
            tailCell.onComplete {
              case Success(xs) =>
                completer.putFinal(x :: xs)
              case f @ Failure(_) =>
                completer.tryComplete(f)
            }
          case f @ Failure(_) =>
            completer.tryComplete(f.asInstanceOf[Failure[List[V]]])
        }
    }
    completer.cell
  }

}

/* State of a cell that is not yet completed.
 *
 * This is not a case class, since it is important that equality is by-reference.
 *
 * @param res       current intermediate result (optional)
 * @param deps      dependent Cells + a staged value for this cell (this is not needed for completeDeps, the staged value is always the final one)
 * @param callbacks those callbacks are run if the cell t(hat we depend on) changes.
 */
private class State[K <: Key[V], V](
     val res: V,
     val tasksActive: Boolean,
     val completeDependentCells: List[Cell[K, V]],
     val completeCallbacks: Map[Cell[K, V], CompleteCallbackRunnable[K, V]],
     val nextDependentCells: Map[Cell[K, V], Outcome[V]],
     val nextCallbacks: Map[Cell[K, V], NextCallbackRunnable[K, V]],
     val combinedCallbacks: Map[Cell[K, V], CombinedCallbackRunnable[K, V]]
)

private object State {
  def empty[K <: Key[V], V](updater: Updater[V]): State[K, V] =
    new State[K, V](updater.bottom, false, List(), Map(), Map(), Map(), Map())
}

private class CellImpl[K <: Key[V], V](pool: HandlerPool, val key: K, updater: Updater[V], val init: (Cell[K, V]) => Outcome[V]) extends Cell[K, V] with CellCompleter[K, V] {

  override val completer: CellCompleter[K, V] = this.asInstanceOf[CellCompleter[K, V]]

  implicit val ctx = pool

  private val nocompletedepslatch = new CountDownLatch(1)
  private val nonextdepslatch = new CountDownLatch(1)

  /* Contains a value either of type
   * (a) `Try[V]`      for the final result, or
   * (b) `State[K,V]`  for an incomplete state.
   *
   * Assumes that dependencies need to be kept until a final result is known.
   *
   * This is only true, if Updater.ignoreIfFinal==true, because we miss
   * IllegalStateExceptions.
   */
  private val state = new AtomicReference[AnyRef](State.empty[K, V](updater))

  private var onCompleteHandler: List[Try[V] => Any] = List()
  private var onNextHandler: List[Try[V] => Any] = List()

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

  def zipFinal(that: Cell[K, V]): Cell[DefaultKey[(V, V)], (V, V)] = {
    implicit val theUpdater: Updater[V] = updater
    val completer =
      CellCompleter[DefaultKey[(V, V)], (V, V)](new DefaultKey[(V, V)])(Updater.pair(updater), pool)
    this.onComplete {
      case Success(x) =>
        that.onComplete {
          case Success(y) =>
            completer.putFinal((x, y))
          case f @ Failure(_) =>
            completer.tryComplete(f.asInstanceOf[Try[(V, V)]])
        }
      case f @ Failure(_) =>
        completer.tryComplete(f.asInstanceOf[Try[(V, V)]])
    }
    completer.cell
  }

  private[this] def currentState(): State[K, V] =
    state.get() match {
      case finalRes: Try[_] => // completed with final result
        null
      case pre: State[_, _] => // not completed
        pre.asInstanceOf[State[K, V]]
    }

  override private[rasync] def numCompleteDependencies: Int = {
    val current = currentState()
    if (current == null) 0
    else current.completeCallbacks.keys.size
  }

  override private[rasync] def numNextDependencies: Int = {
    val current = currentState()
    if (current == null) 0
    else current.nextCallbacks.keys.size
  }

  override private[rasync] def numTotalDependencies: Int = {
    val current = currentState()
    if (current == null) 0
    else (current.completeCallbacks.keys ++ current.nextCallbacks.keys).size
  }

  override def completeCellDependencies: Seq[Cell[K, V]] = {
    state.get() match {
      case finalRes: Try[_] => // completed with final result
        Seq[Cell[K, V]]()
      case pre: State[_, _] => // not completed
        val current = pre.asInstanceOf[State[K, V]]
        current.completeCallbacks.keys.toSeq
    }
  }

  override def totalCellDependencies: Seq[Cell[K, V]] = {
    state.get() match {
      case finalRes: Try[_] => // completed with final result
        Seq[Cell[K, V]]()
      case pre: State[_, _] => // not completed
        val current = pre.asInstanceOf[State[K, V]]
        (current.completeCallbacks.keys ++ current.nextCallbacks.keys).toSeq
    }
  }

  override def isIndependent(): Boolean = {
    state.get() match {
      case finalRes: Try[_] => // completed with final result
        true
      case pre: State[_, _] => // not completed
        val current = pre.asInstanceOf[State[K, V]]
        current.completeDependentCells.isEmpty && current.nextDependentCells.isEmpty
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
          val current = raw.asInstanceOf[State[K, V]]
          val depRegistered =
            if (current.nextCallbacks.contains(other)) true
            else {
              val newCallback: CombinedCallbackRunnable[K, V] =
                if (sequential) new CombinedSequentialCallbackRunnable(pool, this, other, valueCallback)
                else new CombinedConcurrentCallbackRunnable(pool, this, other, valueCallback)

              val newState = new State(current.res, current.tasksActive, current.completeDependentCells, current.completeCallbacks, current.nextDependentCells, current.nextCallbacks, current.combinedCallbacks  + (other -> newCallback) )
              // TODO Check, what we need to do, if a callback has been registered already
              state.compareAndSet(current, newState)
            }
          if (depRegistered) {
            success = true
            other.addCombinedDependentCell(this)
            pool.triggerExecution(other)
          }
      }
    }
  }

  override def whenNext(other: Cell[K, V], valueCallback: V => Outcome[V]): Unit = {
    this.whenNext(other, valueCallback, sequential = false)
  }

  override def whenNextSequential(other: Cell[K, V], valueCallback: V => Outcome[V]): Unit = {
    this.whenNext(other, valueCallback, sequential = true)
  }

  private def whenNext(other: Cell[K, V], valueCallback: V => Outcome[V], sequential: Boolean): Unit = {
    var success = false
    while (!success) {
      state.get() match {
        case finalRes: Try[_] => // completed with final result
          // do not add dependency
          // in fact, do nothing
          success = true

        case raw: State[_, _] => // not completed
          val current = raw.asInstanceOf[State[K, V]]
          val depRegistered =
            if (current.nextCallbacks.contains(other)) true
            else {
              val newCallback: NextCallbackRunnable[K, V] =
                if (sequential) new NextSequentialCallbackRunnable(pool, this, other, valueCallback)
                else new NextConcurrentCallbackRunnable(pool, this, other, valueCallback)

              val newState = new State(current.res, current.tasksActive, current.completeDependentCells, current.completeCallbacks, current.nextDependentCells, current.nextCallbacks + (other -> newCallback ), current.combinedCallbacks)
              // TODO Check, what we need to do, if a callback has been registered already
              state.compareAndSet(current, newState)
            }
          if (depRegistered) {
            success = true
            other.addNextDependentCell(this)
            pool.triggerExecution(other)
          }
      }
    }
  }

  override def whenComplete(other: Cell[K, V], valueCallback: V => Outcome[V]): Unit = {
    this.whenComplete(other, valueCallback, false)
  }

  override def whenCompleteSequential(other: Cell[K, V], valueCallback: V => Outcome[V]): Unit = {
    this.whenComplete(other, valueCallback, true)
  }

  private def whenComplete(other: Cell[K, V], valueCallback: V => Outcome[V], sequential: Boolean): Unit = {
    var success = false
    while (!success) {
      state.get() match {
        case _: Try[_] => // completed with final result
          // do not add dependency
          // in fact, do nothing
          success = true

        case raw: State[_, _] => // not completed
          val current = raw.asInstanceOf[State[K, V]]
          val depRegistered =
            if (current.completeCallbacks.contains(other)) true
            else {
              val newCallback: CompleteCallbackRunnable[K, V] =
                if (sequential) new CompleteSequentialCallbackRunnable(pool, this, other, valueCallback)
                else new CompleteConcurrentCallbackRunnable(pool, this, other, valueCallback)

              val newState = new State(current.res, current.tasksActive, current.completeDependentCells, current.completeCallbacks + (other -> newCallback ), current.nextDependentCells, current.nextCallbacks, current.combinedCallbacks)
              // TODO see whenNext
              state.compareAndSet(current, newState)
            }
          if (depRegistered) {
            success = true
            other.addCompleteDependentCell(this)
            pool.triggerExecution(other)
          }
      }
    }
  }

  override private[rasync] def addCompleteDependentCell(dependentCell: Cell[K, V]): Unit = {
    triggerOrAddCompleteDependentCell(dependentCell)
  }

  override private[rasync] def addNextDependentCell(dependentCell: Cell[K, V]): Unit = {
    triggerOrAddNextDependentCell(dependentCell)
  }

  override private[rasync] def addCombinedDependentCell(dependentCell: Cell[K, V]): Unit = {
    // TODO Do we need to have a field for combinedDependentCells?
    // TODO Are those dependentCells removed correctly?
    triggerOrAddNextDependentCell(dependentCell)
    triggerOrAddCompleteDependentCell(dependentCell)
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
      case _: Try[_] => // completed with final result already
        true // As decided by phaller,  we ignore all updates after freeze and do not throw exceptions

      case raw: State[_, _] => // not completed
        val current = raw.asInstanceOf[State[K, V]]
        val newVal = tryJoin(current.res, value)
        if (current.res != newVal) {

          // create "staging" for dependent cells.
          // To avoid several compareAndSets, this is not moved to a different method
          val newNextDeps = current.nextDependentCells.map {
            case (c, _) => (c, NextOutcome(newVal))
          }

          val newState = new State(newVal, current.tasksActive, current.completeDependentCells, current.completeCallbacks, newNextDeps, current.nextCallbacks, current.combinedCallbacks)
          if (!state.compareAndSet(current, newState)) {
            tryNewState(value)
          } else {
            // CAS was successful, so there was a point in time where `newVal` was in the cell
            // every dependent cell should pull the new value
            onNextHandler.foreach(_.apply(Success(newVal)))
            current.nextDependentCells.foreach(_._1.updateDeps())
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
      case _: Try[_] => // was already complete
        true // As decided by phaller,  we ignore all updates after freeze and do not throw exceptions

      case (pre: State[K, V], _) =>
        pre.completeDependentCells.foreach(_.updateDeps())
        pre.nextDependentCells.foreach(_._1.updateDeps())

        onCompleteHandler.foreach(_.apply(value))
        onNextHandler.foreach(_.apply(value))

        // others do not need to pull our values any more.
        val others = pre.completeCallbacks.keys ++ pre.nextCallbacks.keys
        others.foreach(_.removeDependentCell(this))

        true
    }
    if (res) {
      pool.deregister(this)
    }
    res
  }

  @tailrec
  private[rasync] final def removeDependentCell(dependentCell: Cell[K, V]): Unit = state.get match {
    case pre: State[_, _] =>
      val current = pre.asInstanceOf[State[K, V]]
      val newCompleteDeps = current.completeDependentCells.filterNot(_ == cell)
      val newNextDeps = current.nextDependentCells - cell

      val newState = new State(current.res, current.tasksActive, newCompleteDeps, current.completeCallbacks, newNextDeps, current.nextCallbacks, current.combinedCallbacks)
      if (!state.compareAndSet(current, newState))
        removeDependentCell(dependentCell)

    case _=> /* do nothing */

  }

  @tailrec
  override private[rasync] final def removeCompleteDepentCell(cell: Cell[K, V]): Unit = {
    state.get() match {
      case pre: State[_, _] =>
        val current = pre.asInstanceOf[State[K, V]]
        val newDeps = current.completeDependentCells.filterNot(_ == cell)

        val newState = new State(current.res, current.tasksActive, newDeps, current.completeCallbacks, current.nextDependentCells, current.nextCallbacks, current.combinedCallbacks)
        if (!state.compareAndSet(current, newState))
          removeCompleteDepentCell(cell)
        else if (newDeps.isEmpty)
          nocompletedepslatch.countDown()

      case _ => /* do nothing */
    }
  }

  @tailrec
  override private[rasync] final def removeNextDepentCell(cell: Cell[K, V]): Unit = {
    state.get() match {
      case pre: State[_, _] =>
        val current = pre.asInstanceOf[State[K, V]]
        val newNextDeps = current.nextDependentCells - cell

        val newState = new State(current.res, current.tasksActive, current.completeDependentCells, current.completeCallbacks, newNextDeps, current.nextCallbacks, current.combinedCallbacks)
        if (!state.compareAndSet(current, newState))
          removeNextDepentCell(cell)
        else if (newNextDeps.isEmpty)
          nonextdepslatch.countDown()

      case _ => /* do nothing */
    }
  }

  @tailrec
  override final def removeCompleteCallbacks(cell: Cell[K, V]): Unit = {
    state.get() match {
      case pre: State[_, _] =>
        val current = pre.asInstanceOf[State[K, V]]
        val newCompleteCallbacks = current.completeCallbacks - cell

        val newState = new State(current.res, current.tasksActive, current.completeDependentCells, newCompleteCallbacks, current.nextDependentCells, current.nextCallbacks, current.combinedCallbacks)
        if (!state.compareAndSet(current, newState))
          removeCompleteCallbacks(cell)
        else {
          if (newCompleteCallbacks.isEmpty) nocompletedepslatch.countDown()
        }
      case _ => /* do nothing */
    }
  }

  @tailrec
  override final def removeNextCallbacks(cell: Cell[K, V]): Unit = {
    state.get() match {
      case pre: State[_, _] =>
        val current = pre.asInstanceOf[State[K, V]]
        val newNextCallbacks = current.nextCallbacks - cell

        val newState = new State(current.res, current.tasksActive, current.completeDependentCells, current.completeCallbacks, current.nextDependentCells, newNextCallbacks, current.combinedCallbacks)
        if (!state.compareAndSet(current, newState))
          removeNextCallbacks(cell)
        else {
          if (newNextCallbacks.isEmpty) nonextdepslatch.countDown()
        }
      case _ => /* do nothing */
    }
  }

  @tailrec
  override private[rasync] final def removeAllCallbacks(cell: Cell[K, V]): Unit = {
    state.get() match {
      case pre: State[_, _] =>
        val current = pre.asInstanceOf[State[K, V]]
        val newNextCallbacks = current.nextCallbacks - cell
        val newCompleteCallbacks = current.completeCallbacks - cell

        val newState = new State(current.res, current.tasksActive, current.completeDependentCells, newCompleteCallbacks, current.nextDependentCells, newNextCallbacks, current.combinedCallbacks)
        if (!state.compareAndSet(current, newState))
          removeAllCallbacks(cell)
        else {
          if (newNextCallbacks.isEmpty) nonextdepslatch.countDown()
          if (newCompleteCallbacks.isEmpty) nocompletedepslatch.countDown()
        }
      case _ => /* do nothing */
    }
  }

  @tailrec
  override private[rasync] final def removeAllCallbacks(cells: Seq[Cell[K, V]]): Unit = {
    state.get() match {
      case pre: State[_, _] =>
        val current = pre.asInstanceOf[State[K, V]]
        val newNextCallbacks = current.nextCallbacks -- cells
        val newCompleteCallbacks = current.completeCallbacks -- cells

        val newState = new State(current.res, current.tasksActive, current.completeDependentCells, newCompleteCallbacks, current.nextDependentCells, newNextCallbacks, current.combinedCallbacks)
        if (!state.compareAndSet(current, newState))
          removeAllCallbacks(cells)
        else {
          if (newNextCallbacks.isEmpty) nonextdepslatch.countDown()
          if (newCompleteCallbacks.isEmpty) nocompletedepslatch.countDown()
        }

      case _ => /* do nothing */
    }
  }

  override private[rasync] def waitUntilNoDeps(): Unit = {
    nocompletedepslatch.await()
  }

  override private[rasync] def waitUntilNoNextDeps(): Unit = {
    nonextdepslatch.await()
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
        val newState = new State(current.res, true, current.completeDependentCells, current.completeCallbacks, current.nextDependentCells, current.nextCallbacks, current.combinedCallbacks)
        if (!state.compareAndSet(current, newState)) setTasksActive()
        else !pre.tasksActive
      }
    case _ => false
  }

  // Schedules execution of `callback` when next intermediate result is available.
  override private[rasync] def onNext[U](callback: Function[Try[V], U]): Unit = state.get() match {
    case _: State[_, _] =>
      if (getResult() != updater.bottom) callback(Success(getResult()))
      onNextHandler =  callback :: onNextHandler
    case _ => callback(Success(getResult()))
  }

  // Schedules execution of `callback` when completed with final result.
  override def onComplete[U](callback: Function[Try[V], U]): Unit = state.get() match {
    case _: State[_, _] =>
      if (getResult() != updater.bottom) callback(Success(getResult()))
      onCompleteHandler =  callback :: onCompleteHandler
    case _ => callback(Success(getResult()))
  }

  /**
   * Tries to add the callback, if already completed, it dispatches the callback to be executed.
   *  Used by `onComplete()` to add callbacks to a promise and by `link()` to transfer callbacks
   *  to the root promise when linking two promises together.
   */
  @tailrec
  private def triggerOrAddCompleteDependentCell(dependentCell: Cell[K, V]): Unit = state.get() match {
    case r: Try[_] =>
      dependentCell.updateDeps()
    // case _: DefaultPromise[_] => compressedRoot().triggerOrAddCompleteDependentCell(runnable)
    case pre: State[_, _] =>
      // assemble new state
      val current = pre.asInstanceOf[State[K, V]]
      val newState = new State(current.res, current.tasksActive, dependentCell :: current.completeDependentCells, current.completeCallbacks, current.nextDependentCells, current.nextCallbacks, current.combinedCallbacks)
      if (!state.compareAndSet(pre, newState))
        triggerOrAddCompleteDependentCell(dependentCell)
  }

  private[rasync] def getStagedValueFor(dependentCell: Cell[K, V]): Outcome[V] = state.get() match {
    case r: Try[_] =>
      // We will infinitely return FinalOutcome
      // so the dependent cell needs to remove the dependency, once
      // the first FinalOutcome is seen.
      // An alternative implementation was
      // (1) to return "NoOutcome" here, but that would require the value passed to the callback runnable to be fixed, which could lead to more steps in the dependent cell
      // (2) store the staging even after the cell is completed.
      // Note that in the current implementation, no NoSuchElementException can be thrown as opposed to the non-final State[_, _].
      FinalOutcome(r.asInstanceOf[Try[V]].get)
    case pre: State[_, _] =>
      // assemble new state
      val current = pre.asInstanceOf[State[K, V]]
      try
        current.nextDependentCells(dependentCell) match {
          case NoOutcome => NoOutcome /* just return that no new value is available. Own state does not need to be changed. */
          case v @ Outcome(_) =>
            /* Return v but clear staging before. */

            // NoOutcome signals, that the staged value has been pulled.
            val newNextDependentCells = current.nextDependentCells + (dependentCell -> NoOutcome)
            val newState = new State(current.res, current.tasksActive, current.completeDependentCells, current.completeCallbacks, newNextDependentCells, current.nextCallbacks, current.combinedCallbacks)
            if (!state.compareAndSet(current, newState)) getStagedValueFor(dependentCell) // try again
            else v

        }
      catch {
        case _: NoSuchElementException =>
          throw new NoSuchElementException(s"$dependentCell asked for a value of $this but was not registered as dependent cell.")
      }
  }

  private[rasync] def hasStagedValueFor(dependentCell: Cell[K, V]): Outcome[V] = state.get() match {
    case r: Try[_] =>
      // We will infinitely return FinalOutcome
      // so the dependent cell needs to remove the dependency, once
      // the first FinalOutcome is seen.
      // An alternative implementation was
      // (1) to return "NoOutcome" here, but that would require the value passed to the callback runnable to be fixed, which could lead to more steps in the dependent cell
      // (2) store the staging even after the cell is completed.
      FinalOutcome(r.asInstanceOf[Try[V]].get)
    case pre: State[_, _] =>
      try {
        val current = pre.asInstanceOf[State[K, V]]
        val result = current.nextDependentCells(dependentCell)
        result // TODO Is this always a NextOutcome or NoOutcome? Would a FinalOutcome be OK?
      } catch {
        case _: NoSuchElementException =>
          throw new NoSuchElementException(s"$dependentCell asked for a value of $this but was not registered as dependent cell.")
      }
  }

  /**
   * Tries to add the callback, if already completed, it dispatches the callback to be executed.
   *  Used by `onNext()` to add callbacks to a promise and by `link()` to transfer callbacks
   *  to the root promise when linking two promises together.
   */
  @tailrec
  private def triggerOrAddNextDependentCell(dependentCell: Cell[K, V]): Unit =
    state.get() match {
      case r: Try[_] =>
        dependentCell.updateDeps()
      // case _: DefaultPromise[_] => compressedRoot().triggerOrAddCompleteDependentCell(runnable)
      case pre: State[_, _] =>
        // assemble new state
        val current = pre.asInstanceOf[State[K, V]]
        val staged: Outcome[V] =
          if (current.res != updater.bottom) NextOutcome(current.res)
          else NoOutcome
        val newState = new State(current.res, current.tasksActive, current.completeDependentCells, current.completeCallbacks, current.nextDependentCells + (dependentCell -> staged), current.nextCallbacks, current.combinedCallbacks)
        if (!state.compareAndSet(pre, newState))
          triggerOrAddNextDependentCell(dependentCell)
        else if (staged != NoOutcome) dependentCell.updateDeps()
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

  override private[rasync] def updateDeps(): Unit = state.get() match {
    case _: State[_, _] =>
      // eventually check on all cells that we depend on for new values
      pool.execute(() => state.get() match {
        case state: State[_, _] =>
          state.completeCallbacks.foreach { _._2.execute() }
          state.nextCallbacks.foreach { _._2.execute() }
        case _: Try[_] => /* We are final already, so we ignore all incoming information. */
      })
    case _: Try[_] => /* We are final already, so we ignore all incoming information. */
  }

  /**
   * Checks if this cell is a dependee of some other cells. This is true if some cells called
   * whenNext[Sequential / Complete](thisCell, f)
   * @return True if some cells depends on this one, false otherwise
   */
  override def isADependee(): Boolean = {
    numCompleteCallbacks > 0 || numNextCallbacks > 0
  }

}
