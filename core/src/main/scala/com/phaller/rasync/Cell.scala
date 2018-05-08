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

  private[rasync] def numNextDependentCells: Int
  private[rasync] def numCompletDependentCells: Int

  private[rasync] def addCompleteDependentCell(dependentCell: Cell[K, V]): Unit
  private[rasync] def addNextDependentCell(dependentCell: Cell[K, V]): Unit
  private[rasync] def addCombinedDependentCell(dependentCell: Cell[K, V]): Unit


  private[rasync] def removeDependentCell(dependentCell: Cell[K, V]): Unit

  private[rasync] def resolveWithValue(value: V, dontCall: Seq[Cell[K, V]]): Unit
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
            completer.tryComplete(f.asInstanceOf[Failure[List[V]]], None)
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
                completer.tryComplete(f, None)
            }
          case f @ Failure(_) =>
            completer.tryComplete(f.asInstanceOf[Failure[List[V]]], None)
        }
    }
    completer.cell
  }

}

private class State[V]

/* State of a cell that is not yet completed.
 *
 * This is not a case class, since it is important that equality is by-reference.
 *
 * @param res       current intermediate result (optional)
 * @param deps      dependent Cells + a staged value for this cell (this is not needed for completeDeps, the staged value is always the final one)
 * @param callbacks those callbacks are run if the cell t(hat we depend on) changes.
 */
private class IntermediateState[K <: Key[V], V](
     val res: V,
     val tasksActive: Boolean,
     val completeDependentCells: List[Cell[K, V]],
     val completeCallbacks: Map[Cell[K, V], CompleteCallbackRunnable[K, V]],
     val dependentCells: Map[Cell[K, V], Outcome[V]],
     val nextCallbacks: Map[Cell[K, V], NextCallbackRunnable[K, V]],
     val combinedCallbacks: Map[Cell[K, V], CombinedCallbackRunnable[K, V]]
) extends State[V]

private object IntermediateState {
  def empty[K <: Key[V], V](updater: Updater[V]): IntermediateState[K, V] =
    new IntermediateState[K, V](updater.bottom, false, List(), Map(), Map(), Map(), Map())
}

private class FinalState[K <: Key[V], V] (
     val res: Try[V],
     val dependentCells: List[Cell[K, V]])
  extends State[V]

private class CellImpl[K <: Key[V], V](pool: HandlerPool, val key: K, updater: Updater[V], val init: (Cell[K, V]) => Outcome[V]) extends Cell[K, V] with CellCompleter[K, V] {

  override val completer: CellCompleter[K, V] = this.asInstanceOf[CellCompleter[K, V]]

  implicit val ctx = pool

  private val nocompletedepslatch = new CountDownLatch(1)
  private val nonextdepslatch = new CountDownLatch(1)

  /* Contains a value either of type
   * (a) (`Try[V]`, Map[Cell, Outcome])      for the final result, or
   * (b) `State[K,V]`                         for an incomplete state.
   *
   * In the former case, the map is used to store "staged values"
   * for dependent cells. One could use
   *
   */
  private val state = new AtomicReference[State[V]](IntermediateState.empty[K, V](updater))

  private var onCompleteHandler: List[Try[V] => Any] = List()
  private var onNextHandler: List[Try[V] => Any] = List()

  // `CellCompleter` and corresponding `Cell` are the same run-time object.
  override def cell: Cell[K, V] = this

  override def getResult(): V = state.get() match {
    case finalRes: FinalState[K, V] =>
      finalRes.res match {
        case Success(result) => result
        case Failure(err) => throw new IllegalStateException(err)
      }
    case raw: IntermediateState[K, V] => raw.res
  }

  override def trigger(): Unit = {
    pool.triggerExecution(this)
  }

  override def isComplete: Boolean = state.get match {
    case _: FinalState[K, V] => true
    case _ => false
  }

  override def putFinal(x: V): Unit = {
    val res = tryComplete(Success(x), None)
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
            completer.tryComplete(f.asInstanceOf[Try[(V, V)]], None)
        }
      case f @ Failure(_) =>
        completer.tryComplete(f.asInstanceOf[Try[(V, V)]], None)
    }
    completer.cell
  }

  private[this] def currentState(): IntermediateState[K, V] =
    state.get() match {
      case finalRes: FinalState[K, V] => // completed with final result
        null
      case pre: IntermediateState[_, _] => // not completed
        pre.asInstanceOf[IntermediateState[K, V]]
    }

  override private[rasync] def numCompleteDependencies: Int = {
    val current = currentState()
    if (current == null) 0
    else (current.completeCallbacks.keys ++ current.combinedCallbacks.keys).toSet.size
  }

  override private[rasync] def numNextDependencies: Int = {
    val current = currentState()
    if (current == null) 0
    else (current.nextCallbacks.keys ++ current.combinedCallbacks.keys).toSet.size
  }

  override private[rasync] def numTotalDependencies: Int = {
    val current = currentState()
    if (current == null) 0
    else (current.completeCallbacks.keys ++ current.nextCallbacks.keys ++ current.combinedCallbacks.keys).toSet.size
  }

  override def completeCellDependencies: Seq[Cell[K, V]] = {
    state.get() match {
      case finalRes: FinalState[K, V] => // completed with final result
        Seq[Cell[K, V]]()
      case pre: IntermediateState[_, _] => // not completed
        val current = pre.asInstanceOf[IntermediateState[K, V]]
        current.completeCallbacks.keys.toSeq
    }
  }

  override def totalCellDependencies: Seq[Cell[K, V]] = {
    state.get() match {
      case finalRes: FinalState[K, V] => // completed with final result
        Seq[Cell[K, V]]()
      case pre: IntermediateState[_, _] => // not completed
        val current = pre.asInstanceOf[IntermediateState[K, V]]
        (current.completeCallbacks.keys ++ current.nextCallbacks.keys ++ current.combinedCallbacks.keys).toSeq
    }
  }

  override def isIndependent(): Boolean = {
    state.get() match {
      case finalRes: FinalState[K, V] => // completed with final result
        true
      case pre: IntermediateState[_, _] => // not completed
        val current = pre.asInstanceOf[IntermediateState[K, V]]
        current.combinedCallbacks.isEmpty && current.combinedCallbacks.isEmpty && current.nextCallbacks.isEmpty
    }
  }

  override def numNextDependentCells: Int = {
    state.get() match {
      case finalRes: FinalState[K, V] => // completed with final result
        0
      case pre: IntermediateState[_, _] => // not completed
        val current = pre.asInstanceOf[IntermediateState[K, V]]
        current.dependentCells.keys.size
    }
  }

  override def numCompletDependentCells: Int = {
    state.get() match {
      case finalRes: FinalState[K, V] => // completed with final result
        0
      case pre: IntermediateState[_, _] => // not completed
        val current = pre.asInstanceOf[IntermediateState[K, V]]
        current.completeDependentCells.size
    }
  }

  override private[rasync] def resolveWithValue(value: V, dontCall: Seq[Cell[K, V]]): Unit = {
    val res = tryComplete(Success(value), Some(dontCall))
    if (!res)
      throw new IllegalStateException("Cell already completed.")
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
        case finalRes: FinalState[K, V] => // completed with final result
          // do not add dependency
          // in fact, do nothing
          success = true

        case raw: IntermediateState[_, _] => // not completed
          val current = raw.asInstanceOf[IntermediateState[K, V]]
          val depRegistered =
            if (current.combinedCallbacks.contains(other)) true
            else {
              val newCallback: CombinedCallbackRunnable[K, V] =
                if (sequential) new CombinedSequentialCallbackRunnable(pool, this, other, valueCallback)
                else new CombinedConcurrentCallbackRunnable(pool, this, other, valueCallback)

              val newState = new IntermediateState(current.res, current.tasksActive, current.completeDependentCells, current.completeCallbacks, current.dependentCells, current.nextCallbacks, current.combinedCallbacks  + (other -> newCallback) )
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
        case finalRes: FinalState[K, V] => // completed with final result
          // do not add dependency
          // in fact, do nothing
          success = true

        case raw: IntermediateState[_, _] => // not completed
          val current = raw.asInstanceOf[IntermediateState[K, V]]
          val depRegistered =
            if (current.nextCallbacks.contains(other)) true
            else {
              val newCallback: NextCallbackRunnable[K, V] =
                if (sequential) new NextSequentialCallbackRunnable(pool, this, other, valueCallback)
                else new NextConcurrentCallbackRunnable(pool, this, other, valueCallback)

              val newState = new IntermediateState(current.res, current.tasksActive, current.completeDependentCells, current.completeCallbacks, current.dependentCells, current.nextCallbacks + (other -> newCallback ), current.combinedCallbacks)
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
        case _: FinalState[K, V] => // completed with final result
          // do not add dependency
          // in fact, do nothing
          success = true

        case raw: IntermediateState[_, _] => // not completed
          val current = raw.asInstanceOf[IntermediateState[K, V]]
          val depRegistered =
            if (current.completeCallbacks.contains(other)) true
            else {
              val newCallback: CompleteCallbackRunnable[K, V] =
                if (sequential) new CompleteSequentialCallbackRunnable(pool, this, other, valueCallback)
                else new CompleteConcurrentCallbackRunnable(pool, this, other, valueCallback)

              val newState = new IntermediateState(current.res, current.tasksActive, current.completeDependentCells, current.completeCallbacks + (other -> newCallback ), current.dependentCells, current.nextCallbacks, current.combinedCallbacks)
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
      case _: FinalState[K, V] => // completed with final result already
        true // As decided by phaller,  we ignore all updates after freeze and do not throw exceptions

      case raw: IntermediateState[_, _] => // not completed
        val current = raw.asInstanceOf[IntermediateState[K, V]]
        val newVal = tryJoin(current.res, value)
        if (current.res != newVal) {

          // create "staging" for dependent cells.
          // To avoid several compareAndSets, this is not moved to a different method
          val newNextDeps = current.dependentCells.map {
            case (c, _) => (c, NextOutcome(newVal))
          }

          val newState = new IntermediateState(newVal, current.tasksActive, current.completeDependentCells, current.completeCallbacks, newNextDeps, current.nextCallbacks, current.combinedCallbacks)
          if (!state.compareAndSet(current, newState)) {
            tryNewState(value)
          } else {
            // CAS was successful, so there was a point in time where `newVal` was in the cell
            // every dependent cell should pull the new value
            onNextHandler.foreach(_.apply(Success(newVal)))
            current.dependentCells.foreach(_._1.updateDeps())
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
      case current: IntermediateState[_, _] =>
        val currentState = current.asInstanceOf[IntermediateState[K, V]]
        val newVal = tryJoin(currentState.res, v.get)
        val newDependentCells = currentState.dependentCells.keys.toList ++ currentState.completeDependentCells // TODO Can we merge nextDependentCells and completeDependentCells? I guess so, but maybe tests need to be adapted.
        val newState = new FinalState(Success(newVal), newDependentCells)
        if (state.compareAndSet(current, newState))
          (currentState, newState)
        else
          tryCompleteAndGetState(v)

      case finalRes: FinalState[K, V] => finalRes
    }
  }

  override def tryComplete(value: Try[V], dontCall: Option[Seq[Cell[K, V]]]): Boolean = {
    val resolved: Try[V] = resolveTry(value)

    // the only call to `tryCompleteAndGetState`
    val res = tryCompleteAndGetState(resolved) match {
      case _: FinalState[K, V] => // was already complete
        true // As decided by phaller,  we ignore all updates after freeze and do not throw exceptions

      case (pre: IntermediateState[K, V], finalValue: FinalState[K, V]) =>

        dontCall match {
          case Some(cells) =>
            pre.dependentCells.foreach(c => if (!cells.contains(c._1)) c._1.updateDeps())
            pre.completeDependentCells.foreach(c => if (!cells.contains(c)) c.updateDeps())
          case None =>
            pre.dependentCells.foreach(_._1.updateDeps())
            pre.completeDependentCells.foreach(_.updateDeps())
        }

        onCompleteHandler.foreach(_.apply(finalValue.res))
        onNextHandler.foreach(_.apply(finalValue.res))

        // others do not need to pull our values any more.
        val others = pre.completeCallbacks.keys ++ pre.nextCallbacks.keys ++ pre.combinedCallbacks.keys
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
    case pre: IntermediateState[_, _] =>
      val current = pre.asInstanceOf[IntermediateState[K, V]]
      val newCompleteDeps = current.completeDependentCells.filterNot(_ == dependentCell)
      val newNextDeps = current.dependentCells - dependentCell

      val newState = new IntermediateState(current.res, current.tasksActive, newCompleteDeps, current.completeCallbacks, newNextDeps, current.nextCallbacks, current.combinedCallbacks)
      if (!state.compareAndSet(current, newState))
        removeDependentCell(dependentCell)

    case _=> /* do nothing */

  }

  @tailrec
  override private[rasync] final def removeCompleteDepentCell(dependentCell: Cell[K, V]): Unit = {
    state.get() match {
      case pre: IntermediateState[_, _] =>
        val current = pre.asInstanceOf[IntermediateState[K, V]]
        val newDeps = current.completeDependentCells.filterNot(_ == dependentCell)

        val newState = new IntermediateState(current.res, current.tasksActive, newDeps, current.completeCallbacks, current.dependentCells, current.nextCallbacks, current.combinedCallbacks)
        if (!state.compareAndSet(current, newState))
          removeCompleteDepentCell(cell)
        else if (newDeps.isEmpty)
          nocompletedepslatch.countDown()

      case _ => /* do nothing */
    }
  }

  @tailrec
  override private[rasync] final def removeNextDepentCell(dependentCell: Cell[K, V]): Unit = {
    state.get() match {
      case pre: IntermediateState[_, _] =>
        val current = pre.asInstanceOf[IntermediateState[K, V]]
        val newNextDeps = current.dependentCells - dependentCell

        val newState = new IntermediateState(current.res, current.tasksActive, current.completeDependentCells, current.completeCallbacks, newNextDeps, current.nextCallbacks, current.combinedCallbacks)
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
      case pre: IntermediateState[_, _] =>
        val current = pre.asInstanceOf[IntermediateState[K, V]]
        val newCompleteCallbacks = current.completeCallbacks - cell

        val newState = new IntermediateState(current.res, current.tasksActive, current.completeDependentCells, newCompleteCallbacks, current.dependentCells, current.nextCallbacks, current.combinedCallbacks)
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
      case pre: IntermediateState[_, _] =>
        val current = pre.asInstanceOf[IntermediateState[K, V]]
        val newNextCallbacks = current.nextCallbacks - cell

        val newState = new IntermediateState(current.res, current.tasksActive, current.completeDependentCells, current.completeCallbacks, current.dependentCells, newNextCallbacks, current.combinedCallbacks)
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
      case pre: IntermediateState[_, _] =>
        val current = pre.asInstanceOf[IntermediateState[K, V]]
        val newNextCallbacks = current.nextCallbacks - cell
        val newCompleteCallbacks = current.completeCallbacks - cell

        val newState = new IntermediateState(current.res, current.tasksActive, current.completeDependentCells, newCompleteCallbacks, current.dependentCells, newNextCallbacks, current.combinedCallbacks)
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
      case pre: IntermediateState[_, _] =>
        val current = pre.asInstanceOf[IntermediateState[K, V]]
        val newNextCallbacks = current.nextCallbacks -- cells
        val newCompleteCallbacks = current.completeCallbacks -- cells

        val newState = new IntermediateState(current.res, current.tasksActive, current.completeDependentCells, newCompleteCallbacks, current.dependentCells, newNextCallbacks, current.combinedCallbacks)
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
    case _: FinalState[K, V] => false
    case s: IntermediateState[_, _] => s.tasksActive
  }

  /**
   * Mark this cell as "running".
   *
   * @return Returns true, iff the cell's status changed (i.e. it had not been running before).
   */
  @tailrec
  override private[rasync] final def setTasksActive(): Boolean = state.get() match {
    case pre: IntermediateState[_, _] =>
      if (pre.tasksActive)
        false
      else {
        val current = pre.asInstanceOf[IntermediateState[K, V]]
        val newState = new IntermediateState(current.res, true, current.completeDependentCells, current.completeCallbacks, current.dependentCells, current.nextCallbacks, current.combinedCallbacks)
        if (!state.compareAndSet(current, newState)) setTasksActive()
        else !pre.tasksActive
      }
    case _ => false
  }

  // Schedules execution of `callback` when next intermediate result is available.
  override private[rasync] def onNext[U](callback: Function[Try[V], U]): Unit = state.get() match {
    case _: IntermediateState[_, _] =>
      if (getResult() != updater.bottom) callback(Success(getResult()))
      onNextHandler =  callback :: onNextHandler
    case _ => callback(Success(getResult()))
  }

  // Schedules execution of `callback` when completed with final result.
  override def onComplete[U](callback: Function[Try[V], U]): Unit = state.get() match {
    case _: IntermediateState[_, _] =>
      onCompleteHandler =  callback :: onCompleteHandler
    case _ =>
      callback(Success(getResult()))
  }


  private[rasync] def getStagedValueFor(dependentCell: Cell[K, V]): Outcome[V] = state.get() match {
    case pre: FinalState[K, V] =>
      // assemble new state
      val current = pre.asInstanceOf[FinalState[K, V]]

        if (current.dependentCells.contains(dependentCell)) {
          /* Return v but clear staging before. This avoids repeated invocations of the same callback later. */

          val newNextDependentCells = current.dependentCells.filterNot(_ == dependentCell)
          val newState = new FinalState(current.res, newNextDependentCells)
          if (!state.compareAndSet(current, newState)) getStagedValueFor(dependentCell) // try again
          else FinalOutcome(current.res.get)
        }
        else NoOutcome

    case pre: IntermediateState[_, _] =>
      // assemble new state
      val current = pre.asInstanceOf[IntermediateState[K, V]]
      try
        current.dependentCells(dependentCell) match {
          case v @ Outcome(_) =>
            /* Return v but clear staging before. */

            // NoOutcome signals, that the staged value has been pulled.
            val newNextDependentCells = current.dependentCells + (dependentCell -> NoOutcome)
            val newState = new IntermediateState(current.res, current.tasksActive, current.completeDependentCells, current.completeCallbacks, newNextDependentCells, current.nextCallbacks, current.combinedCallbacks)
            if (!state.compareAndSet(current, newState)) getStagedValueFor(dependentCell) // try again
            else v
          case NoOutcome => NoOutcome /* just return that no new value is available. Own state does not need to be changed. */
        }
      catch {
        case _: NoSuchElementException =>
          if (current.res != updater.bottom)
            NextOutcome(current.res)
          else NoOutcome
      }
  }

  private[rasync] def hasStagedValueFor(dependentCell: Cell[K, V]): Outcome[V] = state.get() match {
    case pre: FinalState[K, V] =>
      val current = pre.asInstanceOf[FinalState[K, V]]
      try {
        if( current.dependentCells.contains(dependentCell))
          FinalOutcome(current.res.get)
        else NoOutcome
      } catch {
        case _: NoSuchElementException =>
          if (current.res != updater.bottom)
            FinalOutcome(current.res.get)
          else NoOutcome
      }
    case pre: IntermediateState[_, _] =>
      val current = pre.asInstanceOf[IntermediateState[K, V]]
      try {
        val result = current.dependentCells(dependentCell)
        // TODO Is this always a NextOutcome or NoOutcome? Would a FinalOutcome be OK?
        result
      } catch {
        case _: NoSuchElementException =>
          if (current.res != updater.bottom)
            NextOutcome(current.res)
          else NoOutcome
      }

  }


  /**
    * Tries to add the callback, if already completed, it dispatches the callback to be executed.
    *  Used by `onComplete()` to add callbacks to a promise and by `link()` to transfer callbacks
    *  to the root promise when linking two promises together.
    */
  @tailrec
  private def triggerOrAddCompleteDependentCell(dependentCell: Cell[K, V]): Unit = state.get() match {
    case pre: FinalState[K, V] =>
      // depdentCell can update its deps immediately, but
      // we need to stage a value first. (Otherwise, it would
      // receive a NoOutcome via getStagedValueFor(dependentCell))
      val current = pre.asInstanceOf[FinalState[K, V]]
      val newState = new FinalState(current.res, dependentCell :: current.dependentCells)
      if (!state.compareAndSet(pre, newState))
        triggerOrAddCompleteDependentCell(dependentCell)
      else
        dependentCell.updateDeps()
    case pre: IntermediateState[_, _] =>
      // assemble new state
      val current = pre.asInstanceOf[IntermediateState[K, V]]
      val newState = new IntermediateState(current.res, current.tasksActive, dependentCell :: current.completeDependentCells, current.completeCallbacks, current.dependentCells, current.nextCallbacks, current.combinedCallbacks)
      if (!state.compareAndSet(pre, newState))
        triggerOrAddCompleteDependentCell(dependentCell)
  }

  /**
   * Tries to add the callback, if already completed, it dispatches the callback to be executed.
   *  Used by `onNext()` to add callbacks to a promise and by `link()` to transfer callbacks
   *  to the root promise when linking two promises together.
   */
  @tailrec
  private def triggerOrAddNextDependentCell(dependentCell: Cell[K, V]): Unit =
    state.get() match {
      case pre: FinalState[K, V] =>
        // depdentCell can update its deps immediately, but
        // we need to stage a value first. (Otherwise, it would
        // receive a NoOutcome via getStagedValueFor(dependentCell))
        val current = pre.asInstanceOf[FinalState[K, V]]
        val newDepCells = current.dependentCells.filterNot(_ == dependentCell)
        val newState = new FinalState(pre.res, newDepCells)
        if (!state.compareAndSet(pre, newState))
          triggerOrAddNextDependentCell(dependentCell)
        else
          dependentCell.updateDeps()
      case pre: IntermediateState[_, _] =>
        // assemble new state
        val current = pre.asInstanceOf[IntermediateState[K, V]]
        val staged: Outcome[V] =
          if (current.res != updater.bottom) NextOutcome(current.res)
          else NoOutcome
        val newState = new IntermediateState(current.res, current.tasksActive, current.completeDependentCells, current.completeCallbacks, current.dependentCells + (dependentCell -> staged), current.nextCallbacks, current.combinedCallbacks)
        if (!state.compareAndSet(pre, newState))
          triggerOrAddNextDependentCell(dependentCell)
        else
          if (staged != NoOutcome) dependentCell.updateDeps()
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
    case _: IntermediateState[_, _] =>
      // eventually check on all cells that we depend on for new values
      pool.execute(() => state.get() match {
        case state: IntermediateState[_, _] =>
          state.completeCallbacks.foreach { _._2.execute() }
          state.nextCallbacks.foreach { _._2.execute() }
          state.combinedCallbacks.foreach { _._2.execute() }
        case _: FinalState[K, V] => /* We are final already, so we ignore all incoming information. */
      })
    case _: FinalState[K, V] => /* We are final already, so we ignore all incoming information. */
  }

  /**
   * Checks if this cell is a dependee of some other cells. This is true if some cells called
   * whenNext[Sequential / Complete](thisCell, f)
   * @return True if some cells depends on this one, false otherwise
   */
  override def isADependee(): Boolean = {
    numCompletDependentCells > 0 || numNextDependentCells > 0
  }

}
