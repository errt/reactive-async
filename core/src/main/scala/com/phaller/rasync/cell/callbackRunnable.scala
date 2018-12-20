package com.phaller.rasync
package cell

import java.util.concurrent.atomic.AtomicReference

import pool.HandlerPool

import scala.annotation.tailrec
import scala.concurrent.OnCompleteRunnable
import scala.util.{ Failure, Success, Try }

/**
 * CallbackRunnables are tasks that need to be run, when a value of a cell changes, that
 * some completer depends on.
 *
 * CallbackRunnables store information about whether the dependency has been registered via whenComplete/whenNext
 * and when/whenSequential and the involved cells/completers.
 *
 * Run a callback in a handler pool, if a value in a cell changes.
 * Call execute() to add the callback to the given HandlerPool.
 */
private[rasync] trait CallbackRunnable[V] extends Runnable with OnCompleteRunnable {
  val pool: HandlerPool[V]

  val dependentCompleter: CellCompleter[V]

  /** The cell that triggers the callback. */
  val dependees: Iterable[Cell[V]]

  protected val sequential: Boolean

  /** The callback to be called. It retrieves an updated value of otherCell and returns an Outcome for dependentCompleter. */
  protected val callback: Iterable[(Cell[V], Try[ValueOutcome[V]])] => Outcome[V]

  private val updatedDependees = new AtomicReference[Set[Cell[V]]](Set.empty)

  @tailrec
  final def addUpdate(other: Cell[V]): Unit = {
    val oldUpdatedDependees = updatedDependees.get
    val newUpdatedDependees = oldUpdatedDependees + other
    if (updatedDependees.compareAndSet(oldUpdatedDependees, newUpdatedDependees)) {
      // The first incoming update (since the last execution) starts this runnable.
      // Other cells might still be added to updatedDependees concurrently, the runnable
      // will collect all updates and forward them altogether.
      if (oldUpdatedDependees.isEmpty)
        pool.execute(this, pool.schedulingStrategy.calcPriority(dependentCompleter.cell, other))
    } else addUpdate(other) // retry
    // TODO Do we have to avoid propagations after final propagations?
  }

  /** Call the callback and use update dependentCompleter according to the callback's result. */
  def run(): Unit = {
    if (sequential) {
      dependentCompleter.sequential {
        callCallback()
      }
    } else {
      callCallback()
    }
  }

  protected def callCallback(): Unit = {
    if (!dependentCompleter.cell.isComplete) {

      try {
        // remove all updates form the list of updates that need to be handled – they will now be handled
        val dependees = updatedDependees.getAndSet(Set.empty)
        val propagations = dependees.map(c => (c, c.getState()))

        val depRemoved = // see below for depRemoved
          callback(propagations) match {
            case NextOutcome(v) =>
              dependentCompleter.putNext(v)
              false
            case FinalOutcome(v) =>
              dependentCompleter.putFinal(v)
              true
            case FreezeOutcome =>
              dependentCompleter.freeze()
              true
            case NoOutcome => false /* do nothing */
          }
        // if the dependency has not been removed yet,
        // we can remove it, if a FinalOutcome has been propagted
        // or a Failuare has been propagated, i.e. the dependee had been completed
        if (!depRemoved) {
          val toRemove = propagations.filter({
            case (_, Success(NextOutcome(_))) => false
            case _ => true
          }).map(_._1)
          dependentCompleter.cell.removeDependeeCells(toRemove)
        }
      } catch {
        // An exception thrown in a callback is stored as  the final value for the depender
        case e: Exception =>
          dependentCompleter.putFailure(Failure(e))
      }
    }
  }
}

/**
 * Run a callback concurrently, if a value in a cell changes.
 * Call execute() to add the callback to the given HandlerPool.
 */
private[rasync] class ConcurrentCallbackRunnable[V](override val pool: HandlerPool[V], override val dependentCompleter: CellCompleter[V], override val dependees: Iterable[Cell[V]], override val callback: Iterable[(Cell[V], Try[ValueOutcome[V]])] => Outcome[V]) extends CallbackRunnable[V] {
  override protected final val sequential: Boolean = false
}

/**
 * Run a callback sequentially (for a dependent cell), if a value in another cell changes.
 * Call execute() to add the callback to the given HandlerPool.
 */
private[rasync] class SequentialCallbackRunnable[V](override val pool: HandlerPool[V], override val dependentCompleter: CellCompleter[V], override val dependees: Iterable[Cell[V]], override val callback: Iterable[(Cell[V], Try[ValueOutcome[V]])] => Outcome[V]) extends CallbackRunnable[V] {
  override protected final val sequential: Boolean = true
}
