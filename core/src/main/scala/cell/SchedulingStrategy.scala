package cell

import lattice.Key

sealed trait SchedulingStrategy {
  def calcPriority[K <: Key[V], V](cell: Cell[K, V], other: Cell[K, V]): Int
}

object DefaultScheduling extends SchedulingStrategy {
  def calcPriority[K <: Key[V], V](cell: Cell[K, V], other: Cell[K, V]): Int = 0
}

object OthersWithManySuccessorsFirst extends SchedulingStrategy {
  override def calcPriority[K <: Key[V], V](cell: Cell[K, V], other: Cell[K, V]): Int =
    -other.numNextCallbacks
}

object OthersWithManySuccessorsLast extends SchedulingStrategy {
  override def calcPriority[K <: Key[V], V](cell: Cell[K, V], other: Cell[K, V]): Int =
    other.numNextCallbacks
}

object CellsWithManyPredecessorsFirst extends SchedulingStrategy {
  override def calcPriority[K <: Key[V], V](cell: Cell[K, V], other: Cell[K, V]): Int =
    -cell.totalCellDependencies.size
}

object CellsWithManyPredecessorsLast extends SchedulingStrategy {
  override def calcPriority[K <: Key[V], V](cell: Cell[K, V], other: Cell[K, V]): Int =
    cell.totalCellDependencies.size
}