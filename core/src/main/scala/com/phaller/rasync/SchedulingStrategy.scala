package com.phaller.rasync

import com.phaller.rasync.lattice.Key

trait SchedulingStrategy {
  def calcPriority[K <: Key[V], V](dependentCell: Cell[K, V], other: Cell[K, V]): Int
  def calcPriority[K <: Key[V], V](dependentCell: Cell[K, V]): Int

}

object DefaultScheduling extends SchedulingStrategy {
  override def calcPriority[K <: Key[V], V](dependentCell: Cell[K, V], other: Cell[K, V]): Int = 0
  override def calcPriority[K <: Key[V], V](cell: Cell[K, V]): Int = 0
}

object OthersWithManySuccessorsFirst extends SchedulingStrategy {
  override def calcPriority[K <: Key[V], V](dependentCell: Cell[K, V], other: Cell[K, V]): Int =
    -(other.numNextDependentCells + other.numCompleteDependentCells)

  override def calcPriority[K <: Key[V], V](cell: Cell[K, V]): Int =
    -(cell.numNextDependentCells + cell.numCompleteDependentCells)
}

object OthersWithManySuccessorsLast extends SchedulingStrategy {
  override def calcPriority[K <: Key[V], V](dependentCell: Cell[K, V], other: Cell[K, V]): Int =
    other.numNextDependentCells + other.numCompleteDependentCells

  override def calcPriority[K <: Key[V], V](cell: Cell[K, V]): Int =
    cell.numNextDependentCells + cell.numCompleteDependentCells
}

object CellsWithManyPredecessorsFirst extends SchedulingStrategy {
  override def calcPriority[K <: Key[V], V](dependentCell: Cell[K, V], other: Cell[K, V]): Int =
    -dependentCell.totalCellDependencies.size

  override def calcPriority[K <: Key[V], V](cell: Cell[K, V]): Int = 0
}

object CellsWithManyPredecessorsLast extends SchedulingStrategy {
  override def calcPriority[K <: Key[V], V](dependentCell: Cell[K, V], other: Cell[K, V]): Int =
    dependentCell.totalCellDependencies.size

  override def calcPriority[K <: Key[V], V](dependentCell: Cell[K, V]): Int = 0
}