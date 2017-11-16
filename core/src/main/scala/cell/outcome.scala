package cell

/** Use this trait in callbacks to return the new value of a cell.
  * `NextOutcome(v)` and `FinalOutcome(v)` put the value `v` to
  * the cell, in the latter case, the cell is completed.
  * Use `NoOutcome` to indicate that no progress is possible.
  */
sealed trait WhenNextOutcome[+V]
case class NextOutcome[+V](x: V) extends WhenNextOutcome[V]
case class FinalOutcome[+V](x: V) extends WhenNextOutcome[V]
case object NoOutcome extends WhenNextOutcome[Nothing]

object WhenNextOutcome {

  /** Returns a `NextOutcome(v)` or `FinalOutcome(v)` object. */
  def apply[V](v: V, isFinal: Boolean): WhenNextOutcome[V] = {
    if (isFinal) FinalOutcome(v)
    else NextOutcome(v)
  }

  /** Returns a `NextOutcome(v)` or `FinalOutcome(v)` or `NoOutcome` object.
    *
    * If `v` is `None`, `NoOutcome` is returned. Otherwise, `NextOutcome(v)` or
    * `FinalOutcome(v)` is returned depending on `isFinal`.
    *
    * @param v Option of a new value.
    * @param isFinal Indicates if the value is final.
    * @return Returns a `NextOutcome(v)` or `FinalOutcome(v)` or `NoOutcome` object.
    */
  def apply[V](v: Option[V], isFinal: Boolean): WhenNextOutcome[V] = {
    if (isFinal) v.map(FinalOutcome(_)).getOrElse(NoOutcome)
    else v.map(NextOutcome(_)).getOrElse(NoOutcome)
  }

  /** Match non-empty outcomes. */
  def unapply[V](arg: WhenNextOutcome[V]): Option[(V, Boolean)] = arg match {
    case FinalOutcome(v) => Some(v, true)
    case NextOutcome(v) => Some(v, false)
    case _ => None
  }
}
