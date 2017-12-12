package cell

import java.util
import java.util.concurrent.{TimeUnit, TimeUnit => _, _}
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec
import scala.util.control.NonFatal
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import lattice.{DefaultKey, Key, Lattice}
import org.opalj.graphs._

/* Need to have reference equality for CAS.
 */
private class PoolState(val handlers: List[() => Unit] = List(), val submittedTasks: Int = 0) {
  def isQuiescent(): Boolean =
    submittedTasks == 0
}

trait PriorityV {
  def priority: Int
}

trait PriorityRunnable extends Runnable with PriorityV

private class GroupedBlockingQueue[V] extends BlockingQueue[V] {
  private val q = Seq(new LinkedBlockingDeque[V](), new LinkedBlockingDeque[V](), new LinkedBlockingDeque[V]())

  override def poll(l: Long, timeUnit: TimeUnit): V = {
    // TODO use collectFirst?
    if (!q(0).isEmpty) q(0).poll(l, timeUnit)
    else if (!q(1).isEmpty) q(1).poll(l, timeUnit)
    else q(2).poll(l, timeUnit)
  }

  override def remove(o: scala.Any): Boolean = {
    q.foldLeft(false)((v, queue) => queue.remove(o) && v)
  }

  override def put(e: V): Unit = {
    e match {
      case v: PriorityV => q(v.priority).put(e)
      case _ => q(1).put(e)
    }
  }

  override def offer(e: V): Boolean = {
    e match {
      case v: PriorityV => q(v.priority).offer(e)
      case _ => q(1).offer(e)
    }
  }

  override def offer(e: V, l: Long, timeUnit: TimeUnit): Boolean = {
    e match {
      case v: PriorityV => q(v.priority).offer(e, l, timeUnit)
      case _ => q(1).offer(e, l, timeUnit)
    }
  }

  override def add(e: V): Boolean = {
    e match {
      case v: PriorityV => q(v.priority).add(e)
      case _ => q(1).add(e)
    }
  }

  override def drainTo(collection: util.Collection[_ >: V]): Int = {
    q.foldLeft(0)((c, queue) => c + queue.drainTo(collection))
  }

  override def drainTo(collection: util.Collection[_ >: V], i: Int): Int = {
    q.foldLeft((i, 0))((n, queue) => {
      val transferred = queue.drainTo(collection, n._1)
      (n._1 - transferred, n._2 + transferred)
    })._2
  }

  override def take(): V = {
    var r: Option[V] = None
    while (r.isEmpty)
      r = q.collectFirst({case queue: BlockingQueue[V] if !queue.isEmpty => queue.poll()}) // is there a concurrency issue? maybe a non-severe one
    r.get
  }

  override def contains(o: scala.Any): Boolean = {
    q.exists(_.contains(o))
  }

  override def remainingCapacity(): Int = {
    q.map(_.remainingCapacity()).min
  }

  override def poll(): V = {
    q.collectFirst({case queue: BlockingQueue[V] if !queue.isEmpty => queue.poll()}).orNull
  }

  override def remove(): V = {
    val head = poll()
    if (head == null)
      throw new NoSuchElementException
    else
      head
  }

  override def element(): V = {
    val head = peek()
    if (head == null)
      throw new NoSuchElementException
    else
      head
  }

  override def peek(): V = {
    q.collectFirst({case q: BlockingQueue[V] if !q.isEmpty => q.peek()}).orNull
  }

  override def iterator(): util.Iterator[V] = ???

  override def removeAll(collection: util.Collection[_]): Boolean = {
    q.foldLeft(false)((success, q) => q.removeAll(collection) && success)
  }

  override def toArray: Array[AnyRef] = ???

  override def toArray[T](ts: Array[T]): Array[T] = ???

  override def containsAll(collection: util.Collection[_]): Boolean = {
    collection.stream().allMatch(contains(_))
  }

  override def clear(): Unit = {
    q.foreach(_.clear())
  }

  override def isEmpty: Boolean = {
    q.forall(_.isEmpty)
  }

  override def size(): Int = {
    q.foldLeft(0)((c, q) => c + q.size())
  }

  override def addAll(collection: util.Collection[_ <: V]): Boolean = {
    var success = false
    collection.forEach(r => add(r) && success)
    success
  }

  override def retainAll(collection: util.Collection[_]): Boolean = {
    q.foldLeft(false)((success, q) => q.retainAll(collection) && success)
  }
}

class HandlerPool(parallelism: Int = 8, unhandledExceptionHandler: Throwable => Unit = _.printStackTrace()) {

//  private val pool: ForkJoinPool = new ForkJoinPool(parallelism)
  private val pool: ThreadPoolExecutor = new ThreadPoolExecutor(parallelism, parallelism, Int.MaxValue, TimeUnit.NANOSECONDS, new GroupedBlockingQueue[Runnable])
  private val poolState = new AtomicReference[PoolState](new PoolState)

  private val cellsNotDone = new AtomicReference[Map[Cell[_, _], Cell[_, _]]](Map())

  /**
   * Returns a new cell in this HandlerPool.
   *
   * Creates a new cell with the given key. The `init` method is used to
   * retrieve an initial value for that cell and to set up dependencies via `whenNext`.
   * It gets called, when the cell is awaited, either directly by the awaitResult method
   * of the HandlerPool or if a cell that depends on this cell is awaited.
   *
   * @param key The key to resolve this cell if in a cycle or insufficient input.
   * @param init A callback to return the initial value for this cell and to set up dependencies.
   * @param lattice The lattice of which the values of this cell are taken from.
   * @return Returns a cell.
   */
  def createCell[K <: Key[V], V](key: K, init: () => Outcome[V])(implicit lattice: Lattice[V]): Cell[K, V] = {
    CellCompleter(this, key, init)(lattice).cell
  }

  /**
   * Returns a new cell in this HandlerPool.
   *
   * Creates a new, completed cell with value `v`.
   *
   * @param lattice The lattice of which the values of this cell are taken from.
   * @return Returns a cell with value `v`.
   */
  def createCompletedCell[V](result: V)(implicit lattice: Lattice[V]): Cell[DefaultKey[V], V] = {
    CellCompleter.completed(this, result)(lattice).cell
  }

  @tailrec
  final def onQuiescent(handler: () => Unit): Unit = {
    val state = poolState.get()
    if (state.isQuiescent) {
      execute(new Runnable { def run(): Unit = handler() })
    } else {
      val newState = new PoolState(handler :: state.handlers, state.submittedTasks)
      val success = poolState.compareAndSet(state, newState)
      if (!success)
        onQuiescent(handler)
    }
  }

  /**
   * Register a cell at this HandlerPool.
   *
   * @param cell The cell.
   */
  private[cell] def register[K <: Key[V], V](cell: Cell[K, V]): Unit = {
    val registered = cellsNotDone.get()
    val newRegistered = registered + (cell -> cell)
    cellsNotDone.compareAndSet(registered, newRegistered)
  }

  /**
   * Deregister a cell at this HandlerPool.
   *
   * @param cell The cell.
   */
  private[cell] def deregister[K <: Key[V], V](cell: Cell[K, V]): Unit = {
    var success = false
    while (!success) {
      val registered = cellsNotDone.get()
      val newRegistered = registered - cell
      success = cellsNotDone.compareAndSet(registered, newRegistered)
    }
  }

  /** Returns all non-completed cells, when quiescence is reached. */
  def quiescentIncompleteCells: Future[List[Cell[_, _]]] = {
    val p = Promise[List[Cell[_, _]]]
    this.onQuiescent { () =>
      val registered = this.cellsNotDone.get()
      p.success(registered.values.toList)
    }
    p.future
  }

  def whileQuiescentResolveCell[K <: Key[V], V]: Unit = {
    while (!cellsNotDone.get().isEmpty) {
      val fut = this.quiescentResolveCell
      Await.ready(fut, 15.minutes)
    }
  }

  def whileQuiescentResolveDefault[K <: Key[V], V]: Unit = {
    while (!cellsNotDone.get().isEmpty) {
      val fut = this.quiescentResolveDefaults
      Await.ready(fut, 15.minutes)
    }
  }

  def quiescentResolveCycles[K <: Key[V], V]: Future[Boolean] = {
    val p = Promise[Boolean]
    this.onQuiescent { () =>
      // Find one closed strongly connected component (cell)
      val registered: Seq[Cell[K, V]] = this.cellsNotDone.get().values.asInstanceOf[Iterable[Cell[K, V]]].toSeq
      if (registered.nonEmpty) {
        val cSCCs = closedSCCs(registered, (cell: Cell[K, V]) => cell.totalCellDependencies)
        cSCCs.foreach(cSCC => resolveCycle(cSCC.asInstanceOf[Seq[Cell[K, V]]]))
      }
      p.success(true)
    }
    p.future
  }

  def quiescentResolveDefaults[K <: Key[V], V]: Future[Boolean] = {
    val p = Promise[Boolean]
    this.onQuiescent { () =>
      // Finds the rest of the unresolved cells (that have been triggered)
      val rest = this.cellsNotDone.get().values.filter(_.isRunning).asInstanceOf[Iterable[Cell[K, V]]].toSeq
      if (rest.nonEmpty) {
        resolveDefault(rest)
      }
      p.success(true)
    }
    p.future
  }

  def quiescentResolveCell[K <: Key[V], V]: Future[Boolean] = {
    val p = Promise[Boolean]
    this.onQuiescent { () =>
      // Find one closed strongly connected component (cell)
      val registered: Seq[Cell[K, V]] = this.cellsNotDone.get().values.asInstanceOf[Iterable[Cell[K, V]]].toSeq
      if (registered.nonEmpty) {
        val cSCCs = closedSCCs(registered, (cell: Cell[K, V]) => cell.totalCellDependencies)
        cSCCs.foreach(cSCC => resolveCycle(cSCC.asInstanceOf[Seq[Cell[K, V]]]))
      }
      // Finds the rest of the unresolved cells (that have been triggered)
      val rest = this.cellsNotDone.get().values.filter(_.isRunning).asInstanceOf[Iterable[Cell[K, V]]].toSeq
      if (rest.nonEmpty) {
        resolveDefault(rest)
      }
      p.success(true)
    }
    p.future
  }

  /**
   * Resolves a cycle of unfinished cells.
   */
  private def resolveCycle[K <: Key[V], V](cells: Seq[Cell[K, V]]): Unit = {
    val key = cells.head.key
    val result = key.resolve(cells)

    for ((c, v) <- result) {
      cells.filterNot(_ == c).foreach(cell => {
        c.removeNextCallbacks(cell)
        c.removeCompleteCallbacks(cell)
      })
      c.resolveWithValue(v)
    }
  }

  /**
   * Resolves a cell with default value.
   */
  private def resolveDefault[K <: Key[V], V](cells: Seq[Cell[K, V]]): Unit = {
    val key = cells.head.key
    val result = key.fallback(cells)

    for ((c, v) <- result) {
      cells.filterNot(_ == c).foreach(cell => {
        c.removeNextCallbacks(cell)
        c.removeCompleteCallbacks(cell)
      })
      c.resolveWithValue(v)
    }
  }

  // Shouldn't we use:
  //def execute(f : => Unit) : Unit =
  //  execute(new Runnable{def run() : Unit = f})

  private[cell] def execute(fun: () => Unit): Unit =
    execute(new Runnable { def run(): Unit = fun() })

  private[cell] def execute(fun: () => Unit, priority: Int): Unit =
    execute(new PriorityRunnable {
      override def priority: Int = priority
      override def run(): Unit = fun()
    })

  /**
    * Use a PriorityRunnable to set priority!
    * @param task
    */
  private[cell] def execute(task: Runnable): Unit = {
    // Submit task to the pool
    var submitSuccess = false
    while (!submitSuccess) {
      val state = poolState.get()
      val newState = new PoolState(state.handlers, state.submittedTasks + 1)
      submitSuccess = poolState.compareAndSet(state, newState)
    }

    // Run the task
    pool.execute(new Runnable {
      def run(): Unit = {
        try {
          task.run()
        } catch {
          case NonFatal(e) =>
            unhandledExceptionHandler(e)
        } finally {
          var success = false
          var handlersToRun: Option[List[() => Unit]] = None
          while (!success) {
            val state = poolState.get()
            if (state.submittedTasks > 1) {
              handlersToRun = None
              val newState = new PoolState(state.handlers, state.submittedTasks - 1)
              success = poolState.compareAndSet(state, newState)
            } else if (state.submittedTasks == 1) {
              handlersToRun = Some(state.handlers)
              val newState = new PoolState()
              success = poolState.compareAndSet(state, newState)
            } else {
              throw new Exception("BOOM")
            }
          }
          if (handlersToRun.nonEmpty) {
            handlersToRun.get.foreach { handler =>
              execute(new Runnable {
                def run(): Unit = handler()
              })
            }
          }
        }
      }
    })
  }

  /**
   * If a cell is triggered, it's `init` method is
   * run to both get an initial (or possibly final) value
   * and to set up dependencies. All dependees automatically
   * get triggered.
   *
   * @param cell The cell that is triggered.
   */
  def triggerExecution[K <: Key[V], V](cell: Cell[K, V], priority: Int): Unit = {
    if (cell.markAsRunning())
      execute(() => {
        val completer = cell.asInstanceOf[CellImpl[K, V]]
        val outcome = completer.init()
        outcome match {
          case Outcome(v, isFinal) =>
            completer.put(v, isFinal)
          case NoOutcome => /* don't do anything */
        }
      }, priority = priority)
  }

  /**
   * Possibly initiates an orderly shutdown in which previously
   * submitted tasks are executed, but no new tasks will be accepted.
   */
  def shutdown(): Unit =
    pool.shutdown()

  def reportFailure(t: Throwable): Unit =
    t.printStackTrace()
}
