package cell

import java.util
import java.util.concurrent.{BlockingQueue, LinkedBlockingDeque, ThreadPoolExecutor, TimeUnit}
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
private class PoolState(val quiescenceHandlers: List[() => Unit] = List(), val submittedTasks: Int = 0) {
  def isQuiescent(): Boolean =
    submittedTasks == 0
}

trait PriorityV {
  def priority: Int
}

trait PriorityRunnable extends Runnable with PriorityV

private class GroupedBlockingQueue extends BlockingQueue[Runnable] {
  private val q = Seq(new LinkedBlockingDeque[Runnable](), new LinkedBlockingDeque[Runnable](), new LinkedBlockingDeque[Runnable]())

  override def poll(l: Long, timeUnit: TimeUnit): Runnable = {
    // TODO use collectFirst?
    if (!q(0).isEmpty) q(0).poll(l, timeUnit)
    else if (!q(1).isEmpty) q(1).poll(l, timeUnit)
    else q(2).poll(l, timeUnit)
  }

  override def remove(o: scala.Any): Boolean = {
    q.foldLeft(false)((v, queue) => queue.remove(o) && v)
  }

  override def put(e: Runnable): Unit = {
    e match {
      case v: PriorityRunnable => q(v.priority).put(e)
      case _ => q(1).put(e)
    }
  }

  override def offer(e: Runnable): Boolean = {
    // THIS IS USED BY THE THREADPOOL
    e match {
      case v: PriorityRunnable =>
        assert(v.priority >= 0)
        assert(v.priority <= 2)
        q(v.priority).offer(e)
      case _ => q(1).offer(e)
    }
  }

  override def offer(e: Runnable, l: Long, timeUnit: TimeUnit): Boolean = {
    e match {
      case v: PriorityRunnable => q(v.priority).offer(e, l, timeUnit)
      case _ => q(1).offer(e, l, timeUnit)
    }
  }

  override def add(e: Runnable): Boolean = {
    e match {
      case v: PriorityRunnable => q(v.priority).add(e)
      case _ => q(1).add(e)
    }
  }

  override def drainTo(collection: util.Collection[_ >: Runnable]): Int = {
    q.foldLeft(0)((c, queue) => c + queue.drainTo(collection))
  }

  override def drainTo(collection: util.Collection[_ >: Runnable], i: Int): Int = {
    q.foldLeft((i, 0))((n, queue) => {
      val transferred = queue.drainTo(collection, n._1)
      (n._1 - transferred, n._2 + transferred)
    })._2
  }

  def statusString = q.foldLeft("")((s, q) => s + " " + q.size)

  override def take(): Runnable = {
    // THIS IS USED BY THE THREADPOOL
    val s = statusString
    var r: Option[Runnable] = None
    while (r.isEmpty || r.get == null)
      r = q.collectFirst({case queue: BlockingQueue[Runnable] if !queue.isEmpty => queue.poll()}) // is there a concurrency issue? maybe a non-severe one
    try {
//      println("Taking " + r.get.asInstanceOf[PriorityRunnable].priority + "  while status was " + statusString)
    } catch {
      case e: NullPointerException => println("Taking something else: " + r)
    }
    r.get
  }

  override def contains(o: scala.Any): Boolean = {
    q.exists(_.contains(o))
  }

  override def remainingCapacity(): Int = {
    q.map(_.remainingCapacity()).min
  }

  override def poll(): Runnable = q.collectFirst({ case queue: BlockingQueue[Runnable] if !queue.isEmpty => queue.poll() }).orNull

  override def remove(): Runnable = {
    val head = poll()
    if (head == null)
      throw new NoSuchElementException
    else
      head
  }

  override def element(): Runnable = {
    val head = peek()
    if (head == null)
      throw new NoSuchElementException
    else
      head
  }

  override def peek(): Runnable = q.collectFirst({ case q: BlockingQueue[Runnable] if !q.isEmpty => q.peek() }).orNull

  override def iterator(): util.Iterator[Runnable] = ???

  override def removeAll(collection: util.Collection[_]): Boolean = {
    q.foldLeft(false)((success, q) => q.removeAll(collection) && success)
  }

  override def toArray: Array[AnyRef] = ???

  override def toArray[T](ts: Array[T with Object]): Array[T with Object] = ???

  override def containsAll(collection: util.Collection[_]): Boolean = {
    collection.stream().allMatch(contains(_))
  }

  override def clear(): Unit = {
    q.foreach(_.clear())
  }

  override def isEmpty: Boolean = {
    // THIS IS USED BY THE THREADPOOL
    q.forall(_.isEmpty)
  }

  override def size(): Int = {
    q.foldLeft(0)((c, q) => c + q.size())
  }

  override def addAll(collection: util.Collection[_ <: Runnable]): Boolean = {
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
  private val pool: ThreadPoolExecutor = new ThreadPoolExecutor(parallelism, parallelism, Int.MaxValue, TimeUnit.NANOSECONDS, new GroupedBlockingQueue)
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
      execute(new Runnable { def run(): Unit = handler() }, 1)
    } else {
      val newState = new PoolState(handler :: state.quiescenceHandlers, state.submittedTasks)
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

  private[cell] def execute(fun: () => Unit, prio: Int): Unit =
    execute(new Runnable {
      override def run(): Unit = fun()
    }, prio)

  private[cell] def execute(task: Runnable, prio: Int): Unit = {
    // Submit task to the pool
    var submitSuccess = false
    while (!submitSuccess) {
      val state = poolState.get()
      val newState = new PoolState(state.quiescenceHandlers, state.submittedTasks + 1)
      submitSuccess = poolState.compareAndSet(state, newState)
    }

    // Run the task
    pool.execute(new PriorityRunnable {
      override def priority: Int = prio

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
              val newState = new PoolState(state.quiescenceHandlers, state.submittedTasks - 1)
              success = poolState.compareAndSet(state, newState)
            } else if (state.submittedTasks == 1) {
              handlersToRun = Some(state.quiescenceHandlers)
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
              }, prio)
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
  def triggerExecution[K <: Key[V], V](cell: Cell[K, V], priority: Int = 1): Unit = {
    if (cell.markAsRunning())
      execute(() => {
        val completer = cell.asInstanceOf[CellImpl[K, V]]
        val outcome = completer.init()
        outcome match {
          case Outcome(v, isFinal) =>
            completer.put(v, isFinal)
          case NoOutcome => /* don't do anything */
        }
      }, priority)
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
