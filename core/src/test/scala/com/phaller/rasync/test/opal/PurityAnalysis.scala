package com.phaller.rasync
package test
package opal

import java.net.URL

import com.phaller.rasync.cell._
import com.phaller.rasync.lattice.Updater
import com.phaller.rasync.pool._

import scala.concurrent.Await
import scala.concurrent.duration._
import org.opalj.Success
import org.opalj.br.{ ClassFile, Method }
import org.opalj.br.analyses.{ BasicReport, DefaultOneStepAnalysis, Project }
import org.opalj.br.instructions.GETFIELD
import org.opalj.br.instructions.GETSTATIC
import org.opalj.br.instructions.PUTFIELD
import org.opalj.br.instructions.PUTSTATIC
import org.opalj.br.instructions.MONITORENTER
import org.opalj.br.instructions.MONITOREXIT
import org.opalj.br.instructions.NEW
import org.opalj.br.instructions.NEWARRAY
import org.opalj.br.instructions.MULTIANEWARRAY
import org.opalj.br.instructions.ANEWARRAY
import org.opalj.br.instructions.AALOAD
import org.opalj.br.instructions.AASTORE
import org.opalj.br.instructions.ARRAYLENGTH
import org.opalj.br.instructions.LALOAD
import org.opalj.br.instructions.IALOAD
import org.opalj.br.instructions.CALOAD
import org.opalj.br.instructions.BALOAD
import org.opalj.br.instructions.BASTORE
import org.opalj.br.instructions.CASTORE
import org.opalj.br.instructions.IASTORE
import org.opalj.br.instructions.LASTORE
import org.opalj.br.instructions.SASTORE
import org.opalj.br.instructions.SALOAD
import org.opalj.br.instructions.DALOAD
import org.opalj.br.instructions.FALOAD
import org.opalj.br.instructions.FASTORE
import org.opalj.br.instructions.DASTORE
import org.opalj.br.instructions.INVOKEDYNAMIC
import org.opalj.br.instructions.INVOKESTATIC
import org.opalj.br.instructions.INVOKESPECIAL
import org.opalj.br.instructions.INVOKEVIRTUAL
import org.opalj.br.instructions.INVOKEINTERFACE
import org.opalj.br.instructions.MethodInvocationInstruction
import org.opalj.br.instructions.NonVirtualMethodInvocationInstruction
import org.opalj.bytecode.JRELibraryFolder
import org.opalj.util.{ Nanoseconds, PerformanceEvaluation }
import org.scalatest.FunSuite

import scala.util.Try

class PurityAnalysisTest extends FunSuite {
  test("main") {
    PurityAnalysis.main(null)
  }
}

object PurityAnalysis {

  def main(args: Array[String]): Unit = {
    val lib = Project(new java.io.File(JRELibraryFolder.getAbsolutePath))

    var lastAvg = 0L
    for (
      scheduling <- List(DefaultScheduling, SourcesWithManyTargetsFirst, SourcesWithManyTargetsLast, TargetsWithManySourcesFirst, TargetsWithManySourcesLast, TargetsWithManyTargetsFirst, TargetsWithManyTargetsLast, SourcesWithManySourcesFirst, SourcesWithManySourcesLast);
      threads <- List(1, 2, 4, 8, 16, 32)
    ) {
      PerformanceEvaluation.time(2, 4, 3, {
        val p = lib.recreate()
        val report = PurityAnalysis.doAnalyze(p, List.empty, () => false)
      }) { (_, ts) ⇒
        val sTs = ts.map(_.toSeconds).mkString(", ")
        val avg = ts.map(_.timeSpan).sum / ts.size
        if (lastAvg != avg) {
          lastAvg = avg
          val avgInSeconds = new Nanoseconds(lastAvg).toSeconds
          println(s"RES: Scheduling = ${scheduling.getClass.getSimpleName}, #threads = $threads, avg = $avgInSeconds;Ts: $sTs")
        }
      }
      println(s"AVG,${scheduling.getClass.getSimpleName},$threads,$lastAvg")
    }
  }

  def doAnalyze(
    project: Project[URL],
    parameters: Seq[String] = List.empty,
    isInterrupted: () ⇒ Boolean): BasicReport = {

    // 1. Initialization of key data structures (one cell(completer) per method)
    implicit val pool: HandlerPool[Purity] = new HandlerPool(PurityKey)
    var methodToCell = Map.empty[Method, Cell[Purity]]
    for {
      classFile <- project.allProjectClassFiles
      method <- classFile.methods
    } {
      val cell = pool.mkCell(_ => {
        analyze(project, methodToCell, classFile, method)
      })(Updater.partialOrderingToUpdater)
      methodToCell = methodToCell + ((method, cell))
    }

    // 2. trigger analyses
    for {
      classFile <- project.allProjectClassFiles.par
      method <- classFile.methods
    } {
      methodToCell(method).trigger()
    }
    val fut = pool.quiescentResolveCell
    Await.ready(fut, 30.minutes)
    pool.shutdown()

    BasicReport("")
  }

  /**
   * Determines the purity of the given method.
   */
  private def analyze(
    project: Project[URL],
    methodToCell: Map[Method, Cell[Purity]],
    classFile: ClassFile,
    method: Method): Outcome[Purity] = {
    import project.nonVirtualCall

    val cell = methodToCell(method)

    if ( // Due to a lack of knowledge, we classify all native methods or methods that
    // belong to a library (and hence lack the body) as impure...
    method.body.isEmpty /*HERE: method.isNative || "isLibraryMethod(method)"*/ ||
      // for simplicity we are just focusing on methods that do not take objects as parameters
      method.parameterTypes.exists(!_.isBaseType)) {
      return FinalOutcome(Impure)
    }

    val dependencies = scala.collection.mutable.Set.empty[Method]
    val declaringClassType = classFile.thisType
    val methodDescriptor = method.descriptor
    val methodName = method.name
    val body = method.body.get
    val instructions = body.instructions
    val maxPC = instructions.size

    var currentPC = 0
    while (currentPC < maxPC) {
      val instruction = instructions(currentPC)

      (instruction.opcode: @scala.annotation.switch) match {
        case GETSTATIC.opcode ⇒
          val GETSTATIC(declaringClass, fieldName, fieldType) = instruction
          import project.resolveFieldReference
          resolveFieldReference(declaringClass, fieldName, fieldType) match {

            case Some(field) if field.isFinal ⇒ NoOutcome
            /* Nothing to do; constants do not impede purity! */

            // case Some(field) if field.isPrivate /*&& field.isNonFinal*/ ⇒
            // check if the field is effectively final

            case _ ⇒
              return FinalOutcome(Impure);
          }

        case INVOKESPECIAL.opcode | INVOKESTATIC.opcode ⇒ instruction match {

          case MethodInvocationInstruction(`declaringClassType`, _, `methodName`, `methodDescriptor`) ⇒
          // We have a self-recursive call; such calls do not influence
          // the computation of the method's purity and are ignored.
          // Let's continue with the evaluation of the next instruction.

          case mii: NonVirtualMethodInvocationInstruction ⇒

            nonVirtualCall(method.classFile.thisType, mii) match {

              case Success(callee) ⇒
                /* Recall that self-recursive calls are handled earlier! */
                dependencies.add(callee)

              case _ /* Empty or Failure */ ⇒

                // We know nothing about the target method (it is not
                // found in the scope of the current project).
                return FinalOutcome(Impure)
            }

        }

        case NEW.opcode |
          GETFIELD.opcode |
          PUTFIELD.opcode | PUTSTATIC.opcode |
          NEWARRAY.opcode | MULTIANEWARRAY.opcode | ANEWARRAY.opcode |
          AALOAD.opcode | AASTORE.opcode |
          BALOAD.opcode | BASTORE.opcode |
          CALOAD.opcode | CASTORE.opcode |
          SALOAD.opcode | SASTORE.opcode |
          IALOAD.opcode | IASTORE.opcode |
          LALOAD.opcode | LASTORE.opcode |
          DALOAD.opcode | DASTORE.opcode |
          FALOAD.opcode | FASTORE.opcode |
          ARRAYLENGTH.opcode |
          MONITORENTER.opcode | MONITOREXIT.opcode |
          INVOKEDYNAMIC.opcode | INVOKEVIRTUAL.opcode | INVOKEINTERFACE.opcode ⇒
          return FinalOutcome(Impure)

        case _ ⇒
        /* All other instructions (IFs, Load/Stores, Arith., etc.) are pure. */
      }
      currentPC = body.pcOfNextInstruction(currentPC)
    }

    // Every method that is not identified as being impure is (conditionally) pure.
    if (dependencies.isEmpty) {
      FinalOutcome(Pure)
    } else {
      cell.when(dependencies.map(methodToCell).toSeq: _*)(c)
      NextOutcome(UnknownPurity) // == NoOutcome
    }
  }

  def c(v: Iterable[(Cell[Purity], Try[ValueOutcome[Purity]])]): Outcome[Purity] = {
    // If any dependee is Impure, the dependent Cell is impure.
    // Otherwise, we do not know anything new.
    // Exception will be rethrown.
    if (v.collectFirst({
      case (_, scala.util.Success(FinalOutcome(Impure))) => true
      case (_, scala.util.Failure(_)) => true
    }).isDefined)
      FinalOutcome(Impure)
    else NoOutcome
  }
}
