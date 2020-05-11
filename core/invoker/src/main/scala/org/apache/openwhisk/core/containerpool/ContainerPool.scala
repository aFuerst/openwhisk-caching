/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.openwhisk.core.containerpool

import akka.actor.{Actor, ActorRef, ActorRefFactory, Props}
import org.apache.openwhisk.common.MetricEmitter
import org.apache.openwhisk.common.{AkkaLogging, LoggingMarkers, TransactionId}
import org.apache.openwhisk.core.connector.MessageFeed
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.entity.size._
import scala.annotation.tailrec
import scala.collection.immutable
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.util.Try
import java.time.Instant

sealed trait WorkerState
case object Busy extends WorkerState
case object Free extends WorkerState

case class WorkerData(data: ContainerData, state: WorkerState)

case object EmitMetrics

/**
 * A pool managing containers to run actions on.
 *
 * This pool fulfills the other half of the ContainerProxy contract. Only
 * one job (either Start or Run) is sent to a child-actor at any given
 * time. The pool then waits for a response of that container, indicating
 * the container is done with the job. Only then will the pool send another
 * request to that container.
 *
 * Upon actor creation, the pool will start to prewarm containers according
 * to the provided prewarmConfig, iff set. Those containers will **not** be
 * part of the poolsize calculation, which is capped by the poolSize parameter.
 * Prewarm containers are only used, if they have matching arguments
 * (kind, memory) and there is space in the pool.
 *
 * @param childFactory method to create new container proxy actor
 * @param feed actor to request more work from
 * @param prewarmConfig optional settings for container prewarming
 * @param poolConfig config for the ContainerPool
 */
class ContainerPool(childFactory: ActorRefFactory => ActorRef,
                    feed: ActorRef,
                    prewarmConfig: List[PrewarmingConfig] = List.empty,
                    poolConfig: ContainerPoolConfig)
    extends Actor {
  import ContainerPool.memoryConsumptionOf

  implicit val logging = new AkkaLogging(context.system.log)
  implicit val ec = context.dispatcher

  var freePool = immutable.Map.empty[ActorRef, ContainerData]
  var busyPool = immutable.Map.empty[ActorRef, ContainerData]
  var prewarmedPool = immutable.Map.empty[ActorRef, ContainerData]
  var prewarmStartingPool = immutable.Map.empty[ActorRef, (String, ByteSize)]
  
  var prewarmStartingPoolStartTimes = immutable.Map.empty[ActorRef, Long]
  var seen = mutable.Set.empty[String]
  
  var startedTimes = mutable.Map.empty[String, (Long, String)] // container-action-key, (time, state)
  var coldTimes = mutable.Map.empty[String, Long]  // action-key, time
  var warmTimes = mutable.Map.empty[String, Long]  // action-key, time
  var lastCalled = mutable.Map.empty[String, Long] // action-key, time
  var callCount = mutable.Map.empty[String, Long] // action-key, count

  // If all memory slots are occupied and if there is currently no container to be removed, than the actions will be
  // buffered here to keep order of computation.
  // Otherwise actions with small memory-limits could block actions with large memory limits.
  var runBuffer = immutable.Queue.empty[Run]
  // Track the resent buffer head - so that we don't resend buffer head multiple times
  var resent: Option[Run] = None
  val logMessageInterval = 10.seconds
  //periodically emit metrics (don't need to do this for each message!)
  context.system.scheduler.schedule(30.seconds, 10.seconds, self, EmitMetrics)

  backfillPrewarms(true)

  def cost(action: ExecutableWhiskAction): Long = {
    val key = getActionKey(action)
    val c = (coldTimes get key).getOrElse(0L)
    val w = (warmTimes get key).getOrElse(0L)
    c - w
  }

  def getMilis(): Long = {
    Instant.now().toEpochMilli()
  }

  def getActionKey(action: ExecutableWhiskAction): String = {
    s"${action.namespace}/${action.name}"
  }

  def getContainerActionKey(container: Option[Container], action: ExecutableWhiskAction): String = {
    container match {
      case Some(cont) => s"${cont}-${action.namespace}/${action.name}"
      case None => s"None-${action.namespace}/${action.name}"
    }
  }

  def calcPriority(containerData: ContainerData) : Double = {
    def size(container:Container, action:ExecutableWhiskAction):Double = {
      action.limits.memory.megabytes
    }

    def pri(container:Container, action:ExecutableWhiskAction):Double = {
      val key = getActionKey(action)
      val freq = (callCount get key).getOrElse(0L)
      (lastCalled get key).getOrElse(0L) + ((freq*cost(action))/size(container, action))
    }

    containerData match {
      // case ContainerNotInUse() => 0 // unallocated container has no priority
      case WarmedData(container, invocationNamespace, action, _, _, _) => return pri(container, action)
      case WarmingColdData(_,_,_,_) => {logging.error(this, "cant remove warming cold container as it will be running something"); return 0;}
      case WarmingData(container, invocationNamespace, action, _, _) => return pri(container, action)
      case other => { logging.error(this, "!!!unknown type to get priority of!!!"); return 0;}
    }
  }

  def sortOnPriorities(pool: Map[ActorRef, ContainerData]) : List[(ActorRef, ContainerData)] = {
    var unordered = ListBuffer[(Double, (ActorRef, ContainerData))]()
    pool.foreach{case (actor, data) =>
      unordered += ((calcPriority(data), (actor, data)))
    }
    unordered.foreach{case (pri, (actor, data)) =>
      logging.info(this, s"unordered; priority: ${pri}, data: ${data}")
    }
    logging.info(this, s"${unordered}")
    var ordered = unordered.sortWith((l, r) => l._1 < r._1).to[List]
    ordered.foreach{case (pri, (actor, data)) =>
      logging.info(this, s"ordered; priority: ${pri}, data: ${data}")
    }
    for (item <- ordered) yield item._2
    // ordered.collect {i => i._2 }
  }

  def logContainerStart(r: Run, containerState: String, activeActivations: Int, container: Option[Container]): Unit = {
    val namespaceName = r.msg.user.namespace.name
    val actionName = r.action.name.name
    val maxConcurrent = r.action.limits.concurrency.maxConcurrent
    val activationId = r.msg.activationId.toString

    r.msg.transid.mark(
      this,
      LoggingMarkers.INVOKER_CONTAINER_START(
        containerState,
        r.msg.user.namespace.toString,
        r.msg.action.namespace.toString,
        r.msg.action.name.toString),
      s"containerStart containerState: $containerState container: $container activations: $activeActivations of max $maxConcurrent action: $actionName namespace: $namespaceName activationId: $activationId",
      akka.event.Logging.InfoLevel)
  }

  def receive: Receive = {
    // A job to run on a container
    //
    // Run messages are received either via the feed or from child containers which cannot process
    // their requests and send them back to the pool for rescheduling (this may happen if "docker" operations
    // fail for example, or a container has aged and was destroying itself when a new request was assigned)
    case r: Run =>
      // Check if the message is resent from the buffer. Only the first message on the buffer can be resent.
      val isResentFromBuffer = runBuffer.nonEmpty && runBuffer.dequeueOption.exists(_._1.msg == r.msg)

      // Only process request, if there are no other requests waiting for free slots, or if the current request is the
      // next request to process
      // It is guaranteed, that only the first message on the buffer is resent.
      if (runBuffer.isEmpty || isResentFromBuffer) {
        if (isResentFromBuffer) {
          //remove from resent tracking - it may get resent again, or get processed
          resent = None
        }
        val createdContainer =
          // Is there enough space on the invoker for this action to be executed.
          if (hasPoolSpaceFor(busyPool, r.action.limits.memory.megabytes.MB)) {
            // Schedule a job to a warm container
            ContainerPool
              .schedule(r.action, r.msg.user.namespace.name, freePool)
              .map(container => (container, container._2.initingState)) //warmed, warming, and warmingCold always know their state
              .orElse(
                // There was no warm/warming/warmingCold container. Try to take a prewarm container or a cold container.

                // Is there enough space to create a new container or do other containers have to be removed?
                if (hasPoolSpaceFor(busyPool ++ freePool, r.action.limits.memory.megabytes.MB)) {
                  takePrewarmContainer(r.action)
                    .map(container => (container, "prewarmed"))
                    .orElse(Some(createContainer(r.action.limits.memory.megabytes.MB), "cold"))
                } else {
                  sortOnPriorities(freePool)
                  None
                })
              .orElse(
                // Remove a container and create a new one for the given job
                ContainerPool
                // Only free up the amount, that is really needed to free up
                  .remove(freePool, Math.min(r.action.limits.memory.megabytes, memoryConsumptionOf(freePool)).MB)
                  .map(removeContainer)
                  // If the list had at least one entry, enough containers were removed to start the new container. After
                  // removing the containers, we are not interested anymore in the containers that have been removed.
                  .headOption
                  .map(_ =>
                    takePrewarmContainer(r.action)
                      .map(container => (container, "recreatedPrewarm"))
                      .getOrElse(createContainer(r.action.limits.memory.megabytes.MB), "recreated")))

          } else None

        createdContainer match {
          case Some(((actor, data), containerState)) =>
            //increment active count before storing in pool map
            val newData = data.nextRun(r)
            val container = newData.getContainer

            if (newData.activeActivationCount < 1) {
              logging.error(this, s"invalid activation count < 1 ${newData}")
            }

            //only move to busyPool if max reached
            if (!newData.hasCapacity()) {
              if (r.action.limits.concurrency.maxConcurrent > 1) {
                logging.info(
                  this,
                  s"container ${container} is now busy with ${newData.activeActivationCount} activations")
              }
              busyPool = busyPool + (actor -> newData)
              freePool = freePool - actor
            } else {
              //update freePool to track counts
              freePool = freePool + (actor -> newData)
            }
            // Remove the action that was just executed from the buffer and execute the next one in the queue.
            if (isResentFromBuffer) {
              // It is guaranteed that the currently executed messages is the head of the queue, if the message comes
              // from the buffer
              val (_, newBuffer) = runBuffer.dequeue
              runBuffer = newBuffer
              // Try to process the next item in buffer (or get another message from feed, if buffer is now empty)
              processBufferOrFeed()
            }

            val key = getContainerActionKey(container, r.action)
            seen += key
            logging.info(this, s"starting action with key ${key}, state ${containerState}")
            startedTimes += (key -> (getMilis(), containerState))
            val actkey = getActionKey(r.action)
            var cnt = (callCount get actkey).getOrElse(0L)
            callCount -= actkey
            callCount += (actkey -> (cnt+1))
            lastCalled -= actkey
            lastCalled += (actkey -> getMilis())

            actor ! r // forwards the run request to the container
            logContainerStart(r, containerState, newData.activeActivationCount, container)
          case None =>
            // this can also happen if createContainer fails to start a new container, or
            // if a job is rescheduled but the container it was allocated to has not yet destroyed itself
            // (and a new container would over commit the pool)
            val isErrorLogged = r.retryLogDeadline.map(_.isOverdue).getOrElse(true)
            val retryLogDeadline = if (isErrorLogged) {
              logging.warn(
                this,
                s"Rescheduling Run message, too many message in the pool, " +
                  s"freePoolSize: ${freePool.size} containers and ${memoryConsumptionOf(freePool)} MB, " +
                  s"busyPoolSize: ${busyPool.size} containers and ${memoryConsumptionOf(busyPool)} MB, " +
                  s"maxContainersMemory ${poolConfig.userMemory.toMB} MB, " +
                  s"userNamespace: ${r.msg.user.namespace.name}, action: ${r.action}, " +
                  s"needed memory: ${r.action.limits.memory.megabytes} MB, " +
                  s"waiting messages: ${runBuffer.size}")(r.msg.transid)
              MetricEmitter.emitCounterMetric(LoggingMarkers.CONTAINER_POOL_RESCHEDULED_ACTIVATION)
              Some(logMessageInterval.fromNow)
            } else {
              r.retryLogDeadline
            }
            if (!isResentFromBuffer) {
              // Add this request to the buffer, as it is not there yet.
              runBuffer = runBuffer.enqueue(Run(r.action, r.msg, retryLogDeadline))
            }
          //buffered items will be processed via processBufferOrFeed()
        }
      } else {
        // There are currently actions waiting to be executed before this action gets executed.
        // These waiting actions were not able to free up enough memory.
        runBuffer = runBuffer.enqueue(r)
      }

    // Container is free to take more work
    case NeedWork(warmData: WarmedData) =>
      val oldData = freePool.get(sender()).getOrElse(busyPool(sender()))
      val newData =
        warmData.copy(lastUsed = oldData.lastUsed, activeActivationCount = oldData.activeActivationCount - 1)
      if (newData.activeActivationCount < 0) {
        logging.error(this, s"invalid activation count after warming < 1 ${newData}")
      }
      if (newData.hasCapacity()) {
        var fullKey = getContainerActionKey(Some(warmData.container), warmData.action)
        val found = startedTimes get fullKey
        val now = getMilis()
        val actKey = getActionKey(warmData.action)
        found match {
          case Some((startTime, state)) => {
            if (state == "warmed")
            {
              warmTimes += (actKey -> (now - startTime))
              logging.info(this, s"Warm action ${actKey} finished in ${now - startTime}")
            }
            else
            {
              coldTimes += (actKey -> (now - startTime))
              logging.info(this, s"Prewarmed action ${actKey} finished in ${now - startTime} from state ${state}")
            }
            seen -= fullKey
          }
          case None => {
            val key = getContainerActionKey(None, warmData.action)
            val newFound = startedTimes get key
            newFound match {
              case Some((startTime, state)) => {
                coldTimes += (actKey -> (now - startTime))
                seen -= key
                logging.info(this, s"Cold action ${actKey} finished in ${now - startTime}")
              }
              case None => logging.error(this, s"Unexpected data incoming ${warmData}")
            }
          }
        }
        // if (found.isEmpty)
        // {
        //   val key = getContainerActionKey(None, warmData.action)
        //   val t = (startedTimes get key).getOrElse(now)
        //   coldTimes += (actKey -> (now - t))
        //   seen -= key
        //   logging.info(this, s"Cold action ${actKey} finished in ${now - t}")
        // }
        // else
        // {
        //   warmTimes += (actKey -> (now - found.getOrElse(now)))
        //   logging.info(this, s"Warm action ${actKey} finished in ${now - found.getOrElse(now)}")
        //   seen -= fullKey
        // }
        // val key = found match {
        //   case Some(k) => warmTimes += (actKey -> now - k)
        //   case None => coldTimes += (getContainerActionKey(None, warmData.action) -> 0L)
        // }

        //remove from busy pool (may already not be there), put back into free pool (to update activation counts)
        freePool = freePool + (sender() -> newData)
        if (busyPool.contains(sender())) {
          busyPool = busyPool - sender()
          if (newData.action.limits.concurrency.maxConcurrent > 1) {
            logging.info(
              this,
              s"concurrent container ${newData.container} is no longer busy with ${newData.activeActivationCount} activations")
          }
        }
      } else {
        busyPool = busyPool + (sender() -> newData)
        freePool = freePool - sender()
      }
      processBufferOrFeed()
    // Container is prewarmed and ready to take work
    case NeedWork(data: PreWarmedData) =>
      // TODO: track container startup times
      // initTime = prewarmStartingPoolStartTimes get sender()
      prewarmStartingPool = prewarmStartingPool - sender()
      prewarmedPool = prewarmedPool + (sender() -> data)
      // logging.info(this, s"Container prewarm completed at ${before} finished at ${after} took ${after - before} ms")

    // Container got removed
    case ContainerRemoved =>
      // if container was in free pool, it may have been processing (but under capacity),
      // so there is capacity to accept another job request
      freePool.get(sender()).foreach { f =>
        freePool = freePool - sender()
      }
      // container was busy (busy indicates at full capacity), so there is capacity to accept another job request
      busyPool.get(sender()).foreach { _ =>
        busyPool = busyPool - sender()
      }
      processBufferOrFeed()

      //in case this was a prewarm
      prewarmedPool.get(sender()).foreach { _ =>
        logging.info(this, "failed prewarm removed")
        prewarmedPool = prewarmedPool - sender()
      }
      //in case this was a starting prewarm
      prewarmStartingPool.get(sender()).foreach { _ =>
        logging.info(this, "failed starting prewarm removed")
        prewarmStartingPool = prewarmStartingPool - sender()
      }

      //backfill prewarms on every ContainerRemoved, just in case
      backfillPrewarms(false) //in case a prewarm is removed due to health failure or crash

    // This message is received for one of these reasons:
    // 1. Container errored while resuming a warm container, could not process the job, and sent the job back
    // 2. The container aged, is destroying itself, and was assigned a job which it had to send back
    // 3. The container aged and is destroying itself
    // Update the free/busy lists but no message is sent to the feed since there is no change in capacity yet
    case RescheduleJob =>
      freePool = freePool - sender()
      busyPool = busyPool - sender()
    case EmitMetrics =>
      emitMetrics()
  }

  /** Resend next item in the buffer, or trigger next item in the feed, if no items in the buffer. */
  def processBufferOrFeed() = {
    // If buffer has more items, and head has not already been resent, send next one, otherwise get next from feed.
    runBuffer.dequeueOption match {
      case Some((run, _)) => //run the first from buffer
        implicit val tid = run.msg.transid
        //avoid sending dupes
        if (resent.isEmpty) {
          logging.info(this, s"re-processing from buffer (${runBuffer.length} items in buffer)")
          resent = Some(run)
          self ! run
        } else {
          //do not resend the buffer head multiple times (may reach this point from multiple messages, before the buffer head is re-processed)
        }
      case None => //feed me!
        feed ! MessageFeed.Processed
    }
  }

  /** Install prewarm containers up to the configured requirements for each kind/memory combination. */
  def backfillPrewarms(init: Boolean) = {
    prewarmConfig.foreach { config =>
      val kind = config.exec.kind
      val memory = config.memoryLimit
      val currentCount = prewarmedPool.count {
        case (_, PreWarmedData(_, `kind`, `memory`, _)) => true //done starting
        case _                                          => false //started but not finished starting
      }
      val startingCount = prewarmStartingPool.count(p => p._2._1 == kind && p._2._2 == memory)
      val containerCount = currentCount + startingCount
      if (containerCount < config.count) {
        logging.info(
          this,
          s"found ${currentCount} started and ${startingCount} starting; ${if (init) "initing" else "backfilling"} ${config.count - containerCount} pre-warms to desired count: ${config.count} for kind:${config.exec.kind} mem:${config.memoryLimit.toString}")(
          TransactionId.invokerWarmup)
        (containerCount until config.count).foreach { _ =>
          prewarmContainer(config.exec, config.memoryLimit)
        }
      }
    }
  }

  /** Creates a new container and updates state accordingly. */
  def createContainer(memoryLimit: ByteSize): (ActorRef, ContainerData) = {
    val before = System.currentTimeMillis()
    val ref = childFactory(context)
    val data = MemoryData(memoryLimit)
    freePool = freePool + (ref -> data)
    val after = System.currentTimeMillis()
    logging.info(this, s"Container created at ${before} finished at ${after} took ${after - before} ms")
    ref -> data
  }

  /** Creates a new prewarmed container */
  def prewarmContainer(exec: CodeExec[_], memoryLimit: ByteSize): Unit = {
    val before = System.currentTimeMillis()
    // prewarmStartingPoolStartTimes = prewarmStartingPoolStartTimes + (newContainer -> before)
    val newContainer = childFactory(context)
    prewarmStartingPool = prewarmStartingPool + (newContainer -> (exec.kind, memoryLimit))
    val after = System.currentTimeMillis()
    logging.info(this, s"Container prewarm created at ${before} finished at ${after} took ${after - before} ms")
    newContainer ! Start(exec, memoryLimit)
  }

  /**
   * Takes a prewarm container out of the prewarmed pool
   * iff a container with a matching kind and memory is found.
   *
   * @param action the action that holds the kind and the required memory.
   * @return the container iff found
   */
  def takePrewarmContainer(action: ExecutableWhiskAction): Option[(ActorRef, ContainerData)] = {
    val kind = action.exec.kind
    val memory = action.limits.memory.megabytes.MB
    prewarmedPool
      .find {
        case (_, PreWarmedData(_, `kind`, `memory`, _)) => true
        case _                                          => false
      }
      .map {
        case (ref, data) =>
          // Move the container to the usual pool
          freePool = freePool + (ref -> data)
          prewarmedPool = prewarmedPool - ref
          // Create a new prewarm container
          // NOTE: prewarming ignores the action code in exec, but this is dangerous as the field is accessible to the
          // factory
          prewarmContainer(action.exec, memory)
          (ref, data)
      }
  }

  /** Removes a container and updates state accordingly. */
  def removeContainer(toDelete: ActorRef) = {
    toDelete ! Remove
    logging.info(this, s"removing container ${toDelete}")
    freePool = freePool - toDelete
    busyPool = busyPool - toDelete
  }

  /**
   * Calculate if there is enough free memory within a given pool.
   *
   * @param pool The pool, that has to be checked, if there is enough free memory.
   * @param memory The amount of memory to check.
   * @return true, if there is enough space for the given amount of memory.
   */
  def hasPoolSpaceFor[A](pool: Map[A, ContainerData], memory: ByteSize): Boolean = {
    logging.info(this, s"system/user? has allowed memory of ${poolConfig.userMemory.toMB}, poolConfig: ${poolConfig}")
    memoryConsumptionOf(pool) + memory.toMB <= poolConfig.userMemory.toMB
  }

  /**
   * Log metrics about pool state (buffer size, buffer memory requirements, active number, active memory, prewarm number, prewarm memory)
   */
  private def emitMetrics() = {
    MetricEmitter.emitGaugeMetric(LoggingMarkers.CONTAINER_POOL_RUNBUFFER_COUNT, runBuffer.size)
    MetricEmitter.emitGaugeMetric(
      LoggingMarkers.CONTAINER_POOL_RUNBUFFER_SIZE,
      runBuffer.map(_.action.limits.memory.megabytes).sum)
    val containersInUse = freePool.filter(_._2.activeActivationCount > 0) ++ busyPool
    MetricEmitter.emitGaugeMetric(LoggingMarkers.CONTAINER_POOL_ACTIVE_COUNT, containersInUse.size)
    MetricEmitter.emitGaugeMetric(
      LoggingMarkers.CONTAINER_POOL_ACTIVE_SIZE,
      containersInUse.map(_._2.memoryLimit.toMB).sum)
    MetricEmitter.emitGaugeMetric(LoggingMarkers.CONTAINER_POOL_PREWARM_COUNT, prewarmedPool.size)
    MetricEmitter.emitGaugeMetric(
      LoggingMarkers.CONTAINER_POOL_PREWARM_SIZE,
      prewarmedPool.map(_._2.memoryLimit.toMB).sum)
    val unused = freePool.filter(_._2.activeActivationCount == 0)
    val unusedMB = unused.map(_._2.memoryLimit.toMB).sum
    MetricEmitter.emitGaugeMetric(LoggingMarkers.CONTAINER_POOL_IDLES_COUNT, unused.size)
    MetricEmitter.emitGaugeMetric(LoggingMarkers.CONTAINER_POOL_IDLES_SIZE, unusedMB)
  }
}

object ContainerPool {

  /**
   * Calculate the memory of a given pool.
   *
   * @param pool The pool with the containers.
   * @return The memory consumption of all containers in the pool in Megabytes.
   */
  protected[containerpool] def memoryConsumptionOf[A](pool: Map[A, ContainerData]): Long = {
    pool.map(_._2.memoryLimit.toMB).sum
  }

  /**
   * Finds the best container for a given job to run on.
   *
   * Selects an arbitrary warm container from the passed pool of idle containers
   * that matches the action and the invocation namespace. The implementation uses
   * matching such that structural equality of action and the invocation namespace
   * is required.
   * Returns None iff no matching container is in the idle pool.
   * Does not consider pre-warmed containers.
   *
   * @param action the action to run
   * @param invocationNamespace the namespace, that wants to run the action
   * @param idles a map of idle containers, awaiting work
   * @return a container if one found
   */
  protected[containerpool] def schedule[A](action: ExecutableWhiskAction,
                                           invocationNamespace: EntityName,
                                           idles: Map[A, ContainerData]): Option[(A, ContainerData)] = {
    idles
      .find {
        case (_, c @ WarmedData(_, `invocationNamespace`, `action`, _, _, _)) if c.hasCapacity() => true
        case _                                                                                   => false
      }
      .orElse {
        idles.find {
          case (_, c @ WarmingData(_, `invocationNamespace`, `action`, _, _)) if c.hasCapacity() => true
          case _                                                                                 => false
        }
      }
      .orElse {
        idles.find {
          case (_, c @ WarmingColdData(`invocationNamespace`, `action`, _, _)) if c.hasCapacity() => true
          case _                                                                                  => false
        }
      }
  }

  /**
   * Finds the oldest previously used container to remove to make space for the job passed to run.
   * Depending on the space that has to be allocated, several containers might be removed.
   *
   * NOTE: This method is never called to remove an action that is in the pool already,
   * since this would be picked up earlier in the scheduler and the container reused.
   *
   * @param pool a map of all free containers in the pool
   * @param memory the amount of memory that has to be freed up
   * @return a list of containers to be removed iff found
   */
  @tailrec
  protected[containerpool] def remove[A](pool: Map[A, ContainerData],
                                         memory: ByteSize,
                                         toRemove: List[A] = List.empty): List[A] = {
    // Try to find a Free container that does NOT have any active activations AND is initialized with any OTHER action
    val freeContainers = pool.collect {
      // Only warm containers will be removed. Prewarmed containers will stay always.
      case (ref, w: WarmedData) if w.activeActivationCount == 0 =>
        ref -> w
    }

    if (memory > 0.B && freeContainers.nonEmpty && memoryConsumptionOf(freeContainers) >= memory.toMB) {
      // Remove the oldest container if:
      // - there is more memory required
      // - there are still containers that can be removed
      // - there are enough free containers that can be removed
      val (ref, data) = freeContainers.minBy(_._2.lastUsed)
      // Catch exception if remaining memory will be negative
      val remainingMemory = Try(memory - data.memoryLimit).getOrElse(0.B)
      remove(freeContainers - ref, remainingMemory, toRemove ++ List(ref))
    } else {
      // If this is the first call: All containers are in use currently, or there is more memory needed than
      // containers can be removed.
      // Or, if this is one of the recursions: Enough containers are found to get the memory, that is
      // necessary. -> Abort recursion
      toRemove
    }
  }

  def props(factory: ActorRefFactory => ActorRef,
            poolConfig: ContainerPoolConfig,
            feed: ActorRef,
            prewarmConfig: List[PrewarmingConfig] = List.empty) =
    Props(new ContainerPool(factory, feed, prewarmConfig, poolConfig))
}

/** Contains settings needed to perform container prewarming. */
case class PrewarmingConfig(count: Int, exec: CodeExec[_], memoryLimit: ByteSize)
