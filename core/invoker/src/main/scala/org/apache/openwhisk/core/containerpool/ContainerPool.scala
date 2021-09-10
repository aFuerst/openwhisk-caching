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
import org.apache.openwhisk.common.{Logging, LoggingMarkers, MetricEmitter, TransactionId}
import org.apache.openwhisk.core.connector.MessageFeed
import org.apache.openwhisk.core.entity.ExecManifest.ReactivePrewarmingConfig
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.entity.size._

import scala.annotation.tailrec
import scala.collection.immutable
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import java.time.Instant

sealed trait WorkerState
case object Busy extends WorkerState
case object Free extends WorkerState
import scala.util.{Random, Try}

case class ColdStartKey(kind: String, memory: ByteSize)

case object EmitMetrics

case object AdjustPrewarmedContainer

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
                    poolConfig: ContainerPoolConfig)(implicit val logging: Logging)
    extends Actor {
  import ContainerPool.memoryConsumptionOf

  implicit val ec = context.dispatcher

  var freePool = immutable.Map.empty[ActorRef, ContainerData]
  var busyPool = immutable.Map.empty[ActorRef, ContainerData]
  var prewarmedPool = immutable.Map.empty[ActorRef, PreWarmedData]
  var prewarmStartingPool = immutable.Map.empty[ActorRef, (String, ByteSize)]
  
  var actorRunning = mutable.Map.empty[ActorRef, ExecutableWhiskAction]
  var actorStartTimes = mutable.Map.empty[ActorRef, (Long, String)] // ActorRef, (time, state)

  // var coldTimes = mutable.Map.empty[ExecutableWhiskAction, Long]  // action-key, time
  // var warmTimes = mutable.Map.empty[ExecutableWhiskAction, Long]  // action-key, time
  // var lastCalled = mutable.Map.empty[ActorRef, Double] // action-key, time

  // var callCount = mutable.Map.empty[ExecutableWhiskAction, Long] // action-key, count
  // var activeActionCount = mutable.Map.empty[ExecutableWhiskAction, Long] // action-key, count
  var clock : Double = 0.0;

  var warmHits : Long = 0;
  var coldHits : Long = 0;

  var warmHitsAct = mutable.Map.empty[ExecutableWhiskAction, Long] // action-key, count
  var coldHitsAct = mutable.Map.empty[ExecutableWhiskAction, Long] // action-key, count

  var priorities = mutable.Map.empty[ExecutableWhiskAction, TrackedAction] // action-key, count

  // If all memory slots are occupied and if there is currently no container to be removed, than the actions will be
  // buffered here to keep order of computation.
  // Otherwise actions with small memory-limits could block actions with large memory limits.
  var runBuffer = immutable.Queue.empty[Run]
  // Track the resent buffer head - so that we don't resend buffer head multiple times
  var resent: Option[Run] = None
  val logMessageInterval = 10.seconds
  //periodically emit metrics (don't need to do this for each message!)
  context.system.scheduler.scheduleAtFixedRate(30.seconds, 10.seconds, self, EmitMetrics)

  // Key is ColdStartKey, value is the number of cold Start in minute
  var coldStartCount = immutable.Map.empty[ColdStartKey, Int]

  adjustPrewarmedContainer(true, false)

  // check periodically, adjust prewarmed container(delete if unused for some time and create some increment containers)
  // add some random amount to this schedule to avoid a herd of container removal + creation
  val interval = poolConfig.prewarmExpirationCheckInterval + poolConfig.prewarmExpirationCheckIntervalVariance
    .map(v =>
      Random
        .nextInt(v.toSeconds.toInt))
    .getOrElse(0)
    .seconds
  if (prewarmConfig.exists(!_.reactive.isEmpty)) {
    context.system.scheduler.scheduleAtFixedRate(
      poolConfig.prewarmExpirationCheckInitDelay,
      interval,
      self,
      AdjustPrewarmedContainer)
  }

  // def cost(action: ExecutableWhiskAction): Long = {
  //   val c = (coldTimes get action).getOrElse(0L)
  //   val w = (warmTimes get action).getOrElse(0L)
  //   // logging.info(this, s"cost of action ${action} is ${c} - ${w} = ${c-w}")
  //   Math.max(c - w, 0)
  // }

  def getClock(): Double = {
    clock
  }

  def setClock(priority: Double) = {
    clock = Math.max(priority, clock)
  }

  def getMilis(): Long = {
    Instant.now().toEpochMilli()
  }

  // def getContainerActionKey(container: Option[Container], action: ExecutableWhiskAction): String = {
  //   container match {
  //     case Some(cont) => s"${cont}-${action.namespace}/${action.name}"
  //     case None => s"None-${action.namespace}/${action.name}"
  //   }
  // }

  // def calcPriority(actor: ActorRef) : Double = {
  //   def size(action:ExecutableWhiskAction):Double = {
  //       action.limits.memory.megabytes
  //   }

  //   def pri(actor: ActorRef):Double = {
  //     val action = actorRunning get actor
  //     action match {
  //       case Some(a) => (lastCalled get actor).getOrElse(0.0) + (((callCount get a).getOrElse(0L)*cost(a))/size(a))
  //       case None => 0.0
  //     }
      
  //   }
  //   pri(actor)
  // }

  def calcPri(actor: ActorRef, data: ContainerData) : (Double, (ActorRef, ContainerData)) = {
      val action = actorRunning get actor
       action match {
        case Some(a) => (priorities.getOrElse(a, new TrackedAction()).priority(), (actor, data))
        case None => (0.0, (actor, data))
      }
  }

  def sortOnPriorities(pool: Map[ActorRef, ContainerData]) : ListBuffer[(ActorRef, ContainerData)] = {
    var unordered = ListBuffer[(Double, (ActorRef, ContainerData))]()
    pool.foreach{case (actor, data) =>
      // unordered += ((priorities get action).priority(), (actor, data))
      unordered += calcPri(actor, data)
    }
    var ordered = unordered.sortWith((l, r) => l._1 < r._1)
    // ordered.foreach{case (pri, (actor, data)) =>
    //   logging.info(this, s"ordered; priority: ${pri}, data: ${data}")
    // }
    for (item <- ordered) yield item._2
  }

  def logContainerStart(r: Run, containerState: String, activeActivations: Int, container: Option[Container]): Unit = {
    val namespaceName = r.msg.user.namespace.name.asString
    val actionName = r.action.name.name
    val actionNamespace = r.action.namespace.namespace
    val maxConcurrent = r.action.limits.concurrency.maxConcurrent
    val activationId = r.msg.activationId.toString

    r.msg.transid.mark(
      this,
      LoggingMarkers.INVOKER_CONTAINER_START(containerState, namespaceName, actionNamespace, actionName),
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

      var priority_data = priorities.getOrElse(r.action, new TrackedAction())
      priority_data.memory = r.action.limits.memory.megabytes;


      // Only process request, if there are no other requests waiting for free slots, or if the current request is the
      // next request to process
      // It is guaranteed, that only the first message on the buffer is resent.
      if (runBuffer.isEmpty || isResentFromBuffer) {
        if (isResentFromBuffer) {
          //remove from resent tracking - it may get resent again, or get processed
          resent = None
        }
        val kind = r.action.exec.kind
        val memory = r.action.limits.memory.megabytes.MB

        val createdContainer =
            // Schedule a job to a warm container
            ContainerPool
              .schedule(r.action, r.msg.user.namespace.name, freePool)
              .map(container => (container, container._2.initingState)) //warmed, warming, and warmingCold always know their state
              .orElse(
                // There was no warm/warming/warmingCold container. Try to take a prewarm container or a cold container.
                // When take prewarm container, has no need to judge whether user memory is enough
                takePrewarmContainer(r.action)
                  .map(container => (container, "prewarmed"))
                  .orElse {
                    // Is there enough space to create a new container or do other containers have to be removed?
                    if (hasPoolSpaceFor(busyPool ++ freePool ++ prewarmedPool, prewarmStartingPool, memory)) {
                      val container = Some(createContainer(memory), "cold")
                      incrementColdStartCount(kind, memory)
                      container
                  } else {
                    None
                  }
                  })
              .orElse(
                // Remove a container and create a new one for the given job
                ContainerPool
                // Only free up the amount, that is really needed to free up
                  .remove2Boogaloo(sortOnPriorities(freePool), Math.min(r.action.limits.memory.megabytes, memoryConsumptionOf(freePool)).MB)
                  .map(removeContainer2Boogaloo)
                  // If the list had at least one entry, enough containers were removed to start the new container. After
                  // removing the containers, we are not interested anymore in the containers that have been removed.
                  .headOption
                  .map(_ =>
                    takePrewarmContainer(r.action)
                      .map(container => (container, "recreatedPrewarm"))
                      .getOrElse {
                        val container = (createContainer(memory), "recreated")
                        incrementColdStartCount(kind, memory)
                        container
                      }))

        createdContainer match {
          case Some(((actor, data), containerState)) =>
            // logging.info(this, s"assigning work to actor ${actor}")
            //increment active count before storing in pool map
            val newData = data.nextRun(r)
            val container = newData.getContainer
            actorRunning += (actor -> r.action)

            if (newData.activeActivationCount < 1) {
              logging.error(this, s"invalid activation count < 1 ${newData}")
            }
            // var i = (activeActionCount get r.action).getOrElse(0L)

            priority_data.invocations += 1
            // logging.info(this, s"active action count for action ${r.action} is ${i}")
            if (containerState == "cold" || containerState == "recreated") {          
              priority_data.active += 1
              // activeActionCount += (r.action -> (i+1))
              coldHits += 1
              val t = coldHitsAct get r.action
              t match {
                case Some(long) => coldHitsAct += (r.action -> (long + 1L))
                case None => coldHitsAct += (r.action -> 1L)
              }            }
            else if (containerState == "warmed") {
              warmHits += 1
              val t = warmHitsAct get r.action
              t match {
                case Some(long) => warmHitsAct += (r.action -> (long + 1L))
                case None => warmHitsAct += (r.action -> 1L)
              }
            }
            else {
              logging.error(this, s"unknown container state $containerState")
            }
            logging.info(this, s"cold hits: $coldHits, warm hits:$warmHits, cold mapits: $coldHitsAct, warm map: $warmHitsAct")
            
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

            // var cnt = (callCount get r.action).getOrElse(0L)
            priority_data.invocations += 1
            priority_data.lastcalled = getClock()
            // callCount -= r.action
            // callCount += (r.action -> (cnt+1))
            // lastCalled -= actor
            // lastCalled += (actor -> getClock())

            actorStartTimes += (actor -> (getMilis(), containerState))
            actor ! r // forwards the run request to the container
            logContainerStart(r, containerState, newData.activeActivationCount, container)
            priorities += (r.action -> priority_data)
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
      // logging.info(this, s"work completed sender: ${sender}, warmData: ${warmData}")
      val oldData = freePool.get(sender()).getOrElse(busyPool(sender()))
      val newData =
        warmData.copy(lastUsed = oldData.lastUsed, activeActivationCount = oldData.activeActivationCount - 1)
      if (newData.activeActivationCount < 0) {
        logging.error(this, s"invalid activation count after warming < 1 ${newData}")
      }
      if (newData.hasCapacity()) {
        val found = actorStartTimes get sender
        val now = getMilis()
        var priority_data = priorities.getOrElse(warmData.action, new TrackedAction())
        found match {
          case Some((startTime, state)) => {
            if (state == "warmed")
            {
              // val old = (warmTimes get warmData.action).getOrElse(0L)
              priority_data.warmTime = Math.max(priority_data.warmTime, (now - startTime))
              // warmTimes += (warmData.action -> Math.max(old, (now - startTime)))
              // logging.info(this, s"Warm action ${warmData.action} finished in ${now - startTime}")
            }
            else
            {
              // val old = (coldTimes get warmData.action).getOrElse(0L)
              // coldTimes += (warmData.action -> Math.max(old, (now - startTime)))
              priority_data.coldTime = Math.max(priority_data.coldTime, (now - startTime))
              // logging.info(this, s"Prewarmed action ${warmData.action} finished in ${now - startTime} from state ${state}")
            }
          }
          case None => logging.error(this, s"Unexpected data incoming ${warmData} from sender ${sender}")
        }

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
      // logging.info(this, s"prewarmed container arrived ${data}, ${sender}")
      prewarmStartingPool = prewarmStartingPool - sender()
      prewarmedPool = prewarmedPool + (sender() -> data)

    // Container got removed
    case ContainerRemoved(replacePrewarm) =>
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

      // in case this was a prewarm
      prewarmedPool.get(sender()).foreach { data =>
        prewarmedPool = prewarmedPool - sender()
      }

      // in case this was a starting prewarm
      prewarmStartingPool.get(sender()).foreach { _ =>
        logging.info(this, "failed starting prewarm, removed")
        prewarmStartingPool = prewarmStartingPool - sender()
      }

      //backfill prewarms on every ContainerRemoved(replacePrewarm = true), just in case
      if (replacePrewarm) {
        adjustPrewarmedContainer(false, false) //in case a prewarm is removed due to health failure or crash
      }

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

    case AdjustPrewarmedContainer =>
      adjustPrewarmedContainer(false, true)
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

  /** adjust prewarm containers up to the configured requirements for each kind/memory combination. */
  def adjustPrewarmedContainer(init: Boolean, scheduled: Boolean): Unit = {
    if (scheduled) {
      //on scheduled time, remove expired prewarms
      ContainerPool.removeExpired(poolConfig, prewarmConfig, prewarmedPool).foreach { p =>
        prewarmedPool = prewarmedPool - p
        p ! Remove
      }
      //on scheduled time, emit cold start counter metric with memory + kind
      coldStartCount foreach { coldStart =>
        val coldStartKey = coldStart._1
        MetricEmitter.emitCounterMetric(
          LoggingMarkers.CONTAINER_POOL_PREWARM_COLDSTART(coldStartKey.memory.toString, coldStartKey.kind))
      }
    }
    //fill in missing prewarms (replaces any deletes)
    ContainerPool
      .increasePrewarms(init, scheduled, coldStartCount, prewarmConfig, prewarmedPool, prewarmStartingPool)
      .foreach { c =>
        val config = c._1
        val currentCount = c._2._1
        val desiredCount = c._2._2
        if (currentCount < desiredCount) {
          (currentCount until desiredCount).foreach { _ =>
            prewarmContainer(config.exec, config.memoryLimit, config.reactive.map(_.ttl))
          }
        }
      }
    if (scheduled) {
      //   lastly, clear coldStartCounts each time scheduled event is processed to reset counts
      coldStartCount = immutable.Map.empty[ColdStartKey, Int]
    }
  }

  /** Creates a new container and updates state accordingly. */
  def createContainer(memoryLimit: ByteSize): (ActorRef, ContainerData) = {
    val before = System.currentTimeMillis()
    val ref = childFactory(context)
    val data = MemoryData(memoryLimit)
    freePool = freePool + (ref -> data)
    val after = System.currentTimeMillis()
    // logging.info(this, s"Container created at ${before} finished at ${after} took ${after - before} ms")
    ref -> data
  }

  /** Creates a new prewarmed container */
  def prewarmContainer(exec: CodeExec[_], memoryLimit: ByteSize, ttl: Option[FiniteDuration]): Unit = {
    if (hasPoolSpaceFor(busyPool ++ freePool ++ prewarmedPool, prewarmStartingPool, memoryLimit)) {
    // val before = System.currentTimeMillis()
    val newContainer = childFactory(context)
    prewarmStartingPool = prewarmStartingPool + (newContainer -> (exec.kind, memoryLimit))
    // val after = System.currentTimeMillis()
    // logging.info(this, s"Container prewarm created at ${before} finished at ${after} took ${after - before} ms")      newContainer ! Start(exec, memoryLimit, ttl)
    } else {
      logging.warn(
        this,
        s"Cannot create prewarm container due to reach the invoker memory limit: ${poolConfig.userMemory.toMB}")
    }
  }

  /** this is only for cold start statistics of prewarm configs, e.g. not blackbox or other configs. */
  def incrementColdStartCount(kind: String, memoryLimit: ByteSize): Unit = {
    prewarmConfig
      .filter { config =>
        kind == config.exec.kind && memoryLimit == config.memoryLimit
      }
      .foreach { _ =>
        val coldStartKey = ColdStartKey(kind, memoryLimit)
        coldStartCount.get(coldStartKey) match {
          case Some(value) => coldStartCount = coldStartCount + (coldStartKey -> (value + 1))
          case None        => coldStartCount = coldStartCount + (coldStartKey -> 1)
        }
      }
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
    val now = Deadline.now
    prewarmedPool.toSeq
      .sortBy(_._2.expires.getOrElse(now))
      .find {
        case (_, PreWarmedData(_, `kind`, `memory`, _, _)) => true
        case _                                             => false
      }
      .map {
        case (ref, data) =>
          var priority_data = priorities.getOrElse(action, new TrackedAction())
          // var i = (activeActionCount get action).getOrElse(0L)
          // activeActionCount += (action -> (i+1))
          priority_data.active -= 1
          priorities += (action -> priority_data)          
          // Move the container to the usual pool
          freePool = freePool + (ref -> data)
          prewarmedPool = prewarmedPool - ref
          // Create a new prewarm container
          // NOTE: prewarming ignores the action code in exec, but this is dangerous as the field is accessible to the
          // factory

          //get the appropriate ttl from prewarm configs
          val ttl =
            prewarmConfig.find(pc => pc.memoryLimit == memory && pc.exec.kind == kind).flatMap(_.reactive.map(_.ttl))
          prewarmContainer(action.exec, memory, ttl)
          (ref, data)
      }
  }

  /** Removes a container and updates state accordingly. */
  def removeContainer2Boogaloo(toDelete: (ActorRef, ContainerData)) = {
    val possAction = toDelete._2 match {
      case WarmedData(_, _, act, _, _, _) => act
      case WarmingData(_, _, act, _, _) => act
      case other => None
    }
    possAction match {
      case action: ExecutableWhiskAction => {
        var priority_data = priorities.getOrElse(action, new TrackedAction())
        setClock(priority_data.priority())
        // warmTimes -= action
        // coldTimes -= action
        // var i = (activeActionCount get action).getOrElse(0L)
        // logging.info(this, s"active action count before removal of key ${action}: ${i}")
        if (priority_data.active <= 1)
        {
          priorities -= action
          // logging.info(this, s"removing ${action} from active action count and call count")
          // activeActionCount -= action
          // callCount -= action
        }
        else
        {
          priority_data.active -= 1
          priorities += (action -> priority_data)
        }
      }
      case other => logging.error(this, s"unkonwn action to remove possAction:${possAction}: toDelete:${toDelete}")
    }
    logging.info(this, s"removing container $toDelete")
    actorRunning -= toDelete._1
    toDelete._1 ! Remove
    // logging.info(this, s"removing container ${toDelete._1}")
    freePool = freePool - toDelete._1
    busyPool = busyPool - toDelete._1
  }


  /** Removes a container and updates state accordingly. */
  def removeContainer(toDelete: ActorRef) = {
    toDelete ! Remove
    // logging.info(this, s"removing container ${toDelete}")
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
  def hasPoolSpaceFor[A](pool: Map[A, ContainerData],
                         prewarmStartingPool: Map[A, (String, ByteSize)],
                         memory: ByteSize): Boolean = {
    // logging.info(this, s"${poolName} allocated: ${memoryConsumptionOf(pool)}, adding ${memory}, max: ${poolConfig}")
    memoryConsumptionOf(pool) + prewarmStartingPool.map(_._2._2.toMB).sum + memory.toMB <= poolConfig.userMemory.toMB
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
    MetricEmitter.emitGaugeMetric(
      LoggingMarkers.CONTAINER_POOL_PREWARM_COUNT,
      prewarmedPool.size + prewarmStartingPool.size)
    MetricEmitter.emitGaugeMetric(
      LoggingMarkers.CONTAINER_POOL_PREWARM_SIZE,
      prewarmedPool.map(_._2.memoryLimit.toMB).sum + prewarmStartingPool.map(_._2._2.toMB).sum)
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

  protected[containerpool] def memoryConsumptionOf2Boogaloo[A](pool: ListBuffer[(A, ContainerData)]): Long = {
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
   * Finds the lowest-priority used container to remove to make space for the job passed to run.
   * Depending on the space that has to be allocated, several containers might be removed.
   *
   * NOTE: This method is never called to remove an action that is in the pool already,
   * since this would be picked up earlier in the scheduler and the container reused.
   *
   * @param pool a _ priority sorted_ list of free containers to remove
   * @param memory the amount of memory that has to be freed up
   * @return a list of containers to be removed iff found
   */
  @tailrec
  protected[containerpool] def remove2Boogaloo[A](pool: ListBuffer[(A, ContainerData)],
                                         memory: ByteSize,
                                         toRemove: List[(A, ContainerData)] = List.empty): List[(A, ContainerData)] = {
    // Try to find a Free container that does NOT have any active activations AND is initialized with any OTHER action
    val freeContainers: ListBuffer[(A, ContainerData)] = pool.collect {
      // Only warm containers will be removed. Prewarmed containers will stay always.
      case (ref, w: WarmedData) if w.activeActivationCount == 0 =>
        ref -> w
    }

    if (memory > 0.B && freeContainers.nonEmpty && memoryConsumptionOf2Boogaloo(freeContainers) >= memory.toMB) {
      // Remove the lowest-priority container if:
      // - there is more memory required
      // - there are still containers that can be removed
      // - there are enough free containers that can be removed
      val (ref, data) = freeContainers.head
      // Catch exception if remaining memory will be negative
      val remainingMemory = Try(memory - data.memoryLimit).getOrElse(0.B)
      remove2Boogaloo(freeContainers.drop(1), remainingMemory, toRemove ++ List((ref, data)))
    } else {
      // If this is the first call: All containers are in use currently, or there is more memory needed than
      // containers can be removed.
      // Or, if this is one of the recursions: Enough containers are found to get the memory, that is
      // necessary. -> Abort recursion
      toRemove
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

  /**
   * Find the expired actor in prewarmedPool
   *
   * @param poolConfig
   * @param prewarmConfig
   * @param prewarmedPool
   * @param logging
   * @return a list of expired actor
   */
  def removeExpired[A](poolConfig: ContainerPoolConfig,
                       prewarmConfig: List[PrewarmingConfig],
                       prewarmedPool: Map[A, PreWarmedData])(implicit logging: Logging): List[A] = {
    val now = Deadline.now
    val expireds = prewarmConfig
      .flatMap { config =>
        val kind = config.exec.kind
        val memory = config.memoryLimit
        config.reactive
          .map { c =>
            val expiredPrewarmedContainer = prewarmedPool.toSeq
              .filter { warmInfo =>
                warmInfo match {
                  case (_, p @ PreWarmedData(_, `kind`, `memory`, _, _)) if p.isExpired() => true
                  case _                                                                  => false
                }
              }
              .sortBy(_._2.expires.getOrElse(now))

            if (expiredPrewarmedContainer.nonEmpty) {
              // emit expired container counter metric with memory + kind
              MetricEmitter.emitCounterMetric(LoggingMarkers.CONTAINER_POOL_PREWARM_EXPIRED(memory.toString, kind))
              logging.info(
                this,
                s"[kind: ${kind} memory: ${memory.toString}] ${expiredPrewarmedContainer.size} expired prewarmed containers")
            }
            expiredPrewarmedContainer.map(e => (e._1, e._2.expires.getOrElse(now)))
          }
          .getOrElse(List.empty)
      }
      .sortBy(_._2) //need to sort these so that if the results are limited, we take the oldest
      .map(_._1)
    if (expireds.nonEmpty) {
      logging.info(this, s"removing up to ${poolConfig.prewarmExpirationLimit} of ${expireds.size} expired containers")
      expireds.take(poolConfig.prewarmExpirationLimit).foreach { e =>
        prewarmedPool.get(e).map { d =>
          logging.info(this, s"removing expired prewarm of kind ${d.kind} with container ${d.container} ")
        }
      }
    }
    expireds.take(poolConfig.prewarmExpirationLimit)
  }

  /**
   * Find the increased number for the prewarmed kind
   *
   * @param init
   * @param scheduled
   * @param coldStartCount
   * @param prewarmConfig
   * @param prewarmedPool
   * @param prewarmStartingPool
   * @param logging
   * @return the current number and increased number for the kind in the Map
   */
  def increasePrewarms(init: Boolean,
                       scheduled: Boolean,
                       coldStartCount: Map[ColdStartKey, Int],
                       prewarmConfig: List[PrewarmingConfig],
                       prewarmedPool: Map[ActorRef, PreWarmedData],
                       prewarmStartingPool: Map[ActorRef, (String, ByteSize)])(
    implicit logging: Logging): Map[PrewarmingConfig, (Int, Int)] = {
    prewarmConfig.map { config =>
      val kind = config.exec.kind
      val memory = config.memoryLimit

      val runningCount = prewarmedPool.count {
        // done starting (include expired, since they may not have been removed yet)
        case (_, p @ PreWarmedData(_, `kind`, `memory`, _, _)) => true
        // started but not finished starting (or expired)
        case _ => false
      }
      val startingCount = prewarmStartingPool.count(p => p._2._1 == kind && p._2._2 == memory)
      val currentCount = runningCount + startingCount

      // determine how many are needed
      val desiredCount: Int =
        if (init) config.initialCount
        else {
          if (scheduled) {
            // scheduled/reactive config backfill
            config.reactive
              .map(c => getReactiveCold(coldStartCount, c, kind, memory).getOrElse(c.minCount)) //reactive -> desired is either cold start driven, or minCount
              .getOrElse(config.initialCount) //not reactive -> desired is always initial count
          } else {
            // normal backfill after removal - make sure at least minCount or initialCount is started
            config.reactive.map(_.minCount).getOrElse(config.initialCount)
          }
        }

      if (currentCount < desiredCount) {
        logging.info(
          this,
          s"found ${currentCount} started and ${startingCount} starting; ${if (init) "initing" else "backfilling"} ${desiredCount - currentCount} pre-warms to desired count: ${desiredCount} for kind:${config.exec.kind} mem:${config.memoryLimit.toString}")(
          TransactionId.invokerWarmup)
      }
      (config, (currentCount, desiredCount))
    }.toMap
  }

  /**
   * Get the required prewarmed container number according to the cold start happened in previous minute
   *
   * @param coldStartCount
   * @param config
   * @param kind
   * @param memory
   * @return the required prewarmed container number
   */
  def getReactiveCold(coldStartCount: Map[ColdStartKey, Int],
                      config: ReactivePrewarmingConfig,
                      kind: String,
                      memory: ByteSize): Option[Int] = {
    coldStartCount.get(ColdStartKey(kind, memory)).map { value =>
      // Let's assume that threshold is `2`, increment is `1` in runtimes.json
      // if cold start number in previous minute is `2`, requireCount is `2/2 * 1 = 1`
      // if cold start number in previous minute is `4`, requireCount is `4/2 * 1 = 2`
      math.min(math.max(config.minCount, (value / config.threshold) * config.increment), config.maxCount)
    }
  }

  def props(factory: ActorRefFactory => ActorRef,
            poolConfig: ContainerPoolConfig,
            feed: ActorRef,
            prewarmConfig: List[PrewarmingConfig] = List.empty)(implicit logging: Logging) =
    Props(new ContainerPool(factory, feed, prewarmConfig, poolConfig))
}

/** Contains settings needed to perform container prewarming. */
case class PrewarmingConfig(initialCount: Int,
                            exec: CodeExec[_],
                            memoryLimit: ByteSize,
                            reactive: Option[ReactivePrewarmingConfig] = None)
