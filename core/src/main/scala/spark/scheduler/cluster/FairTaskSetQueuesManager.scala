package spark.scheduler.cluster

import java.io.{File, FileInputStream, FileOutputStream, PrintWriter}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.immutable.TreeSet
import scala.util.control.Breaks._
import scala.xml._
import scala.compat.Platform

import spark.Logging

/**
 * A Fair Implementation of the TaskSetQueuesManager
 * 
 * The current implementation makes the following assumptions: A pool has a fixed configuration of weight. 
 * Within a pool, it just uses FIFO.
 * Also, currently we assume that pools are statically defined
 * We currently don't support min shares
 */
private[spark] class FairTaskSetQueuesManager extends TaskSetQueuesManager with Logging {
  
  val schedulerAllocFile = System.getProperty("spark.fairscheduler.allocation.file","unspecified")  
  val poolNameToPool= new HashMap[String, Pool]
  val pools = new ArrayBuffer[Pool]
  
  val poolNames = new ArrayBuffer[String]
  val poolsForLogging = new ArrayBuffer[Pool]
  
  loadPoolProperties()
  
  def loadPoolProperties() {
    //first check if the file exists
    val file = new File(schedulerAllocFile)
    if(!file.exists()) {
    //if file does not exist, we just create 1 pool, default
      val pool = new Pool("default",100)
      pools += pool
      poolNameToPool("default") = pool
      logInfo("Created a default pool with weight = 100")
      poolsForLogging += pool
      poolNames += "default"
    }
    else {
      val xml = XML.loadFile(file)
      for (poolNode <- (xml \\ "pool")) {
        if((poolNode \ "weight").text != ""){
          val pool = new Pool((poolNode \ "@name").text,(poolNode \ "weight").text.toInt)
          pools += pool
          poolNameToPool((poolNode \ "@name").text) = pool
          logInfo("Created pool "+ pool.name +"with weight = "+pool.weight)
          poolsForLogging += pool
          poolNames += ((poolNode \"@name").text)
        } else {
          val pool = new Pool((poolNode \ "@name").text,100)
          pools += pool
          poolNameToPool((poolNode \ "@name").text) = pool
          logInfo("Created pool "+ pool.name +"with weight = 100")
          poolsForLogging += pool
          poolNames += ((poolNode \"@name").text)
        }
      }
      val listOfPools = new ArrayBuffer[String]
      listOfPools.appendAll(poolNames)
      loadCombinations(listOfPools)
      
      if(!poolNameToPool.contains("default")) {
        val pool = new Pool("default", 100)
        pools += pool
        poolNameToPool("default") = pool
        logInfo("Created a default pool with weight = 100")
        poolsForLogging += pool
        poolNames += "default"
      }
        
    }    
  }

  /**
   * A Utility function that generates combinations of the pools 
   * and load it into the "list of pools" with weight = the sum of the pools we combined it from
   */
  def loadCombinations(listOfPools: ArrayBuffer[String]) {
    val listCnt = listOfPools.size
    val power = Math.pow(2, listCnt).toInt
    val treeSetOrdering = new Ordering[TreeSet[Int]] {
      def compare(t1: TreeSet[Int], t2: TreeSet[Int]): Int = {
        if(t1.size < t2.size) {
          return -1
        } else if(t1.size > t2.size) {
          return 1
        } else {
          val t1Iterator = t1.iterator
          val t2Iterator = t2.iterator          
          while(t1Iterator.hasNext) {
            val t1Item = t1Iterator.next
            val t2Item = t2Iterator.next
            if(t1Item < t2Item) {
              return -1
            } else if(t1Item > t2Item) {
              return 1
            }  
          }
          return 0
        }
      }
    }        
    var combinationsSet = new TreeSet[TreeSet[Int]]()(treeSetOrdering)
    for(outer <- 1 until power ) {
      var member = new TreeSet[Int]
      for(inner <- 1 until listCnt+1) {
        val pow = Math.pow(2,inner - 1).toInt
        if((outer & pow) != 0) {
          member += (inner - 1)         
          if(member.size > 1) {
            combinationsSet += member
          }
        }
      }
    }
    for(currComb <- combinationsSet) {
      var combPoolName = ""
      var delim = ""
      var weight = 0  
      for(indx <- currComb) {
        val poolName = listOfPools(indx)
        combPoolName += delim+poolName
        weight = weight + poolNameToPool(poolName).weight
        delim = "|"
      }
      val pool = new Pool(combPoolName,weight)
      pools += pool
      poolNameToPool(combPoolName) = pool
      logInfo("Created pool "+ pool.name +"with weight = "+weight)
      poolsForLogging += pool
      poolNames += combPoolName
    }            
  }
  
  override def addTaskSetManager(manager: TaskSetManager) {
    var poolName = "default"
    if(manager.taskSet.properties != null)  
      poolName = manager.taskSet.properties.getProperty("spark.scheduler.cluster.fair.pool","default")
    if(poolNameToPool.contains(poolName)) {           		
      poolNameToPool(poolName).activeTaskSetsQueue += manager
      if(poolName.contains("|")) {
        val splits = poolName.split("\\|")
        for(split <- splits) {
          poolNameToPool(split).isDisabled = true
        }
      }      
    } else
      poolNameToPool("default").activeTaskSetsQueue += manager
    logInfo("Added task set " + manager.taskSet.id + " tasks to pool "+poolName)
        
  }
  
  override def removeTaskSetManager(manager: TaskSetManager) {
    var poolName = "default"
    if(manager.taskSet.properties != null)  
      poolName = manager.taskSet.properties.getProperty("spark.scheduler.cluster.fair.pool","default")
    if(poolNameToPool.contains(poolName)) {
      poolNameToPool(poolName).activeTaskSetsQueue -= manager
      if(poolName.contains("|") && poolNameToPool(poolName).activeTaskSetsQueue.size <= 0) {        
        val splits = poolName.split("\\|")
        for(split <- splits) {
          poolNameToPool(split).isDisabled = false
        }
      }      
    } else
      poolNameToPool("default").activeTaskSetsQueue -= manager      
  }
  
  override def taskFinished(manager: TaskSetManager) {
    var poolName = "default"
    var anycached = "false"
    if(manager.taskSet.properties != null) {  
      poolName = manager.taskSet.properties.getProperty("spark.scheduler.cluster.fair.pool","default")
      anycached = manager.taskSet.properties.getProperty("spark.stage.anycached")
    }
    if(poolNameToPool.contains(poolName)) {
      poolNameToPool(poolName).numRunningTasks -= 1
      if(anycached == "true") {
        poolNameToPool(poolName).numRunningTasksUsingCache -= 1
      }
    } else {
      poolNameToPool("default").numRunningTasks -= 1
      if(anycached == "true") {
        poolNameToPool(poolName).numRunningTasksUsingCache -= 1
      }
    }
  }
  
  override def removeExecutor(executorId: String, host: String) {
    for (pool <- pools) {
      pool.activeTaskSetsQueue.foreach(_.executorLost(executorId, host))      
    }    
  }
  
  /**
   * This is the comparison function used for sorting to determine which 
   * pool to allocate next based on fairness.
   * The algorithm is as follows: we sort by the pool's running tasks to weight ratio
   * (pools number running tast / pool's weight)
   */
  def poolFairCompFn(pool1: Pool, pool2: Pool): Boolean = {
    if(pool1.isDisabled && !pool2.isDisabled) { //Not disabled gets priority (pool 2)
      return false
    } else if(!pool1.isDisabled && pool2.isDisabled) { //Not Disabled gets priority (pool 1)
      return true
    } else { //If both are the same then we compare both using their tasks to weight ratio
      val tasksToWeightRatio1 = pool1.numRunningTasks.toDouble / pool1.weight.toDouble 
      val tasksToWeightRatio2 = pool2.numRunningTasks.toDouble / pool2.weight.toDouble
      var res = Math.signum(tasksToWeightRatio1 - tasksToWeightRatio2)
      if (res == 0) {
        //Jobs are tied in fairness ratio. We break the tie by name
        res = pool1.name.compareTo(pool2.name)
      }
      if (res < 0)
        return true
      else
        return false
    }    
  }
  
  override def receiveOffer(tasks: Seq[ArrayBuffer[TaskDescription]], offers: Seq[WorkerOffer]): Seq[Seq[String]] = {
    val taskSetIds = offers.map(o => new ArrayBuffer[String](o.cores))
    val availableCpus = offers.map(o => o.cores).toArray
    var launchedTask = false
      
    for (i <- 0 until offers.size) { //we loop through the list of offers
      val execId = offers(i).executorId
      val host = offers(i).hostname
      var breakOut = false
      while(availableCpus(i) > 0 && !breakOut) {
        breakable{
          launchedTask = false          
          for (pool <- pools.sortWith(poolFairCompFn)) { //we loop through the list of pools
            if(!pool.activeTaskSetsQueue.isEmpty) {
              //sort the tasksetmanager in the pool
              pool.activeTaskSetsQueue.sortBy(m => (m.taskSet.priority, m.taskSet.stageId))
              for(manager <- pool.activeTaskSetsQueue) { //we loop through the activeTaskSets in this pool
                //Make an offer
                manager.slaveOffer(execId, host, availableCpus(i)) match {
                    case Some(task) =>
                      tasks(i) += task
                      taskSetIds(i) += manager.taskSet.id
                      availableCpus(i) -= 1
                      pool.numRunningTasks += 1
                      if(manager.taskSet.properties != null && manager.taskSet.properties.getProperty("spark.stage.anycached") == "true") {
                        pool.numRunningTasksUsingCache += 1
                      }
                      launchedTask = true
                      logInfo("launched task for pool"+pool.name);
                      break
                    case None => {}
                }
              }
            }
          }
          //If there is not one pool that can assign the task then we have to exit the outer loop and continue to the next offer
          if(!launchedTask){
            breakOut = true
          }              
        }
      }
    }
    return taskSetIds    
  }

  override def checkSpeculatableTasks(): Boolean = {
    var shouldRevive = false
    for (pool <- pools) {
      for (ts <- pool.activeTaskSetsQueue) {
        shouldRevive |= ts.checkSpeculatableTasks()
      }       
    }
    return shouldRevive
  }
  
  val logFile = System.getProperty("spark.fairscheduler.fairness.logfile","unspecified")
  val logInterval = System.getProperty("spark.fairscheduler.fairness.loginterval","1000").toLong
  
  startLogging()
  
  def startLogging() {
    if(logFile != "unspecified") {
      val t = new Thread(new Runnable {
        def run() {
          val file = new File(logFile)
          val writer = new PrintWriter(new FileOutputStream(file), true)
          writer.print("time")
          val file_mem = new File(logFile+"-MEMORY")
          val writer_mem = new PrintWriter(new FileOutputStream(file_mem), true)
          writer_mem.print("time")
          for(name <- poolNames) {
            writer.print(","+name)
            writer_mem.print(","+name)
          }
          writer.println()
          writer_mem.println()
          val startTime: Long = Platform.currentTime
          while(true) {
            writer.print((Platform.currentTime - startTime))
            writer_mem.print((Platform.currentTime - startTime))
            for(pool <- poolsForLogging) {
              writer.print(","+pool.numRunningTasks)
              writer_mem.print(","+pool.numRunningTasksUsingCache)
            }
            writer.println
            writer_mem.println
            Thread.sleep(logInterval)
          }          
        }
      })
      t.setDaemon(true)
      t.start
    }
  }
  
}

/**
 * An internal representation of a pool. It contains an ArrayBuffer of TaskSets and also weight
 */
class Pool(val name: String, val weight: Int)
{
  var activeTaskSetsQueue = new ArrayBuffer[TaskSetManager]
  var numRunningTasks: Int = 0
  var numRunningTasksUsingCache: Int = 0
  var isDisabled: Boolean = false
}