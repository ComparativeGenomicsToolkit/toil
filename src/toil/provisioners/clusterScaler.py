# Copyright (C) 2015-2016 Regents of the University of California
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import
from __future__ import division

from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import map
from builtins import object
import json
import logging
import os
from collections import deque, defaultdict
from threading import Lock

import time
from bd2k.util.exceptions import require
from bd2k.util.retry import retry
from bd2k.util.threading import ExceptionalThread
from bd2k.util.throttle import throttle
from itertools import islice

from toil.batchSystems.abstractBatchSystem import AbstractScalableBatchSystem, NodeInfo
from toil.provisioners.abstractProvisioner import Shape
from toil.job import ServiceJobNode

logger = logging.getLogger(__name__)

logger.setLevel(logging.DEBUG)
import sys

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logging.getLogger().addHandler(ch)


class RecentJobShapes(object):
    """
    Used to track the 'shapes' of the last N jobs run (see Shape).
    """

    def __init__(self, config, nodeShape, N=1000):
        # As a prior we start of with 10 jobs each with the default memory, cores, and disk. To
        # estimate the running time we use the the default wall time of each node allocation,
        # so that one job will fill the time per node.
        self.jobShapes = deque(maxlen=N,
                               iterable=10 * [Shape(wallTime=nodeShape.wallTime,
                                                    memory=config.defaultMemory,
                                                    cores=config.defaultCores,
                                                    disk=config.defaultDisk,
                                                    preemptable=True)])
        # Calls to add and getLastNJobShapes may be concurrent
        self.lock = Lock()
        # Number of jobs to average over
        self.N = N

    def add(self, jobShape):
        """
        Adds a job shape as the last completed job.
        :param Shape jobShape: The memory, core and disk requirements of the completed job
        """
        with self.lock:
            self.jobShapes.append(jobShape)

    def get(self):
        """
        Gets the last N job shapes added.
        """
        with self.lock:
            return list(self.jobShapes)

class BinPackedFit(object):
    """
    Use a first fit decreasing (FFD) bin packing like algorithm to
    calculate an approximate minimum number of nodes that will fit the
    given list of jobs. To run the bin-packing algorithm, use the
    binPack method on a list of job shapes, and use getRequiredNodes
    to get how many nodes it thinks are required.

    :param list nodeShapes: A list of possible types of nodes that can be launched (as "Shape"s).
    :param targetTime: The time before which all jobs should at least be started.
    """
    def __init__(self, nodeShapes, targetTime=3600):
        self.nodeShapes = nodeShapes
        self.targetTime = targetTime
        self.nodeReservations = {nodeShape:[] for nodeShape in nodeShapes}  # The list of node reservations

    def binPack(self, jobShapes):
        """Pack a list of jobShapes into the fewest nodes reasonable. Can be run multiple times."""
        logger.debug('Running bin packing for node shapes %s and %s job(s).', self.nodeShapes, len(jobShapes))
        # Sort in descending order from largest to smallest. The FFD like-strategy will pack the jobs in order from longest
        # to shortest.
        jobShapes.sort()
        jobShapes.reverse()
        assert len(jobShapes) == 0 or jobShapes[0] >= jobShapes[-1]
        for jS in jobShapes:
            self.addJobShape(jS)

    def addJobShape(self, jobShape):
        """
        Function adds the job to the first node reservation in which it will fit (this
        is the bin-packing aspect)
        """
        chosenNodeShape = None
        for nodeShape in self.nodeShapes:
            if NodeReservation(nodeShape).fits(jobShape):
                # This node shape is the first that fits this jobShape
                chosenNodeShape = nodeShape
                break

        if chosenNodeShape is None:
            logger.warning("Couldn't fit job with requirements %r into any nodes in the nodeTypes list." % jobShape)

        nodeReservations = self.nodeReservations[chosenNodeShape]
        for nodeReservation in nodeReservations:
            if nodeReservation.attemptToAddJob(jobShape, chosenNodeShape, self.targetTime):
                # We succeeded adding the job to this node reservation. Now we're done.
                return

        reservation = NodeReservation(chosenNodeShape)
        t = chosenNodeShape.wallTime
        adjustEndingReservationForJob(reservation, jobShape, 0)
        self.nodeReservations[chosenNodeShape].append(reservation)

        # Extend the reservation if necessary to cover the job's entire runtime.
        while t < jobShape.wallTime:
            y = NodeReservation(reservation.shape)
            t += chosenNodeShape.wallTime
            reservation.nReservation = y
            reservation = y

    def getRequiredNodes(self):
        """
        Returns a dict from node shape to number of nodes required to run the packed jobs.
        """
        return {nodeShape:len(self.nodeReservations[nodeShape]) for nodeShape in self.nodeShapes}

class NodeReservation(object):
    """
    Represents a node reservation. To represent the resources available in a reservation a
    node reservation is represented as a sequence of Shapes, each giving the resources free
    within the given interval of time
    """
    def __init__(self, shape):
        # The wall-time and resource available
        self.shape = shape
        # The next portion of the reservation
        self.nReservation = None

    def fits(self, jobShape):
        """Check if a job shape's resource requirements will fit within this allocation."""
        return jobShape.memory <= self.shape.memory and jobShape.cores <= self.shape.cores and jobShape.disk <= self.shape.disk and (jobShape.preemptable or not self.shape.preemptable)

    def shapes(self):
        """Get all time-slice shapes, in order, from this reservation on."""
        shapes = []
        curRes = self
        while curRes is not None:
            shapes.append(curRes.shape)
            curRes = curRes.nReservation
        return shapes

    def subtract(self, jobShape):
        """
        Adjust available resources of a node allocation as a job is scheduled within it.
        """
        self.shape = Shape(self.shape.wallTime, self.shape.memory - jobShape.memory, self.shape.cores - jobShape.cores, self.shape.disk - jobShape.disk, self.shape.preemptable)

    def attemptToAddJob(self, jobShape, nodeShape, targetTime):
        """Attempt to pack a job into this reservation timeslice and/or the reservations after it.

        jobShape is the Shape of the job requirements, nodeShape is the Shape of the node this
        is a reservation for, and targetTime is the maximum time to wait before starting this job.
        """
        # starting slice of time that we can fit in so far
        startingReservation = self
        # current end of the slices we can fit in so far
        endingReservation = startingReservation
        # the amount of runtime of the job currently covered by slices
        jobTimeSoFar = 0
        # total time from when the instance started up to startingReservation
        startingReservationTime = 0

        while True:
            # Considering a new ending reservation.
            if endingReservation.fits(jobShape):
                jobTimeSoFar += endingReservation.shape.wallTime

                if jobTimeSoFar >= jobShape.wallTime:
                    # The job fits into all the slices between startingReservation and endingReservation.
                    t = 0
                    # Update all the slices, reserving the amount of resources that this job needs.
                    while startingReservation != endingReservation:
                        startingReservation.subtract(jobShape)
                        t += startingReservation.shape.wallTime
                        startingReservation = startingReservation.nReservation
                    assert startingReservation == endingReservation
                    assert jobShape.wallTime - t <= startingReservation.shape.wallTime
                    adjustEndingReservationForJob(endingReservation, jobShape, t)
                    # Packed the job.
                    return True

                # If the job would fit, but is longer than the total node allocation
                # extend the node allocation
                elif endingReservation.nReservation == None and startingReservation == self:
                    # Extend the node reservation to accommodate jobShape
                    endingReservation.nReservation = NodeReservation(nodeShape)
            else:
                if startingReservationTime + jobTimeSoFar <= targetTime:
                    startingReservation = endingReservation.nReservation
                    startingReservationTime += jobTimeSoFar + endingReservation.shape.wallTime
                    jobTimeSoFar = 0
                else:
                    break

            endingReservation = endingReservation.nReservation
            if endingReservation is None:
                # Reached the end of the reservation without success so stop trying to
                # add to reservation
                break
        # Couldn't pack the job.
        return False


def adjustEndingReservationForJob(reservation, jobShape, t):
    if jobShape.wallTime - t < reservation.shape.wallTime:
        # This job only partially fills one of the slices. Create a new slice.
        reservation.shape, nS = split(reservation.shape, jobShape, jobShape.wallTime - t)
        nS.nReservation = reservation.nReservation
        reservation.nReservation = nS
    else:
        # This job perfectly fits within the boundaries of the slices.
        reservation.subtract(jobShape)

def split(nodeShape, jobShape, t):
    """
    Partition a node allocation into two
    """
    return (Shape(t, nodeShape.memory - jobShape.memory, nodeShape.cores - jobShape.cores, nodeShape.disk - jobShape.disk, nodeShape.preemptable),
            NodeReservation(Shape(nodeShape.wallTime - t, nodeShape.memory, nodeShape.cores, nodeShape.disk, nodeShape.preemptable)))

def binPacking(nodeShapes, jobShapes):
    bpf = BinPackedFit(nodeShapes)
    bpf.binPack(jobShapes)
    return bpf.getRequiredNodes()

class ClusterScaler(object):
    def __init__(self, provisioner, leader, config):
        """
        Class manages automatically scaling the number of worker nodes.
        :param AbstractProvisioner provisioner: Provisioner instance to scale.
        :param toil.leader.Leader leader: 
        :param Config config: Config object from which to draw parameters.
        """
        self.provisioner = provisioner
        self.leader = leader
        self.config = config
        # Indicates that the scaling threads should shutdown
        self.stop = False

        #Dictionary of job names to their average runtime, used to estimate wall time
        #of queued jobs for bin-packing
        self.jobNameToAvgRuntime = {}
        self.jobNameToNumCompleted = {}
        self.totalAvgRuntime = 0.0
        self.totalJobsCompleted = 0
        

        require(sum(config.maxNodes) > 0, 'Not configured to create nodes of any type.')
        
        self.scaler = ScalerThread(scaler=self)

    def start(self):
        """ 
        Start the cluster scaler thread(s).
        """
        self.scaler.start()

    def check(self):
        """
        Attempt to join any existing scaler threads that may have died or finished. This insures
        any exceptions raised in the threads are propagated in a timely fashion.
        """
        try:
            self.scaler.join(timeout=0)
        except Exception as e:
            logger.exception(e)
            raise RuntimeError('The cluster scaler has exited due to an exception')

    def shutdown(self):
        """
        Shutdown the cluster.
        """
        self.stop = True
        self.scaler.join()
                
    def getAverageRuntime(self, jobName, service=False):
        if service:
            # We short-circuit service jobs and assume that they will
            # take a very long time, because if they are assumed to
            # take a short time, we may try to pack multiple services
            # into the same core/memory/disk "reservation", one after
            # the other. That could easily lead to underprovisioning
            # and a deadlock, because often multiple services need to
            # be running at once for any actual work to get done.
            return 3600.0 * 24
        if jobName in self.jobNameToAvgRuntime:
            #Have seen jobs of this type before, so estimate
            #the runtime based on average of previous jobs of this type
            return self.jobNameToAvgRuntime[jobName]
        elif self.totalAvgRuntime > 0:
            #Haven't seen this job yet, so estimate its runtime as
            #the average runtime of all completed jobs
            return self.totalAvgRuntime
        else:
            #Have no information whatsoever
            return 1.0

    def addCompletedJob(self, job, wallTime):
        """
        Adds the shape of a completed job to the queue, allowing the scalar to use the last N
        completed jobs in factoring how many nodes are required in the cluster.
        :param toil.job.JobNode job: The memory, core and disk requirements of the completed job
        :param int wallTime: The wall-time taken to complete the job in seconds.
        """

        #Adjust average runtimes to include this job.
        if job.jobName in self.jobNameToAvgRuntime:
            prevAvg = self.jobNameToAvgRuntime[job.jobName]
            prevNum = self.jobNameToNumCompleted[job.jobName]
            self.jobNameToAvgRuntime[job.jobName] = float(prevAvg*prevNum + wallTime)/(prevNum + 1)
            self.jobNameToNumCompleted[job.jobName] += 1
        else:
            self.jobNameToAvgRuntime[job.jobName] = wallTime
            self.jobNameToNumCompleted[job.jobName] = 1

        self.totalJobsCompleted += 1
        self.totalAvgRuntime = float(self.totalAvgRuntime*(self.totalJobsCompleted - 1) + wallTime)/self.totalJobsCompleted

        s = Shape(wallTime=wallTime, memory=job.memory, cores=job.cores, disk=job.disk,
                preemptable=job.preemptable)
        self.scaler.addRecentJobShape(s)


class ScalerThread(ExceptionalThread):
    """
    A thread that automatically scales the number of either preemptable or non-preemptable worker
    nodes according to the number of jobs queued and the resource requirements of the last N
    completed jobs.
    The scaling calculation is essentially as follows: Use the RecentJobShapes instance to
    calculate how many nodes, n, can be used to productively compute the last N completed
    jobs. Let M be the number of jobs issued to the batch system. The number of nodes
    required is then estimated to be alpha * n * M/N, where alpha is a scaling factor used to
    adjust the balance between under- and over- provisioning the cluster.
    At each scaling decision point a comparison between the current, C, and newly estimated
    number of nodes is made. If the absolute difference is less than beta * C then no change
    is made, else the size of the cluster is adapted. The beta factor is an inertia parameter
    that prevents continual fluctuations in the number of nodes.
    """
    def __init__(self, scaler):
        """
        :param ClusterScaler scaler: the parent class
        """
        super(ScalerThread, self).__init__(name='scaler')
        self.scaler = scaler

        self.nodeTypes = self.scaler.provisioner.nodeTypes
        self.nodeShapes = self.scaler.provisioner.nodeShapes

        self.nodeShapeToType = dict(zip(self.nodeShapes, self.nodeTypes))

        self.nodeShapes.sort()
        self.ignoredNodes = set()

        # A *deficit* exists when we have more jobs that can run on preemptable 
        # nodes than we have preemptable nodes. In order to not block these jobs, 
        # we want to increase the number of non-preemptable nodes that we have and 
        # need for just non-preemptable jobs. However, we may still
        # prefer waiting for preemptable instances to come available.
        # To accommodate this, we set the delta to the difference between the number 
        # of provisioned preemptable nodes and the number of nodes that were requested. 
        # Then, when provisioning non-preemptable nodes of the same type, we attempt to 
        # make up the deficit.
        self.preemptableNodeDeficit = {nodeType:0 for nodeType in self.nodeTypes}

        assert len(self.nodeShapes) > 0

        # Monitors the requirements of the N most recently completed jobs
        # Start off with 10 jobs with the shape of the smallest node type
        self.jobShapes = RecentJobShapes(scaler.config, nodeShape=self.nodeShapes[0])
        # Minimum/maximum number of either preemptable or non-preemptable nodes in the cluster
        minNodes = scaler.config.minNodes
        if minNodes is None:
            minNodes = [0 for node in self.nodeTypes]
        maxNodes = scaler.config.maxNodes
        while len(maxNodes) < len(self.nodeTypes):
            maxNodes.append(maxNodes[0])
        self.minNodes = dict(zip(self.nodeShapes, minNodes))
        self.maxNodes = dict(zip(self.nodeShapes, maxNodes))

        #Node shape to number of currently provisioned nodes
        self.totalNodes = defaultdict(int)
        if isinstance(self.scaler.leader.batchSystem, AbstractScalableBatchSystem):
            for preemptable in (True, False):
                nodes = []
                for nodeType in self.nodeTypes:
                    nodes_thisType = self.scaler.leader.provisioner.getProvisionedWorkers(nodeType=nodeType, preemptable=preemptable)
                    nodeShape = self.scaler.provisioner.getNodeShape(nodeType, preemptable=preemptable)
                    self.totalNodes[nodeShape] += len(nodes_thisType)
                    nodes.extend(nodes_thisType)

                self.scaler.provisioner.setStaticNodes(nodes, preemptable)
                    

        self.stats = None
        logger.info('Starting with the following nodes in the cluster: %s' % self.totalNodes )
        
        if scaler.config.clusterStats:
            logger.debug("Starting up cluster statistics...")
            self.stats = ClusterStats(self.scaler.leader.config.clusterStats,
                                      self.scaler.leader.batchSystem,
                                      self.scaler.provisioner.clusterName)
            self.stats.startStats(preemptable=preemptable)
            logger.debug("...Cluster stats started.")

    def addRecentJobShape(self, shape):
        self.jobShapes.add(shape)
        
    def tryRun(self):
        while not self.scaler.stop:
            try:
                with throttle(self.scaler.config.scaleInterval):
                    queuedJobs = self.scaler.leader.getJobs()
                    logger.info("avg runtime dict: %s" % repr(self.scaler.jobNameToAvgRuntime))
                    for job in set(job for job in queuedJobs):
                        logger.info("Got avg runtime %s for job %s." % (self.scaler.getAverageRuntime(job.jobName, service=isinstance(job, ServiceJobNode)), job.jobName))
                    queuedJobShapes = [Shape(wallTime=self.scaler.getAverageRuntime(jobName=job.jobName, service=isinstance(job, ServiceJobNode)), memory=job.memory, cores=job.cores, disk=job.disk, preemptable=job.preemptable) for job in queuedJobs]
                    logger.info("job shapes: %s" % (repr(set(queuedJobShapes))))
                    nodesToRunQueuedJobs = binPacking(jobShapes=queuedJobShapes, nodeShapes=self.nodeShapes)
                    for nodeShape in self.nodeShapes:
                        nodeType = self.nodeShapeToType[nodeShape]
                        self.totalNodes[nodeShape] = len(self.scaler.leader.provisioner.getProvisionedWorkers(nodeType=nodeType, preemptable=nodeShape.preemptable))

                        logger.info("Nodes of type %s to run queued jobs = %s" % (nodeType, nodesToRunQueuedJobs[nodeShape]))
                        # Actual calculation of the estimated number of nodes required
                        estimatedNodes = 0 if nodesToRunQueuedJobs[nodeShape] == 0 else max(1, int(round(nodesToRunQueuedJobs[nodeShape])))
                        logger.info("Estimating %i nodes of shape %s" % (estimatedNodes, nodeShape))


                        # If we're scaling a non-preemptable node type, we need to see if we have a 
                        # deficit of preemptable nodes of this type that we should compensate for.
                        if not nodeShape.preemptable:
                            compensation = self.scaler.config.preemptableCompensation
                            assert 0.0 <= compensation <= 1.0
                            # The number of nodes we provision as compensation for missing preemptable
                            # nodes is the product of the deficit (the number of preemptable nodes we did
                            # _not_ allocate) and configuration preference.
                            compensationNodes = int(round(self.preemptableNodeDeficit[nodeType] * compensation))
                            if compensationNodes > 0:
                                logger.info('Adding %d preemptable nodes of type %s to compensate for a deficit of %d '
                                            'non-preemptable ones.', compensationNodes, nodeType, self.preemptableNodeDeficit[nodeType])
                            estimatedNodes += compensationNodes 

                        # Use inertia parameter to stop small fluctuations
                        logger.info("Currently %i nodes of type %s in cluster" % (self.totalNodes[nodeShape], nodeType))
                        if self.scaler.leader.toilMetrics:
                            self.scaler.leader.toilMetrics.logClusterSize(nodeType=nodeType, currentSize=self.totalNodes[nodeShape],
                                                                          desiredSize=estimatedNodes)

                        # Bound number using the max and min node parameters
                        if estimatedNodes > self.maxNodes[nodeShape]:
                            logger.debug('Limiting the estimated number of necessary %s (%s) to the '
                                         'configured maximum (%s).', nodeType, estimatedNodes, self.maxNodes[nodeShape])
                            estimatedNodes = self.maxNodes[nodeShape]
                        elif estimatedNodes < self.minNodes[nodeShape]:
                            logger.info('Raising the estimated number of necessary %s (%s) to the '
                                        'configured mininimum (%s).', nodeType, estimatedNodes, self.minNodes[nodeShape])
                            estimatedNodes = self.minNodes[nodeShape]

                        if estimatedNodes != self.totalNodes[nodeShape]:
                            logger.info('Changing the number of %s from %s to %s.', nodeType, self.totalNodes[nodeShape],
                                        estimatedNodes)
                            self.totalNodes[nodeShape] = self.setNodeCount(nodeType=nodeType, numNodes=estimatedNodes, preemptable=nodeShape.preemptable)


                        # If we were scaling up a preemptable node type and failed to meet
                        # our target, we will attempt to compensate for the deficit while scaling
                        # non-preemptable nodes of this type.
                        if nodeShape.preemptable:
                            if self.totalNodes[nodeShape] < estimatedNodes:
                                deficit = estimatedNodes - self.totalNodes[nodeType]
                                logger.info('Preemptable scaler detected deficit of %d nodes of type %s.' % (deficit, nodeType))
                                self.preemptableNodeDeficit[nodeType] = deficit
                            else:
                                self.preemptableNodeDeficit[nodeType] = 0 

                    #Attempt to terminate any nodes that we previously designated for 
                    #termination, but which still had workers running.
                    self._terminateIgnoredNodes()

                    if self.stats:
                        self.stats.checkStats()
            except:
                log.exception("Exception encountered in scaler thread. Making a "
                              "best-effort attempt to keep going, but things may "
                              "go wrong from now on.")
        self.shutDown()

    def setNodeCount(self, nodeType, numNodes, preemptable=False, force=False):
        """
        Attempt to grow or shrink the number of prepemptable or non-preemptable worker nodes in
        the cluster to the given value, or as close a value as possible, and, after performing
        the necessary additions or removals of worker nodes, return the resulting number of
        preemptable or non-preemptable nodes currently in the cluster.

        :param str nodeType: The node type to add or remove.

        :param int numNodes: Desired size of the cluster

        :param bool preemptable: whether the added nodes will be preemptable, i.e. whether they
               may be removed spontaneously by the underlying platform at any time.

        :param bool force: If False, the provisioner is allowed to deviate from the given number
               of nodes. For example, when downsizing a cluster, a provisioner might leave nodes
               running if they have active jobs running on them.

        :rtype: int :return: the number of worker nodes in the cluster after making the necessary
                adjustments. This value should be, but is not guaranteed to be, close or equal to
                the `numNodes` argument. It represents the closest possible approximation of the
                actual cluster size at the time this method returns.
        """
        for attempt in retry(predicate=self.scaler.provisioner.retryPredicate):
            with attempt:
                workerInstances = self.getNodes(preemptable=preemptable)
                logger.info("Cluster contains %i instances" % len(workerInstances))
                #Reduce to nodes of the correct type
                workerInstances = {node:workerInstances[node] for node in workerInstances if node.nodeType == nodeType}
                logger.info("Cluster contains %i instances of type %s" % (len(workerInstances), nodeType))
                numCurrentNodes = len(workerInstances)
                delta = numNodes - numCurrentNodes
                if delta > 0:
                    logger.info('Adding %i %s nodes to get to desired cluster size of %i.', delta, 'preemptable' if preemptable else 'non-preemptable', numNodes)
                    numNodes = numCurrentNodes + self._addNodes(nodeType, numNodes=delta,
                                                                preemptable=preemptable)
                elif delta < 0:
                    logger.info('Removing %i %s nodes to get to desired cluster size of %i.', -delta, 'preemptable' if preemptable else 'non-preemptable', numNodes)
                    numNodes = numCurrentNodes - self._removeNodes(workerInstances,
                                                                   nodeType = nodeType,
                                                                   numNodes=-delta,
                                                                   preemptable=preemptable,
                                                                   force=force)
                else:
                    logger.info('Cluster already at desired size of %i. Nothing to do.', numNodes)
        return numNodes

    def _addNodes(self, nodeType, numNodes, preemptable):
        return self.scaler.provisioner.addNodes(nodeType=nodeType, numNodes=numNodes, preemptable=preemptable)

    def _removeNodes(self, nodeToNodeInfo, nodeType, numNodes, preemptable=False, force=False):
        # If the batch system is scalable, we can use the number of currently running workers on
        # each node as the primary criterion to select which nodes to terminate.
        if isinstance(self.scaler.leader.batchSystem, AbstractScalableBatchSystem):
            # Unless forced, exclude nodes with runnning workers. Note that it is possible for
            # the batch system to report stale nodes for which the corresponding instance was
            # terminated already. There can also be instances that the batch system doesn't have
            # nodes for yet. We'll ignore those, too, unless forced.
            nodeToNodeInfo = self.getNodes(preemptable)
            #Filter down to nodes of the correct node type
            nodeToNodeInfo = {node:nodeToNodeInfo[node] for node in nodeToNodeInfo if node.nodeType == nodeType}

            nodesToTerminate = self.chooseNodes(nodeToNodeInfo, force, preemptable=preemptable)

            nodesToTerminate = nodesToTerminate[:numNodes]

            # Join nodes and instances on private IP address.
            logger.debug('Nodes considered to terminate: %s', ' '.join(map(str, nodeToNodeInfo)))

            #Tell the batch system to stop sending jobs to these nodes
            for (node, nodeInfo) in nodesToTerminate:
                self.ignoredNodes.add(node.privateIP)
                self.scaler.leader.batchSystem.ignoreNode(node.privateIP)

            if not force:
                # Filter out nodes with jobs still running. These
                # will be terminated in _removeIgnoredNodes later on
                # once all jobs have finished, but they will be ignored by
                # the batch system and cluster scaler from now on
                nodesToTerminate = [(node,nodeInfo) for (node,nodeInfo) in nodesToTerminate if nodeInfo is not None and nodeInfo.workers < 1]
            nodesToTerminate = {node:nodeInfo for (node, nodeInfo) in nodesToTerminate}
            nodeToNodeInfo = nodesToTerminate
        else:
            # Without load info all we can do is sort instances by time left in billing cycle.
            nodeToNodeInfo = sorted(nodeToNodeInfo, key=self.scaler.provisioner.remainingBillingInterval)
            nodeToNodeInfo = [instance for instance in islice(nodeToNodeInfo, numNodes)]
        logger.info('Terminating %i instance(s).', len(nodeToNodeInfo))
        if nodeToNodeInfo:
            self.scaler.provisioner.terminateNodes(nodeToNodeInfo)
        return len(nodeToNodeInfo)

    def _terminateIgnoredNodes(self):
        #Try to terminate any straggling nodes that we designated for
        #termination, but which still has workers running
        nodeToNodeInfo = self.getNodes(preemptable=None)

        #Remove any nodes that have already been terminated from the list
        # of ignored nodes
        allNodeIPs = [node.privateIP for node in nodeToNodeInfo]
        self.ignoredNodes = set([ip for ip in self.ignoredNodes if ip in allNodeIPs])

        logger.info("There are %i nodes being ignored by the batch system, checking if they can be terminated" % len(self.ignoredNodes))
        nodeToNodeInfo = {node:nodeToNodeInfo[node] for node in nodeToNodeInfo if node.privateIP in self.ignoredNodes}

        nodeToNodeInfo = {node:nodeToNodeInfo[node] for node in nodeToNodeInfo if nodeToNodeInfo[node] is not None and nodeToNodeInfo[node].workers < 1}

        for node in nodeToNodeInfo:
            self.ignoredNodes.remove(node.privateIP)
            self.scaler.leader.batchSystem.unignoreNode(node.privateIP)
        if len(nodeToNodeInfo) > 0:
            logger.info("Terminating %i nodes that were being ignored by the batch system" % len(nodeToNodeInfo))
            self.scaler.provisioner.terminateNodes(nodeToNodeInfo)

    def chooseNodes(self, nodeToNodeInfo, force=False, preemptable=False):
        nodesToTerminate = []
        for node, nodeInfo in list(nodeToNodeInfo.items()):
            if node is None:
                logger.info("Node with info %s was not found in our node list", nodeInfo)
                continue
            staticNodes = self.scaler.provisioner.getStaticNodes(preemptable)
            prefix = 'non-' if not preemptable else ''
            if node.privateIP in staticNodes:
                # we don't want to automatically terminate any statically
                # provisioned nodes
                logger.debug("Found %s in %spreemptable static nodes", node.privateIP, prefix)
                continue
            else:
                logger.debug("Did not find %s in %spreemptable static nodes", node.privateIP, prefix)
                pass
            nodesToTerminate.append((node, nodeInfo))
        # Sort nodes by number of workers and time left in billing cycle
        nodesToTerminate.sort(key=lambda node_nodeInfo: (
            node_nodeInfo[1].workers if node_nodeInfo[1] else 1,
            self.scaler.provisioner.remainingBillingInterval(node_nodeInfo[0]))
                              )
        return nodesToTerminate

    def getNodes(self, preemptable):
        """
        Returns a dictionary mapping node identifiers of preemptable or non-preemptable nodes to
        NodeInfo objects, one for each node.


        This method is the definitive source on nodes in cluster, & is responsible for consolidating
        cluster state between the provisioner & batch system.

        :param bool preemptable: If True (False) only (non-)preemptable nodes will be returned.
               If None, all nodes will be returned.

        :rtype: dict[Node, NodeInfo]
        """
        def _getInfo(allMesosNodes, ip):
            info = None
            try:
                info = allMesosNodes[ip]
            except KeyError:
                # never seen by mesos - 1 of 3 possibilities:
                # 1) node is still launching mesos & will come online soon
                # 2) no jobs have been assigned to this worker. This means the executor was never
                #    launched, so we don't even get an executorInfo back indicating 0 workers running
                # 3) mesos crashed before launching, worker will never come online
                # In all 3 situations it's safe to fake executor info with 0 workers, since in all
                # cases there are no workers running.
                info = NodeInfo(coresTotal=1, coresUsed=0, requestedCores=0,
                                memoryTotal=1, memoryUsed=0, requestedMemory=0,
                                workers=0)
            else:
                # Node was tracked but we haven't seen this in the last 10 minutes
                inUse = self.scaler.leader.batchSystem.nodeInUse(ip)
                if not inUse:
                    # The node hasn't reported in the last 10 minutes & last we know
                    # there weren't any tasks running. We will fake executorInfo with no
                    # worker to reflect this, since otherwise this node will never
                    # be considered for termination
                    info.workers = 0
                else:
                    pass
                    # despite the node not reporting to mesos jobs may still be running
                    # so we can't terminate the node
            return info

        allMesosNodes = self.scaler.leader.batchSystem.getNodes(preemptable, timeout=None)
        recentMesosNodes = self.scaler.leader.batchSystem.getNodes(preemptable)
        provisionerNodes = self.scaler.provisioner.getProvisionedWorkers(nodeType=None, preemptable=preemptable)

        if len(recentMesosNodes) != len(provisionerNodes):
            logger.debug("Consolidating state between mesos and provisioner")
        nodeToInfo = {}
        # fixme: what happens if awsFilterImpairedNodes is used?
        # if this assertion is false it means that user-managed nodes are being
        # used that are outside the provisioner's control
        # this would violate many basic assumptions in autoscaling so it currently not allowed
        for node, ip in ((node, node.privateIP) for node in provisionerNodes):
            info = None
            if ip not in recentMesosNodes:
                logger.debug("Worker node at %s is not reporting executor information", ip)
                # we don't have up to date information about the node
                info = _getInfo(allMesosNodes, ip)
            else:
                # mesos knows about the ip & we have up to date information - easy!
                info = recentMesosNodes[ip]
            # add info to dict to return
            nodeToInfo[node] = info
        return nodeToInfo

    def shutDown(self):
        if self.stats:
            self.stats.shutDownStats()
        logger.debug('Forcing provisioner to reduce cluster size to zero.')
        for nodeShape in self.nodeShapes:
            preemptable = nodeShape.preemptable
            nodeType = self.nodeShapeToType[nodeShape]
            self.setNodeCount(nodeType=nodeType, numNodes=0, preemptable=preemptable, force=True)

class ClusterStats(object):

    def __init__(self, path, batchSystem, clusterName):
        logger.debug("Initializing cluster statistics")
        self.stats = {}
        self.statsThreads = []
        self.statsPath = path
        self.stop = False
        self.clusterName = clusterName
        self.batchSystem = batchSystem
        self.scaleable = isinstance(self.batchSystem, AbstractScalableBatchSystem) if batchSystem else False

    def shutDownStats(self):
        if self.stop:
            return
        def getFileName():
            extension = '.json'
            file = '%s-stats' % self.clusterName
            counter = 0
            while True:
                suffix = str(counter).zfill(3) + extension
                fullName = os.path.join(self.statsPath, file + suffix)
                if not os.path.exists(fullName):
                    return fullName
                counter += 1
        if self.statsPath and self.scaleable:
            self.stop = True
            for thread in self.statsThreads:
                thread.join()
            fileName = getFileName()
            with open(fileName, 'w') as f:
                json.dump(self.stats, f)

    def startStats(self, preemptable):
        thread = ExceptionalThread(target=self._gatherStats, args=[preemptable])
        thread.start()
        self.statsThreads.append(thread)

    def checkStats(self):
        for thread in self.statsThreads:
            # propagate any errors raised in the threads execution
            thread.join(timeout=0)

    def _gatherStats(self, preemptable):
        def toDict(nodeInfo):
            # convert NodeInfo object to dict to improve JSON output
            return dict(memory=nodeInfo.memoryUsed,
                        cores=nodeInfo.coresUsed,
                        memoryTotal=nodeInfo.memoryTotal,
                        coresTotal=nodeInfo.coresTotal,
                        requestedCores=nodeInfo.requestedCores,
                        requestedMemory=nodeInfo.requestedMemory,
                        workers=nodeInfo.workers,
                        time=time.time()  # add time stamp
                        )
        if self.scaleable:
            logger.debug("Staring to gather statistics")
            stats = {}
            try:
                while not self.stop:
                    nodeInfo = self.batchSystem.getNodes(preemptable)
                    for nodeIP in list(nodeInfo.keys()):
                        nodeStats = nodeInfo[nodeIP]
                        if nodeStats is not None:
                            nodeStats = toDict(nodeStats)
                            try:
                                # if the node is already registered update the dictionary with
                                # the newly reported stats
                                stats[nodeIP].append(nodeStats)
                            except KeyError:
                                # create a new entry for the node
                                stats[nodeIP] = [nodeStats]
                    time.sleep(60)
            finally:
                threadName = 'Preemptable' if preemptable else 'Non-preemptable'
                logger.debug('%s provisioner stats thread shut down successfully.', threadName)
                self.stats[threadName] = stats
        else:
            pass
