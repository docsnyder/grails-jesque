package grails.plugins.jesque

import grails.core.GrailsApplication
import grails.core.support.GrailsApplicationAware
import grails.persistence.support.PersistenceContextInterceptor
import groovy.transform.CompileDynamic
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import net.greghaines.jesque.Config
import net.greghaines.jesque.Job
import net.greghaines.jesque.admin.Admin
import net.greghaines.jesque.admin.AdminClient
import net.greghaines.jesque.admin.AdminPoolImpl
import net.greghaines.jesque.client.Client
import net.greghaines.jesque.meta.WorkerInfo
import net.greghaines.jesque.meta.dao.WorkerInfoDAO
import net.greghaines.jesque.worker.*
import org.joda.time.DateTime
import org.springframework.context.ApplicationListener
import org.springframework.context.event.ContextClosedEvent
import org.springframework.core.Ordered
import redis.clients.jedis.Jedis
import redis.clients.util.Pool

@Slf4j
@CompileStatic
class JesqueService implements ApplicationListener<ContextClosedEvent>, Ordered {

    static final int DEFAULT_WORKER_POOL_SIZE = 3

    GrailsApplication grailsApplication
    Config jesqueConfig
    PersistenceContextInterceptor persistenceInterceptor
    Client jesqueClient
    WorkerInfoDAO workerInfoDao
    List<Worker> workers = Collections.synchronizedList([] as List<Worker>)
    AdminClient jesqueAdminClient
    Pool<Jedis> redisPool

    void enqueue(String queueName, Job job) {
        jesqueClient.enqueue(queueName, job)
    }

    void enqueue(String queueName, String jobName, List args) {
        enqueue(queueName, new Job(jobName, args))
    }

    void enqueue(String queueName, Class jobClazz, List args) {
        enqueue(queueName, jobClazz.simpleName, args)
    }

    void enqueue(String queueName, String jobName, Object... args) {
        enqueue(queueName, new Job(jobName, args))
    }

    void enqueue(String queueName, Class jobClazz, Object... args) {
        enqueue(queueName, jobClazz.simpleName, args)
    }

    void priorityEnqueue(String queueName, Job job) {
        jesqueClient.priorityEnqueue(queueName, job)
    }

    void priorityEnqueue(String queueName, String jobName, List args) {
        priorityEnqueue(queueName, new Job(jobName, args))
    }

    void priorityEnqueue(String queueName, Class jobClazz, List args) {
        priorityEnqueue(queueName, jobClazz.simpleName, args)
    }

    void priorityEnqueue(String queueName, String jobName, Object... args) {
        priorityEnqueue(queueName, new Job(jobName, args))
    }

    void priorityEnqueue(String queueName, Class jobClazz, Object... args) {
        priorityEnqueue(queueName, jobClazz.simpleName, args)
    }

    void enqueueAt(DateTime dateTime, String queueName, Job job) {
        if (dateTime.millis < System.currentTimeMillis()) {
            // prevent hitting the (expensive) IllegalArgumentException
            jesqueClient.enqueue(queueName, job)
        } else {
            try {
                jesqueClient.delayedEnqueue("${queueName}Delayed", job, dateTime.millis)
            } catch (IllegalArgumentException ignore) {
                // fallback to the regular queue in case jesque complained ("future must be after current time")
                jesqueClient.enqueue(queueName, job)
            }
        }
    }

    void enqueueAt(DateTime dateTime, String queueName, String jobName, Object... args) {
        enqueueAt(dateTime, queueName, new Job(jobName, args))
    }

    void enqueueAt(DateTime dateTime, String queueName, Class jobClazz, Object... args) {
        enqueueAt(dateTime, queueName, jobClazz.simpleName, args)
    }

    void enqueueAt(DateTime dateTime, String queueName, String jobName, List args) {
        enqueueAt(dateTime, queueName, new Job(jobName, args))
    }

    void enqueueAt(DateTime dateTime, String queueName, Class jobClazz, List args) {
        enqueueAt(dateTime, queueName, jobClazz.simpleName, args)
    }


    void enqueueIn(Integer millisecondDelay, String queueName, Job job) {
        enqueueAt(new DateTime().plusMillis(millisecondDelay), queueName, job)
    }

    void enqueueIn(Integer millisecondDelay, String queueName, String jobName, Object... args) {
        enqueueIn(millisecondDelay, queueName, new Job(jobName, args))
    }

    void enqueueIn(Integer millisecondDelay, String queueName, Class jobClazz, Object... args) {
        enqueueIn(millisecondDelay, queueName, jobClazz.simpleName, args)
    }

    void enqueueIn(Integer millisecondDelay, String queueName, String jobName, List args) {
        enqueueIn(millisecondDelay, queueName, new Job(jobName, args))
    }

    void enqueueIn(Integer millisecondDelay, String queueName, Class jobClazz, List args) {
        enqueueIn(millisecondDelay, queueName, jobClazz.simpleName, args)
    }


    void removeDelayed(String queueName, Class jobClass, List args) {
        removeDelayed(queueName, jobClass.simpleName, args)
    }

    void removeDelayed(String queueName, String jobName, List args) {
        removeDelayed(queueName, new Job(jobName, args))
    }

    void removeDelayed(String queueName, Class jobClass, Object... args) {
        removeDelayed(queueName, jobClass.simpleName, args)
    }

    void removeDelayed(String queueName, String jobName, Object... args) {
        removeDelayed(queueName, new Job(jobName, args))
    }

    void removeDelayed(String queueName, Job job) {
        jesqueClient.removeDelayedEnqueue(queueName, job)
    }


    Worker startWorker(String queueName, String jobName, Class jobClass, ExceptionHandler exceptionHandler = null,
                       boolean paused = false) {
        startWorker([queueName], [(jobName): jobClass], exceptionHandler, paused)
    }

    Worker startWorker(List queueName, String jobName, Class jobClass, ExceptionHandler exceptionHandler = null,
                       boolean paused = false) {
        startWorker(queueName, [(jobName): jobClass], exceptionHandler, paused)
    }

    Worker startWorker(String queueName, Map<String, ? extends Class> jobTypes, ExceptionHandler exceptionHandler = null,
                       boolean paused = false) {
        startWorker([queueName], jobTypes, exceptionHandler, paused)
    }

    @CompileDynamic
    Worker startWorker(List<String> queues, Map<String, Class> jobTypes, ExceptionHandler exceptionHandler = null,
                       boolean paused = false) {
        // automatically support delayed queues
        def allQueues = queues.collectMany { queue ->
            [queue, getDelayedQueueName(queue)]
        }

        log.info "Starting worker processing queueus: ${allQueues}"

        Class workerClass = GrailsWorkerImpl
        def customWorkerClass = grailsApplication.config.grails.jesque.custom.worker.clazz
        if (customWorkerClass) {
            if (customWorkerClass instanceof String) {
                try {
                    customWorkerClass = Class.forName(customWorkerClass)
                } catch (ClassNotFoundException ignore) {
                    log.error("Custom Worker class not found for name $customWorkerClass")
                    customWorkerClass = null
                }
            }
            if (customWorkerClass && customWorkerClass in GrailsWorkerImpl) {
                workerClass = customWorkerClass as Class
            } else if (customWorkerClass) {
                // the "null" case should only happen at this point, when we could not find the class, so we can safely assume there was a error message already
                log.warn("The specified custom worker class ${customWorkerClass} does not extend GrailsWorkerImpl. Ignoring it")
            }
        }

        def nextQueueStrategy = NextQueueStrategy.DRAIN_WHILE_MESSAGES_EXISTS
        def customNextQueueStrategy = grailsApplication.config.grails.jesque.nextQueueStrategy
        if (customNextQueueStrategy && customNextQueueStrategy instanceof String) {
            try {
                nextQueueStrategy = NextQueueStrategy.valueOf(customNextQueueStrategy)
            } catch (Exception ignore) {
                log.warn("Unknown nextQueueStrategy specified: $customNextQueueStrategy")
            }
        }

        Worker worker = (GrailsWorkerImpl) workerClass.newInstance(grailsApplication, jesqueConfig, redisPool, allQueues, jobTypes, nextQueueStrategy)

        def customListenerClass = grailsApplication.config.grails.jesque.custom.listener.clazz
        if (customListenerClass) {
            if (customListenerClass instanceof List) {
                customListenerClass.each {
                    addCustomListenerClass(worker, it)
                }
            } else {
                addCustomListenerClass(worker, customListenerClass)
            }
        }

        def failedItemLimit = grailsApplication.config.grails.jesque.failedItemLimit
        if (failedItemLimit && failedItemLimit.toString().isNumber()) {
            worker.setFailQueueStrategy(new LimitFailedItemsFailQueueStrategy(jesqueConfig.namespace, failedItemLimit as int))
        }

        if (exceptionHandler)
            worker.exceptionHandler = exceptionHandler

        if (paused) {
            worker.togglePause(paused)
        }

        workers.add(worker)

        // create an Admin for this worker (makes it possible to administer across a cluster)
        Admin admin = new AdminPoolImpl(jesqueConfig, redisPool)
        admin.setWorker(worker)

        Boolean autoFlush = true
        def autoFlushFromConfig = grailsApplication.config.grails.jesque.autoFlush
        if (autoFlushFromConfig != null) {
            if (autoFlushFromConfig instanceof String) {
                autoFlush = Boolean.parseBoolean(autoFlushFromConfig)
            } else if (autoFlushFromConfig instanceof Boolean) {
                autoFlush = autoFlushFromConfig
            }
        }
        def workerPersistenceListener = new WorkerPersistenceListener(persistenceInterceptor, autoFlush)
        worker.workerEventEmitter.addListener(workerPersistenceListener, WorkerEvent.JOB_EXECUTE, WorkerEvent.JOB_SUCCESS, WorkerEvent.JOB_FAILURE)

        def workerLifeCycleListener = new WorkerLifecycleListener(this)
        worker.workerEventEmitter.addListener(workerLifeCycleListener, WorkerEvent.WORKER_STOP)

        def workerThread = new Thread(worker)
        workerThread.start()

        def adminThread = new Thread(admin)
        adminThread.start()

        worker
    }

    @Override
    int getOrder() {
        // ensure that workers are stopped as early as possible during app shutdown
        return HIGHEST_PRECEDENCE
    }

    @Override
    void onApplicationEvent(ContextClosedEvent event) {
        this.stopAllWorkers()
    }

    void stopAllWorkers() {
        log.info "stopping ${workers.size()} jesque workers"

        List<Worker> workersToRemove = workers.collect { it }
        workersToRemove*.end(false)

        workersToRemove.each { Worker worker ->
            try {
                log.info "stopping worker $worker"
                worker.end(true)
                worker.join(10000)
            } catch (Exception exception) {
                log.error "stopping jesque worker failed", exception
            }
        }
    }

    void withWorker(String queueName, String jobName, Class jobClassName, Closure closure) {
        def worker = startWorker(queueName, jobName, jobClassName)
        try {
            closure()
        } finally {
            worker.end(true)
        }
    }

    @CompileDynamic
    void startWorkersFromConfig(ConfigObject jesqueConfigMap) {
        boolean startPaused = jesqueConfigMap.startPaused as boolean ?: false

        jesqueConfigMap?.workers?.keySet()?.each { String workerPoolName ->
            def value = jesqueConfigMap?.workers[workerPoolName]
            def workers = value.workers ? value.workers.toInteger() : DEFAULT_WORKER_POOL_SIZE
            def queueNames = value.queueNames
            List<String> jobTypes = value.jobTypes

            log.info "Starting $workers workers for pool $workerPoolName"

            if (!((queueNames instanceof String) || (queueNames instanceof List<String>)))
                throw new Exception("Invalid queueNames for pool $workerPoolName, expecting must be a String or a List<String>.")

            if (!(jobTypes instanceof List))
                throw new Exception("Invalid jobTypes (${jobTypes}) for pool $workerPoolName, must be a list")

            Map<String, Class> jobNameClass = [:]
            jobTypes?.each { String k ->
                def clazz = grailsApplication.getClassForName(k)
                if (clazz) {
                    jobNameClass.put(clazz.simpleName, clazz)
                } else {
                    log.info "Could not get grails class $k"
                }
            }

            workers.times {
                startWorker(queueNames, jobNameClass, null, startPaused)
            }
        }
    }

    void pruneWorkers() {
        def hostName = InetAddress.localHost.hostName
        workerInfoDao.allWorkers?.each { WorkerInfo workerInfo ->
            if (workerInfo.host == hostName) {
                log.debug "Removing stale worker $workerInfo.name"
                workerInfoDao.removeWorker(workerInfo.name)
            }
        }
    }

    void removeWorkerFromLifecycleTracking(Worker worker) {
        log.debug "Removing worker ${worker.name} from lifecycle tracking"
        workers.remove(worker)
    }

    void pauseAllWorkersOnThisNode() {
        log.info "Pausing all ${workers.size()} jesque workers on this node"

        List<Worker> workersToPause = workers.collect { it }
        workersToPause.each { Worker worker ->
            log.debug "Pausing worker processing queues: ${worker.queues}"
            worker.togglePause(true)
        }
    }

    void resumeAllWorkersOnThisNode() {
        log.info "Resuming all ${workers.size()} jesque workers on this node"

        List<Worker> workersToPause = workers.collect { it }
        workersToPause.each { Worker worker ->
            log.debug "Resuming worker processing queues: ${worker.queues}"
            worker.togglePause(false)
        }
    }

    void pauseAllWorkersInCluster() {
        log.debug "Pausing all workers in the cluster"
        jesqueAdminClient.togglePausedWorkers(true)
    }

    void resumeAllWorkersInCluster() {
        log.debug "Resuming all workers in the cluster"
        jesqueAdminClient.togglePausedWorkers(false)
    }

    void shutdownAllWorkersInCluster() {
        log.debug "Shutting down all workers in the cluster"
        jesqueAdminClient.shutdownWorkers(true)
    }

    boolean areAllWorkersInClusterPaused() {
        return workerInfoDao.getActiveWorkerCount() == 0
    }

    @CompileDynamic
    private addCustomListenerClass(Worker worker, customListenerClass) {
        if (customListenerClass instanceof String) {
            try {
                customListenerClass = Class.forName(customListenerClass)
            } catch (ClassNotFoundException ignore) {
                log.error("Custom Job Listener class not found for name $customListenerClass")
                customListenerClass = null
            }
        }
        if (customListenerClass && customListenerClass in WorkerListener) {
            def customListener = customListenerClass.newInstance() as WorkerListener
            if (customListener instanceof GrailsApplicationAware) {
                customListener.setGrailsApplication(grailsApplication)
            }
            worker.workerEventEmitter.addListener(customListener)
        } else if (customListenerClass) {
            // the "null" case should only happen at this point, when we could not find the class, so we can safely assume there was a error message already
            log.warn("The specified custom listener class ${customListenerClass} does not implement WorkerListener. Ignoring it")
        }
    }

    private static String getDelayedQueueName(String queueName) {
        "${queueName}Delayed"
    }

}
