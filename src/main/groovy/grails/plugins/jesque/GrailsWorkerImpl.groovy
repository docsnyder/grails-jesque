package grails.plugins.jesque

import groovy.util.logging.Slf4j
import net.greghaines.jesque.Config
import net.greghaines.jesque.Job
import net.greghaines.jesque.worker.JobFactory
import net.greghaines.jesque.worker.NextQueueStrategy
import net.greghaines.jesque.worker.WorkerAware
import net.greghaines.jesque.worker.WorkerPoolImpl
import redis.clients.jedis.Jedis
import redis.clients.util.Pool

import static net.greghaines.jesque.worker.WorkerEvent.JOB_EXECUTE

@Slf4j
class GrailsWorkerImpl extends WorkerPoolImpl {

    GrailsWorkerImpl(final Config config,
                     final Collection<String> queues,
                     final JobFactory factory,
                     final Pool<Jedis> jedisPool,
                     final NextQueueStrategy nextQueueStrategy) {
        super(config, queues, factory, jedisPool, nextQueueStrategy)
    }

    @Override
    protected Object execute(final Job job, final String curQueue, final Object instance) throws Exception {
        log.debug "Executing job ${job.className} with args $job.args from queue $curQueue"
        if (instance instanceof WorkerAware) {
            ((WorkerAware) instance).setWorker(this)
        }
        this.listenerDelegate.fireEvent(JOB_EXECUTE, this, curQueue, job, instance, null, null)
        instance.perform(*job.args)
    }

}
