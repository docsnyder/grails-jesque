package grails.plugins.jesque

import grails.persistence.support.PersistenceContextInterceptor
import groovy.transform.CompileStatic
import net.greghaines.jesque.Job
import net.greghaines.jesque.worker.Worker
import net.greghaines.jesque.worker.WorkerEvent
import net.greghaines.jesque.worker.WorkerListener

@CompileStatic
class WorkerPersistenceListener implements WorkerListener {

    PersistenceContextInterceptor persistenceInterceptor
    boolean initiated = false
    boolean autoFlush

    WorkerPersistenceListener(PersistenceContextInterceptor persistenceInterceptor, boolean autoFlush) {
        this.persistenceInterceptor = persistenceInterceptor
        this.autoFlush = autoFlush
    }

    private boolean bindSession() {
        if (persistenceInterceptor == null)
            throw new IllegalStateException("No persistenceInterceptor found")

        if (!initiated) {
            persistenceInterceptor.init()
        }
        true
    }

    private void unbindSession() {
        if (!initiated) return
        persistenceInterceptor.destroy()
        initiated = false
    }

    private void flushSession() {
        if (!initiated) return
        persistenceInterceptor.flush()
    }

    @Override
    void onEvent(WorkerEvent workerEvent, Worker worker, String queue, Job job, Object runner, Object result, Throwable t) {
        if (workerEvent == WorkerEvent.JOB_EXECUTE) {
            initiated = bindSession()
        } else if (workerEvent == WorkerEvent.JOB_SUCCESS) {
            if (autoFlush) {
                flushSession()
            }
            unbindSession()
        } else if (workerEvent == WorkerEvent.JOB_FAILURE) {
            unbindSession()
        }
    }

}
