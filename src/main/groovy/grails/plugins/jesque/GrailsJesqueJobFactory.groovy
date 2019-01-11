package grails.plugins.jesque

import grails.core.GrailsApplication
import grails.util.Holders
import groovy.transform.CompileDynamic
import groovy.transform.CompileStatic
import net.greghaines.jesque.Job
import net.greghaines.jesque.utils.ReflectionUtils
import net.greghaines.jesque.worker.MapBasedJobFactory
import net.greghaines.jesque.worker.UnpermittedJobException

/**
 * Job Factory that knows how to materialize grails jobs.
 */
@CompileStatic
class GrailsJesqueJobFactory extends MapBasedJobFactory {

    static GrailsApplication grailsApplication = Holders.grailsApplication

    GrailsJesqueJobFactory(Map<String, ? extends Class<?>> jobTypes) {
        super(jobTypes)
    }

    @Override
    @CompileDynamic
    protected void checkJobType(final String jobName, final Class<?> jobType) {
        if (jobName == null) {
            throw new IllegalArgumentException("jobName must not be null")
        }
        if (jobType == null) {
            throw new IllegalArgumentException("jobType must not be null")
        }

        if (!(jobType in grailsApplication.jesqueJobClasses*.clazz)) {
            throw new IllegalArgumentException("jobType is not a valid jesque job class: " + jobType)
        }
    }

    @Override
    Object materializeJob(final Job job) throws Exception {
        Class jobClass = jobTypes[job.className]
        if (!jobClass) {
            throw new UnpermittedJobException(job.className)
        }

        def instance = grailsApplication.mainContext.getBean(jobClass.canonicalName)
        if (job.vars && !job.vars.isEmpty()) {
            ReflectionUtils.invokeSetters(instance, job.vars)
        }
        return instance
    }

}
