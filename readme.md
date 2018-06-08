Grails Jesque
=============

[![Build Status](https://travis-ci.org/Grails-Plugin-Consortium/grails-jesque.svg?branch=master)](https://travis-ci.org/Grails-Plugin-Consortium/grails-jesque)

Jesque is an implementation of [Resque](https://github.com/resque/resque) in [Java](http://www.oracle.com/technetwork/java/index.html).
It is fully-interoperable with the [Ruby](http://www.ruby-lang.org/en/) and [Node.js](http://nodejs.org/) ([Coffee-Resque](https://github.com/technoweenie/coffee-resque)) implementations.

The grails jesque plugin uses [jesque](https://github.com/gresrun/jesque) and the grails redis plugin as a dependency.
While it uses jesque for the core functionality it makes it groovier to use in grails.

There is also a grails [jesque-web](https://github.com/michaelcameron/grails-jesque-web) plugin initially ported from the [jesque-web](https://github.com/gresrun/jesque-web) spring-mvc app, which itself was based on the Sinatra web app resque-web in [resque](https://github.com/resque/resque).
Either UI will allow you to see what's in your queues and view and re-process failed messages.

A scheduler (a la Quartz) has been added to support scheduled injection of jobs. The syntax is very similar to the grails Quartz plugin. 


Demo Project
-------------
There is a demo project located [in github](https://github.com/Grails-Plugin-Consortium/grails-jesque-demo).


How do I use it?
----------------
Add the jesque plugin to grails, it will automatically pull in jesque with it's dependencies, and the grails redis plugin.

```bash
dependencies {
    compile('org.grails.plugins:jesque:1.2.1')
}
```

You must also have [redis](http://redis.io) installed in your environment.


Example to enqueue

```groovy

class BackgroundJob {
    def someOtherService //auto-wiring supported

    def perform( arg1, arg2 ) {
        def domainObject = DomainClass.get(arg1) //GORM supported
        domainObject.message = arg2
        domainObject.save()
    }
}

class SomeOtherClass {
    def jesqueService

    def doWorkAsync() {
        jesqueService.enqueue( 'myQueueName', BackgroundJob.simpleName, 1, 'hi there')
    }

    def doWorkAsyncLater() {
        jesqueService.enqueueAt(System.currentTimeMillis() + (1000 * 60), 'myQueueName', BackgroundJob.simpleName, 1, 'hi there')
    }
}
```

Workers can be started manually by calling

```groovy
    jesqueService.startWorker( 'DemoJesqueJobQueue', DemoJesqueJob.simpleName, DemoJesqueJob )
```

or automatically upon start-up with the following config

```yaml
---
grails:
    redis:
        port: 6379
        host: localhost
    jesque:
        enabled: true
        failedItemLimit: 1000
        pruneOrphanedScheduledJobsOnStartup: true
        pruneWorkersOnStartup: true
        createWorkersOnStartup: true
        schedulerThreadActive: true
        startPaused: false
        autoFlush: true
        nextQueueStrategy: RESET_TO_HIGHEST_PRIORITY // or DRAIN_WHILE_MESSAGES_EXISTS
        workers:
            DemoJesqueJobPool:
                queueNames:
                    - "DemoQueue1"
                    - "DemoQueue2"
                jobTypes:
                    - org.grails.jesque.demo.DemoJesqueJob
                    - org.grails.jesque.demo.DemoTwoJesqueJob
```

The redis pool used is configured in the [redis](https://github.com/grails-plugins/grails-redis) plugin:

```yaml
grails:
    redis:
        host: localhost
        port: 6379
```

Or using sentinels

```yaml
grails:
    redis:
        sentinels:
            - 10.0.0.1:26379
            - 10.0.0.2:26379
        masterName: foobar        
```

Jobs
----
Jobs should be placed in grails-app/jobs similar to the [Quartz](http://grails.org/plugin/quartz) plugin.
However, to not clash with quartz, and to retain similarties with resque, the method to execute must be called perform.

You can run the script create-jesque-job to create a shell of a job for you automatically.  The
following will create a BackgroundJob in the grails-app/jobs folder.

```bash
grails create-jesque-job org.grails.jesque.demo.DemoJesqueJob
```

```groovy
package org.grails.jesque.demo

import groovy.util.logging.Slf4j

@Slf4j
class DemoJesqueJob {

    static queue = 'DemoJesqueJobQueue'
    static workerPool = 'DemoJesqueJobPool'

    static triggers = {
        cron name: 'DemoJesqueJobTrigger', cronExpression: '0/15 * * * * ? *'
    }

    def perform() {
        log.info "Executing Job"
    }
}
```

Custom Worker Listener
----
You can define one or more custom WorkerListener classes that will be automatically added to all workers started from within `jesqueService`.
You can implement the `GrailsApplicationAware` interface if you need access to the `grailsApplication` in your worker listener.

```groovy
grails {
    jesque {
        custom {
            listener.clazz = [LoggingWorkerListener] // accepts String, Class or List<String> or List<Class>
        }
    }
}
```
 
*All Listeners have to implement the WorkerListener Interface otherwise they will simply be ignored*

Roadmap
----
* Upgrade custom Listener and Worker to grails 3 support
* Ability to execute methods on services without creating a job object
* Wrap above ability automatically with annotation and dynamically creating a method with the same name + "Async" suffix
* Create grails/groovy docs (gdoc?) to extensively document options
* Dynamic wake time of delayed jobs thread to reduce polling

Release Notes
----

Note: only changes made by uberall are listed here.

* 1.3.0-UBERALL
    * proper pooling by using jesque classes
    * allow listeners to implement GrailsApplicationAware
    * expose `nextQueueStrategy` setting
    * expose `failedItemLimit` setting
    * use delayed job feature provided by jesque
    * refurbished worker listeners (don't flush if job failed)
    * fix autoFlush bug (allows autoFlush to be set to false)
    * properly close persistenceInterceptor when flush failed
    * synchronous init to prevent race-conditions during app start
    * stop workers as early as possible during application shutdown sequence
    * add ability to prune orphaned scheduled jobs on startup (via `pruneOrphanedScheduledJobsOnStartup`)
    * add ability to remove delayed jobs
    * only stop scheduler thread if it has been started
    * log args of executing job

License
-------
Copyright 2011 Michael Cameron

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   <http://www.apache.org/licenses/LICENSE-2.0>

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
