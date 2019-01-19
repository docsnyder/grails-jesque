package ${packageName}

class ${className}Job {
    static triggers = {
        // execute job every 5 seconds
        cron queueName: 'JobQueue', name: 'JobTrigger-1', cronExpression: '0/5 * * * * ? *', args: []
    }

    def perform() {
        // execute job
    }
}
