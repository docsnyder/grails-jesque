package grails.plugins.jesque

import net.greghaines.jesque.worker.DefaultFailQueueStrategy

class LimitFailedItemsFailQueueStrategy extends DefaultFailQueueStrategy {

    int failedQueueMaxItems

    LimitFailedItemsFailQueueStrategy(final String namespace, final int failedItemLimit) {
        super(namespace)
        this.failedQueueMaxItems = failedItemLimit
    }

    @Override
    int getFailQueueMaxItems(String curQueue) {
        return failedQueueMaxItems
    }

}
