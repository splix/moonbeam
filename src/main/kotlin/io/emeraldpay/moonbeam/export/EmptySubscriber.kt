package io.emeraldpay.moonbeam.export

import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription

/**
 * Empty subscriber, just drops all received items
 */
class EmptySubscriber<T>: Subscriber<T> {

    override fun onComplete() {
    }

    override fun onSubscribe(s: Subscription) {
        s.request(Long.MAX_VALUE)
    }

    override fun onNext(t: T) {
    }

    override fun onError(t: Throwable) {
    }


}