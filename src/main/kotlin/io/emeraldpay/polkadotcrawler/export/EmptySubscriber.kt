package io.emeraldpay.polkadotcrawler.export

import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription

/**
 * Empty subscriber, just drops all received items
 */
class EmptySubscriber<T>: Subscriber<T> {

    override fun onComplete() {
    }

    override fun onSubscribe(s: Subscription?) {
    }

    override fun onNext(t: T) {
    }

    override fun onError(t: Throwable?) {
    }


}