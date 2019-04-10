package codes.recursive.event

import codes.recursive.model.BarnSseEvent
import groovy.transform.CompileStatic
import io.reactivex.Observable
import io.reactivex.subjects.PublishSubject
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.inject.Singleton

@CompileStatic
@Singleton
class BarnEventBus {
    static final Logger logger = LoggerFactory.getLogger(BarnEventBus.class)

    private PublishSubject<Object> eventBus = PublishSubject.create()

    void send(BarnSseEvent barnSseEvent) {
        eventBus.onNext(barnSseEvent)
    }

    Observable<Object> toObservable() {
        return eventBus
    }

}
