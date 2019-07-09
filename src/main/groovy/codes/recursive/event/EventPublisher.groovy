package codes.recursive.event

import codes.recursive.model.BarnSseEvent
import groovy.transform.CompileStatic
import io.reactivex.subjects.PublishSubject

import javax.inject.Singleton

@CompileStatic
@Singleton
class EventPublisher {
    public PublishSubject<BarnSseEvent> publishSubject = PublishSubject.create()
}
