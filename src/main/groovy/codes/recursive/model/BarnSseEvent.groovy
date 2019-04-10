package codes.recursive.model

import groovy.transform.CompileStatic

@CompileStatic
class BarnSseEvent {
    def id
    String type
    Map data
    Date capturedAt

    BarnSseEvent() {

    }

    BarnSseEvent(String type, Map data, Date capturedAt) {
        this.type = type
        this.data = data
        this.capturedAt = capturedAt
    }

    BarnSseEvent(def id, String type, Map data, Date capturedAt) {
        this.id = id
        this.type = type
        this.data = data
        this.capturedAt = capturedAt
    }
}

