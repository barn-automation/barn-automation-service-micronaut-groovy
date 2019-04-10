package codes.recursive.event

import javax.inject.Singleton
import java.util.concurrent.Executors

@Singleton
class EventEmitter {

    def listeners = [:]
    def pool = Executors.newFixedThreadPool(10)

    def addListener(String event, Closure listener) {
        if( hasListener(event) ) {
            listeners[event] += listener
        } else {
            listeners[event] = [listener]
        }
    }

    def hasListener(String event) {
        return listeners.containsKey(event)
    }

    def removeListener(String event, Closure listener) {
        if(hasListener(event)) {
            def removed = listeners[event].remove(listener)
            if(!listeners[event]) {
                listeners.remove(event)
            }
            return removed
        }
        return false
    }

    def removeAllListeners(String event) {
        listeners.remove(event)
    }

    def emit(event, data) {
        if(hasListener(event)) {
            def listenerList = listeners[event]
            listenerList.each { listener ->
                pool.submit(listener.call(data) as Runnable)
            }
            return true
        }
        return false
    }

}