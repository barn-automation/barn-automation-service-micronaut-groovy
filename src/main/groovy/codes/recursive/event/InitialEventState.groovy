package codes.recursive.event

import codes.recursive.model.BarnSseEvent

import java.util.concurrent.Callable

class InitialEventState implements Callable<BarnSseEvent> {
    @Override
    BarnSseEvent call() throws Exception {
        return new BarnSseEvent()
    }
}