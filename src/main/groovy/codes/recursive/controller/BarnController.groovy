package codes.recursive.controller

import codes.recursive.event.EventPublisher
import codes.recursive.event.InitialEventState
import codes.recursive.model.BarnSseEvent
import codes.recursive.service.data.OracleDataService
import codes.recursive.service.streaming.MessageProducerService
import codes.recursive.util.ArduinoMessage
import groovy.json.JsonOutput
import groovy.transform.CompileStatic
import groovy.transform.TypeCheckingMode
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.*
import io.micronaut.http.sse.Event
import io.reactivex.Emitter
import io.reactivex.Flowable
import io.reactivex.disposables.Disposable
import io.reactivex.functions.BiConsumer
import io.reactivex.functions.Consumer
import org.reactivestreams.Publisher
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.inject.Inject
import java.util.concurrent.Callable
import java.util.concurrent.atomic.AtomicBoolean

@CompileStatic(TypeCheckingMode.SKIP)
@Controller("/barn")
class BarnController {
    static final Logger logger = LoggerFactory.getLogger(BarnController.class)

    OracleDataService oracleDataService
    MessageProducerService messageProducerService
    EventPublisher eventPublisher

    @Inject
    BarnController(
            OracleDataService oracleDataService,
            MessageProducerService messageProducerService,
            EventPublisher eventPublisher
    ){
        this.oracleDataService = oracleDataService
        this.messageProducerService = messageProducerService
        this.eventPublisher = eventPublisher
    }

    @Get("/")
    @Produces(MediaType.APPLICATION_JSON)
    Map index() {
        return [
                health: 'OK',
                streamSource: 'OCI',
                at: new Date(),
        ]
    }

    @Get("/events/type/{type}/{offset}/{max}")
    @Produces(MediaType.APPLICATION_JSON)
    List listEventsByTypePaginated(String type, int offset, int max) {
        return oracleDataService.listEventsByEventType(type, offset, max)
    }

    @Get("/events/type/{type}")
    @Produces(MediaType.APPLICATION_JSON)
    List listEventsByType(String type) {
        return oracleDataService.listEventsByEventType(type)
    }


    @Get("/events/count")
    @Produces(MediaType.APPLICATION_JSON)
    Map countEvents() {
        return [
                total: oracleDataService.countEvents(),
        ]
    }

    @Get("/events/count/{type}")
    @Produces(MediaType.APPLICATION_JSON)
    Map countEventsByType(String type) {
        return [
                total: oracleDataService.countEventsByEventType(type),
        ]
    }

    @Get("/events")
    @Produces(MediaType.APPLICATION_JSON)
    List getEvents() {
        return oracleDataService.listEvents()
    }

    @Get("/events/{offset}/{max}")
    @Produces(MediaType.APPLICATION_JSON)
    List listEventsPaginated(int offset, int max) {
        return oracleDataService.listEvents(offset, max)
    }

    @Post("/control")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    Map sendControlMessage(@Body ArduinoMessage message) {
        logger.info  "Received message: ${message}"
        messageProducerService.send(JsonOutput.toJson(message))
        return [
                sent: true
        ]
    }

    @Get("/stream")
    Publisher<Event<BarnSseEvent>> stream() {
        final AtomicBoolean isSubscribed = new AtomicBoolean(false)
        Disposable subscription

        logger.info("oh, hello new client! let us provide you with some data!")

        return Flowable.generate( new InitialEventState() as Callable, ( (sseEvent, sseEmitter) -> {
            logger.info("the consumer has been called.")
            if( !isSubscribed.get() ) {
                logger.info("subscribe to the eventPublisher")
                subscription = eventPublisher.publishSubject
                    .doOnError( err -> {
                        logger.info("something bad happened. clean up and get out of here!")
                        sseEmitter.onComplete()
                        subscription.dispose()
                    })
                    .subscribe ( o -> {
                        logger.info("eventPublisher has received something, pass it along to the client if they are still connected -->")
                        if ( !sseEmitter.cancelled ) {
                            sseEmitter.onNext ( Event .of ( o ) )
                        }
                        else {
                            logger.info("oh no! the client has disconnected, let's clean things up...")
                            sseEmitter.onComplete ( )
                            subscription.dispose()

                        }
                    });
                isSubscribed.set(true)
            }
        }) as BiConsumer );
    }
}
