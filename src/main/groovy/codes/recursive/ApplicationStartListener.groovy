package codes.recursive


import codes.recursive.service.streaming.CameraConsumerService
import codes.recursive.service.streaming.MessageConsumerService
import groovy.transform.CompileStatic
import io.micronaut.context.event.ApplicationEventListener
import io.micronaut.discovery.event.ServiceStartedEvent
import io.micronaut.scheduling.annotation.Async
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.inject.Inject
import javax.inject.Singleton

@Singleton
@CompileStatic
class ApplicationStartListener implements ApplicationEventListener<ServiceStartedEvent> {

    CameraConsumerService cameraConsumerService
    MessageConsumerService messageConsumerService

    static final Logger logger = LoggerFactory.getLogger(ApplicationStartListener.class)

    @Inject
    ApplicationStartListener(
            CameraConsumerService cameraConsumerService,
            MessageConsumerService messageConsumerService
    ){
        this.cameraConsumerService = cameraConsumerService
        this.messageConsumerService = messageConsumerService
    }

    @Async
    @Override
    void onApplicationEvent(final ServiceStartedEvent serviceStartedEvent) {
        logger.info "😐 Starting up..."
        Thread.start {
            cameraConsumerService.start()
        }
        Thread.start {
            messageConsumerService.start()
        }
        def art = """
                             +&-
                           _.-^-._    .--.
                        .-'   _   '-. |__|
                       /     |_|     \\|  |
                      /               \\  |
                     /|     _____     |\\ |
                      |    |==|==|    |  |
  |---|---|---|---|---|    |--|--|    |  |
  |---|---|---|---|---|    |==|==|    |  |
 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
"""
        logger.info art
        logger.info "😀 Started up!"
    }
}
