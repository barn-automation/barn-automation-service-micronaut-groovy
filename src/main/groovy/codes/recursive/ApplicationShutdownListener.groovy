package codes.recursive

import codes.recursive.service.data.OracleDataService
import codes.recursive.service.streaming.CameraConsumerService
import codes.recursive.service.streaming.MessageConsumerService
import groovy.transform.CompileStatic
import io.micronaut.context.event.ApplicationEventListener
import io.micronaut.discovery.event.ServiceShutdownEvent
import io.micronaut.scheduling.annotation.Async
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.inject.Inject
import javax.inject.Singleton

@Singleton
@CompileStatic
class ApplicationShutdownListener implements ApplicationEventListener<ServiceShutdownEvent> {

    CameraConsumerService cameraConsumerService
    MessageConsumerService messageConsumerService
    OracleDataService oracleDataService

    static final Logger logger = LoggerFactory.getLogger(ApplicationShutdownListener.class)

    @Inject
    ApplicationShutdownListener(
            CameraConsumerService cameraConsumerService,
            MessageConsumerService messageConsumerService,
            OracleDataService oracleDataService
    ){
        this.cameraConsumerService = cameraConsumerService
        this.messageConsumerService = messageConsumerService
        this.oracleDataService = oracleDataService
    }

    @Async
    @Override
    void onApplicationEvent(final ServiceShutdownEvent serviceShutdownEvent) {
        logger.info "😲 Shutting down..."
        try {
            oracleDataService.close()
            messageConsumerService.close()
            cameraConsumerService.close()
        }
        catch (e) {
            logger.warn "${e.message}"
        }
        logger.info "🙁 Goodbye!"
    }
}
