package codes.recursive.service.streaming

import codes.recursive.event.EventPublisher
import codes.recursive.model.BarnEvent
import codes.recursive.model.BarnSseEvent
import codes.recursive.service.data.OracleDataService
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider
import com.oracle.bmc.model.BmcException
import com.oracle.bmc.streaming.StreamClient
import com.oracle.bmc.streaming.model.CreateCursorDetails
import com.oracle.bmc.streaming.model.Message
import com.oracle.bmc.streaming.requests.CreateCursorRequest
import com.oracle.bmc.streaming.requests.GetMessagesRequest
import com.oracle.bmc.streaming.responses.CreateCursorResponse
import com.oracle.bmc.streaming.responses.GetMessagesResponse
import groovy.json.JsonException
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.transform.CompileStatic
import io.micronaut.context.annotation.Property
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.inject.Inject
import javax.inject.Singleton
import java.util.concurrent.atomic.AtomicBoolean

@Singleton
@CompileStatic
class CameraConsumerService {
    static final Logger logger = LoggerFactory.getLogger(CameraConsumerService.class)

    String configFilePath
    String streamId
    StreamClient client
    private final AtomicBoolean closed = new AtomicBoolean(false)

    OracleDataService oracleDataService
    EventPublisher eventPublisher

    CameraConsumerService(
            OracleDataService oracleDataService,
            EventPublisher eventPublisher,
            @Property(name="codes.recursive.oracle.oci-config-path") String configFilePath,
            @Property(name="codes.recursive.oracle.streaming.camera-stream-id") String streamId
    ) {
        this.oracleDataService = oracleDataService
        this.eventPublisher = eventPublisher
        this.configFilePath = configFilePath
        this.streamId = streamId
        ConfigFileAuthenticationDetailsProvider provider =  new ConfigFileAuthenticationDetailsProvider(this.configFilePath, 'DEFAULT')
        StreamClient client = new StreamClient(provider)
        client.setRegion('us-phoenix-1')
        this.client = client
    }

    void start() {
        logger.info("Creating camera cursor...")

        CreateCursorDetails cursorDetails = CreateCursorDetails.builder()
                .type(CreateCursorDetails.Type.Latest)
                .partition("0")
                .build()
        CreateCursorRequest cursorRequest = CreateCursorRequest.builder()
                .streamId(streamId)
                .createCursorDetails(cursorDetails)
                .build()

        CreateCursorResponse cursorResponse = this.client.createCursor(cursorRequest)
        String cursor = cursorResponse.cursor.value

        logger.info("Cursor created...")

        int failures = 0

        while(!closed.get()) {
            GetMessagesRequest getRequest = GetMessagesRequest.builder()
                    .cursor(cursor)
                    .streamId(this.streamId)
                    .build()
            GetMessagesResponse getResult
            try {
                getResult = this.client.getMessages(getRequest)
                getResult.items.each { Message record ->
                    Map msg
                    try {
                        def slurper = new JsonSlurper()
                        msg = slurper.parseText(new String(record.value, "UTF-8")) as Map
                        logger.info "Received: ${JsonOutput.toJson(msg)}"
                        BarnEvent evt = new BarnEvent( msg?.type as String, JsonOutput.toJson(msg?.data), 'micronaut-groovy', record.timestamp )
                        BarnSseEvent sseEvent = new BarnSseEvent(msg?.type as String, msg?.data as Map, record.timestamp)
                        eventPublisher.publishSubject.onNext(sseEvent)
                        oracleDataService.save(evt)
                    }
                    catch (JsonException e) {
                        logger.warn("Error parsing JSON from ${record.value}")
                        e.printStackTrace()
                    }
                    catch (Exception e) {
                        logger.warn("Error:")
                        e.printStackTrace()
                    }
                }
                cursor = getResult.opcNextCursor
            }
            catch(BmcException ex) {
                failures++
                if( failures > 9 ) {
                    throw new Exception("CameraConsumerService has failed ${failures} times.  Giving up...")
                }
                else {
                    logger.info "CameraConsumerService call to getMessages() failed with: '${ex.message}'.  Waiting 1000ms to try again..."
                    sleep(1000)
                }
            }
            sleep(500)
        }

    }

    def close() {
        logger.info("Closing camera consumer...")
        closed.set(true)
    }
}
