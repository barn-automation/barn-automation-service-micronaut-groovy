package codes.recursive.service.streaming

import codes.recursive.event.EventPublisher
import codes.recursive.model.BarnEvent
import codes.recursive.model.BarnSseEvent
import codes.recursive.service.data.OracleDataService
import codes.recursive.util.ArduinoMessage
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider
import com.oracle.bmc.model.BmcException
import com.oracle.bmc.streaming.StreamClient
import com.oracle.bmc.streaming.model.CreateGroupCursorDetails
import com.oracle.bmc.streaming.model.Message
import com.oracle.bmc.streaming.requests.CreateGroupCursorRequest
import com.oracle.bmc.streaming.requests.GetMessagesRequest
import com.oracle.bmc.streaming.responses.CreateGroupCursorResponse
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
class MessageConsumerService {
    static final Logger logger = LoggerFactory.getLogger(MessageConsumerService.class)
    private final AtomicBoolean closed = new AtomicBoolean(false)

    String configFilePath
    String streamId
    String groupName = 'group-0'
    StreamClient client
    OracleDataService oracleDataService
    EventPublisher eventPublisher

    @Inject
    MessageConsumerService(
            OracleDataService oracleDataService,
            EventPublisher eventPublisher,
            @Property(name="codes.recursive.oracle.oci-config-path") String configFilePath,
            @Property(name="codes.recursive.oracle.streaming.outgoing-stream-id") String streamId
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
        logger.info("Creating cursor...")

        CreateGroupCursorDetails cursorDetails = CreateGroupCursorDetails.builder()
                .type(CreateGroupCursorDetails.Type.TrimHorizon)
                .commitOnGet(true)
                .groupName(this.groupName)
                .build()
        CreateGroupCursorRequest groupCursorRequest = CreateGroupCursorRequest.builder()
                .streamId(streamId)
                .createGroupCursorDetails(cursorDetails)
                .build()

        CreateGroupCursorResponse cursorResponse = this.client.createGroupCursor(groupCursorRequest)
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
                        JsonSlurper slurper = new JsonSlurper()
                        msg = slurper.parseText( new String(record.value, "UTF-8") ) as Map
                        logger.info "Received: ${JsonOutput.toJson(msg)}"
                        BarnEvent evt = new BarnEvent( msg?.type as String, JsonOutput.toJson(msg?.data), 'micronaut-groovy', record.timestamp )
                        BarnSseEvent sseEvent = new BarnSseEvent( msg?.type as String, msg?.data as Map, record.timestamp )
                        if( evt.type != ArduinoMessage.CAMERA_0 ) {
                            eventPublisher.publishSubject.onNext(sseEvent)
                        }
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
                    throw new Exception("MessageConsumerService has failed ${failures} times.  Giving up...")
                }
                else {
                    logger.info "MessageConsumerService call to getMessages() failed with: '${ex.message}'.  Waiting 1000ms to try again..."
                    sleep(1000)
                }
            }
            sleep(500)
        }

    }

    def close() {
        logger.info("Closing consumer...")
        closed.set(true)
    }
}
