package codes.recursive.service.streaming

import codes.recursive.event.EventEmitter
import codes.recursive.model.BarnEvent
import codes.recursive.model.BarnSseEvent
import codes.recursive.service.data.OracleDataService
import codes.recursive.util.ArduinoMessage
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider
import com.oracle.bmc.streaming.StreamClient
import com.oracle.bmc.streaming.model.CreateGroupCursorDetails
import com.oracle.bmc.streaming.model.Message
import com.oracle.bmc.streaming.requests.CreateGroupCursorRequest
import com.oracle.bmc.streaming.requests.GetMessagesRequest
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
    EventEmitter eventEmitter
    OracleDataService oracleDataService

    @Inject
    MessageConsumerService(
            EventEmitter eventEmitter,
            OracleDataService oracleDataService,
            @Property(name="codes.recursive.oracle.oci-config-path") String configFilePath,
            @Property(name="codes.recursive.oracle.streaming.outgoing-stream-id") String streamId
    ) {
        this.eventEmitter = eventEmitter
        this.oracleDataService = oracleDataService
        this.configFilePath = configFilePath
        this.streamId = streamId
        def provider =  new ConfigFileAuthenticationDetailsProvider(this.configFilePath, 'DEFAULT')
        def client = new StreamClient(provider)
        client.setRegion('us-phoenix-1')
        this.client = client
    }


    void start() {
        logger.info("Creating cursor...")

        def cursorDetails = CreateGroupCursorDetails.builder()
                .type(CreateGroupCursorDetails.Type.TrimHorizon)
                .commitOnGet(true)
                .groupName(this.groupName)
                .build()
        def groupCursorRequest = CreateGroupCursorRequest.builder()
                .streamId(streamId)
                .createGroupCursorDetails(cursorDetails)
                .build()

        def cursorResponse = this.client.createGroupCursor(groupCursorRequest)
        String cursor = cursorResponse.cursor.value

        logger.info("Cursor created...")

        while(!closed.get()) {
            GetMessagesRequest getRequest = GetMessagesRequest.builder()
                    .cursor(cursor)
                    .streamId(this.streamId)
                    .build()
            def getResult = this.client.getMessages(getRequest)
            getResult.items.each { Message record ->
                Map msg
                try {
                    def slurper = new JsonSlurper()
                    msg = slurper.parseText( new String(record.value, "UTF-8") ) as Map
                    logger.info "Received: ${JsonOutput.toJson(msg)}"
                    BarnEvent evt = new BarnEvent( msg?.type as String, JsonOutput.toJson(msg?.data), record.timestamp )
                    BarnSseEvent sseEvent = new BarnSseEvent( msg?.type as String, msg?.data as Map, record.timestamp )
                    if( evt.type != ArduinoMessage.CAMERA_0 ) {
                        eventEmitter.emit('incomingMessage', [barnEvent: evt, barnSseEvent: sseEvent, timestamp: record.timestamp])
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
            sleep(1000)
        }

    }

    def close() {
        logger.info("Closing consumer...")
        closed.set(true)
    }
}
