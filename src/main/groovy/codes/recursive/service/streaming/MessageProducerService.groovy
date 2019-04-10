package codes.recursive.service.streaming

import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider
import com.oracle.bmc.streaming.StreamClient
import com.oracle.bmc.streaming.model.PutMessagesDetails
import com.oracle.bmc.streaming.model.PutMessagesDetailsEntry
import com.oracle.bmc.streaming.requests.PutMessagesRequest
import groovy.transform.CompileStatic
import io.micronaut.context.annotation.Property
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.inject.Singleton
import java.nio.charset.Charset

@Singleton
@CompileStatic
class MessageProducerService {
    static final Logger logger = LoggerFactory.getLogger(MessageProducerService.class)

    String configFilePath
    String streamId
    StreamClient client

    MessageProducerService(
            @Property(name="codes.recursive.oracle.oci-config-path") String configFilePath,
            @Property(name="codes.recursive.oracle.streaming.incoming-stream-id") String streamId
    ) {
        this.configFilePath = configFilePath
        this.streamId = streamId
        def provider =  new ConfigFileAuthenticationDetailsProvider(this.configFilePath, 'DEFAULT')
        def client = new StreamClient(provider)
        client.setRegion('us-phoenix-1')
        this.client = client
    }

    def send(String msg, String key= UUID.randomUUID().toString()) {
        try {
            def putMessageDetails = PutMessagesDetails.builder()
                .messages([
                    PutMessagesDetailsEntry.builder()
                        .key(key.getBytes(Charset.forName("UTF-8")))
                        .value(msg.getBytes(Charset.forName("UTF-8")))
                        .build()
                ])
                .build()
            def putMessageRequest = PutMessagesRequest.builder()
                .streamId(this.streamId)
                .putMessagesDetails(putMessageDetails)
                .build()
            client.putMessages(putMessageRequest)
        }
        catch(e) {
            logger.error("An error occurred whilst sending message...")
            e.printStackTrace()
        }
        finally {
            //logger.info("Send complete for message with key ${key}")
        }
    }

    def close() {
    }
}
