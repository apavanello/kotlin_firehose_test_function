import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest
import com.amazonaws.services.kinesisfirehose.model.Record
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.cloud.function.adapter.aws.SpringBootRequestHandler
import org.springframework.context.annotation.Bean
import org.springframework.messaging.Message
import org.springframework.messaging.support.MessageBuilder
import java.nio.ByteBuffer

@SpringBootApplication
class FirehoseLambdaApplication {
    @Bean
    fun firehoseHandler(): RequestHandler<Message<String>, String> {
        return SpringBootRequestHandler(FirehoseFunction::class.java)
    }
}

class FirehoseFunction : RequestHandler<Message<String>, String> {
    override fun handleRequest(message: Message<String>, context: Context): String {
        val payload = message.payload

        // Initialize AWS Firehose client
        val firehoseClient = AmazonKinesisFirehoseClientBuilder.defaultClient()

        // Prepare the record to send to Firehose
        val record = Record().withData(ByteBuffer.wrap(payload.toByteArray()))

        // Create a PutRecordRequest and send the record to Firehose
        val putRecordRequest = PutRecordRequest()
            .withDeliveryStreamName("your-delivery-stream-name")
            .withRecord(record)

        val putRecordResult = firehoseClient.putRecord(putRecordRequest)

        // Return the result
        return putRecordResult.recordId
    }
}

fun main(args: Array<String>) {
    SpringApplication.run(FirehoseLambdaApplication::class.java, *args)
}
