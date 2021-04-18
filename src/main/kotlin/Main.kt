import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.Constants
import com.twitter.hbc.core.HttpHosts
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.event.Event
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.BasicClient
import com.twitter.hbc.httpclient.auth.OAuth1
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

private val logger = LoggerFactory.getLogger("TwitterProducer")

fun main(args: Array<String>) {
  val msgQueue = LinkedBlockingQueue<String>(100_000)
  val eventQueue = LinkedBlockingQueue<Event>(1000)

  val client = createTwitterClient(msgQueue, eventQueue)

  val props = propertiesOf(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "127.0.0.1:9092",
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
//    ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true // required to compensate for network errors
  )

  val kProducer = KafkaProducer<String, String>(props)

// this caused an error in my environment. not sure if needed.
//  Runtime.getRuntime().addShutdownHook(thread {
//    client.stop()
//    kProducer.close()
//  })

  client.connect()
  while (!client.isDone) {
    val tweet: String? = msgQueue.poll(5, TimeUnit.SECONDS)
    tweet?.let { msg ->
      val producerRecord: ProducerRecord<String, String> =
        ProducerRecord("twitter_tweets", null, msg)
      kProducer.send(producerRecord) { metadata, exception ->
        exception?.let { ex ->
          logger.error("Error sending to tweet to Kafka!", ex)
          return@send
        }
        logger.info(
          """Produced tweet to Kafka:
          |timestamp: ${metadata.timestamp()}
          |topic: ${metadata.topic()}
          |partition: ${metadata.partition()}
          |offset: ${metadata.offset()}
        """.trimMargin()
        )
      }
    }
  }
}

private fun createTwitterClient(
  msgQueue: LinkedBlockingQueue<String>,
  eventQueue: LinkedBlockingQueue<Event>
): BasicClient {
  val hosebirdHosts = HttpHosts(Constants.STREAM_HOST)
  val hosebirdEndpoint = StatusesFilterEndpoint().apply {
    trackTerms(listOf("kotlin", "nextinsurance"))
  }

  val hosebirdAuth = OAuth1(
    System.getenv("TWITTER_CONSUMER_KEY"),
    System.getenv("TWITTER_CONSUMER_SECRET"),
    System.getenv("TWITTER_TOKEN"),
    System.getenv("TWITTER_TOKEN_SECRET")
  )

  return ClientBuilder()
    .name("Hosebird-Client")
    .hosts(hosebirdHosts)
    .authentication(hosebirdAuth)
    .endpoint(hosebirdEndpoint)
    .processor(StringDelimitedProcessor(msgQueue))
    .eventMessageQueue(eventQueue)
    .build()
}
