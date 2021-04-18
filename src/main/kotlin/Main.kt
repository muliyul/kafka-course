import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.Constants
import com.twitter.hbc.core.HttpHosts
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.event.Event
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.BasicClient
import com.twitter.hbc.httpclient.auth.Authentication
import com.twitter.hbc.httpclient.auth.OAuth1
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit


fun main(args: Array<String>) {
  val msgQueue = LinkedBlockingQueue<String>(100_000)
  val eventQueue = LinkedBlockingQueue<Event>(1000)

  val client = createTwitterClient(msgQueue, eventQueue)

  client.connect()

  while (!client.isDone) {
    val tweet: String? = msgQueue.poll(5, TimeUnit.SECONDS)
    tweet?.let { println(it) }
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

  val hosebirdAuth: Authentication = OAuth1(
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
