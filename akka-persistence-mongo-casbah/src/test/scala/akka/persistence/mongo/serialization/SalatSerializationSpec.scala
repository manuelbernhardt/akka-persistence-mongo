package akka.persistence.mongo.serialization

import java.util.Date

import akka.persistence.mongo.MongoCleanup
import com.mongodb.casbah.commons.conversions.MongoConversionHelper
import org.bson.BSON
import org.joda.time.{DateTime, LocalTime}

import scala.concurrent.duration._
import akka.actor._
import akka.persistence._
import akka.testkit._
import com.typesafe.config.ConfigFactory

import SalatSerializationSpec._

import org.scalatest._

object SalatSerializationSpec {
  def config(port: Int) = ConfigFactory.parseString(
    s"""
      |akka.actor.serialize-messages = on
      |casbah-journal.bson-serialization = "salat"
      |casbah-journal.bson-conversion-helpers = "akka.persistence.mongo.serialization.CustomHelpers"
      |akka.persistence.journal.plugin = "casbah-journal"
      |akka.persistence.snapshot-store.plugin = "casbah-snapshot-store"
      |akka.persistence.journal.max-deletion-batch-size = 3
      |akka.persistence.publish-plugin-commands = on
      |akka.persistence.publish-confirmations = on
      |casbah-journal.mongo-journal-url = "mongodb://localhost:$port/store.messages"
      |casbah-journal.mongo-journal-write-concern = "acknowledged"
      |casbah-journal.mongo-journal-write-concern-timeout = 10000
      |casbah-snapshot-store.mongo-snapshot-url = "mongodb://localhost:$port/store.snapshots"
      |casbah-snapshot-store.mongo-snapshot-write-concern = "acknowledged"
      |casbah-snapshot-store.mongo-snapshot-write-concern-timeout = 10000
     """.stripMargin)

  class PersistentActorTest(val persistenceId: String, testEvent: SomeEvent, replyTo: ActorRef) extends PersistentActor with ActorLogging {
    def receiveRecover: Receive = handle
    def receiveCommand: Receive = {
      case "TestMe" => persist(testEvent){ replyTo ! "Persisted "+ _.toString }
    }

    def handle: Receive = {
      case payload: SomeEvent =>
        replyTo ! payload

       case _ =>
    }
  }
}

class SalatSerializationSpec extends TestKit(ActorSystem("test", config(27017)))
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with MongoCleanup {

  override val actorSystem: ActorSystem = system

  val event = SomeEvent("Arthur Dent", 42)
  val customEvent = SomeEventWithCustomType("Ford Prefect", new LocalTime(12, 42))

  "SalatBSONSerialzation" must {
    "round trip a class" in {
      val serializer = serialization.findSerializerFor(event)
      val bytes = serializer.toBinary(event)
      val rehydrated = serializer.fromBinary(bytes, event.getClass)
      info(s"In: $event. Out: $rehydrated")
      assert(event == rehydrated)
    }
    "round trip a class with custom de/serialization helpers" in {
      val serializer = serialization.findSerializerFor(customEvent)
      val bytes = serializer.toBinary(customEvent)
      val rehydrated = serializer.fromBinary(bytes, customEvent.getClass)
      info(s"In: $customEvent. Out: $rehydrated")
      assert(customEvent == rehydrated)
    }
    "persist event as BSON and recover using it" in {
      val origActorRef = system.actorOf(Props(classOf[PersistentActorTest], "Tester1", event, testActor))
      origActorRef ! "TestMe"
      expectMsg(5.second, s"Persisted $event")
      val recoveredActorRef = system.actorOf(Props(classOf[PersistentActorTest], "Tester1", null, testActor))
      expectMsg(5.seconds, event)
    }
  }

}

case class SomeEvent(a: String, b: Int)
case class SomeEventWithCustomType(a: String, b: LocalTime)

class CustomHelpers extends MongoConversionHelper {

    val LocalTime = classOf[LocalTime]
    val Date = classOf[Date]

    import org.bson.Transformer

    val serializer = new Transformer {
      override def transform(o: AnyRef) = {
        o match {
          case l: LocalTime => l.toDateTimeToday.toDate
          case _ => o
        }
      }
    }
    val deserializer = new Transformer {
      override def transform(o: AnyRef) = {
        o match {
          case date: java.util.Date => new DateTime(date).toLocalTime
          case _ => o
        }
      }
    }

  override def register(): Unit = {
    BSON.addEncodingHook(LocalTime, serializer)
    BSON.addDecodingHook(Date, deserializer)
    super.register()
  }

  override def unregister(): Unit = {
    BSON.removeEncodingHook(LocalTime, serializer)
    BSON.removeDecodingHook(Date, deserializer)
    super.unregister()
  }
}