package akka.persistence.mongo.journal

import akka.actor.{ActorSystem, ExtendedActorSystem}
import akka.event.Logging
import akka.persistence.Persistence
import akka.persistence.mongo.MongoPersistenceJournalRoot
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.commons.conversions.MongoConversionHelper
import com.mongodb.casbah.commons.conversions.scala.{DeregisterJodaTimeConversionHelpers, RegisterJodaTimeConversionHelpers, RegisterConversionHelpers}
import com.novus.salat.{Grater, Context}

import scala.util.control.NonFatal

trait BSONSerialization {

  def fromBSON(doc: MongoDBObject, typeHint: String): Any

  def toBSON(obj: Any): MongoDBObject

}

class SalatBSONSerialization(system: ExtendedActorSystem) extends BSONSerialization {

  val log = Logging(system, getClass.getName)

  val conversionHelpers = {
    val ConfigPath = "casbah-journal.bson-conversion-helpers"

    val helpersClass = if (system.settings.config.hasPath(ConfigPath)) {
      val helper = system.settings.config.getString(ConfigPath)
      log.info(s"Using custom BSON conversion helper: $helper")
      helper
    } else {
      "akka.persistence.mongo.journal.DefaultConversionHelpers"
    }

    system.dynamicAccess.classLoader
      .loadClass(helpersClass)
      .newInstance()
      .asInstanceOf[MongoConversionHelper]
  }

  // call these to be ready for all kind of custom types our Events may carry
  conversionHelpers.register()

  implicit val salatContext = new Context {
    override val name = "serializer"
    registerClassLoader(system.dynamicAccess.classLoader)
  }

  override def fromBSON(doc: MongoDBObject, typeHint: String): Any = {
    try {
      val grater = salatContext.lookup(typeHint)
      grater.asObject(doc)
    } catch { case NonFatal(t) =>
      log.error(t, "Could not deserialize object from BSON")
      // grater glitches are verbose, a lot of information is hidden if we don't spill it out
      t.printStackTrace()
      throw t
    }
  }

  override def toBSON(obj: Any): MongoDBObject = {
    try {
      val grater: Grater[AnyRef] = salatContext.lookup(obj.getClass.getName).asInstanceOf[Grater[AnyRef]]
      grater.asDBObject(obj.asInstanceOf[AnyRef])
    } catch {
      case NonFatal(t) =>
        log.error(t, "Could not serialize object as BSON")
        // grater glitches are verbose, a lot of information is hidden if we don't spill it out
        t.printStackTrace()
        throw t
    }
  }

}

class DefaultConversionHelpers extends MongoConversionHelper {

  override def register(): Unit = {
    RegisterConversionHelpers()
    RegisterJodaTimeConversionHelpers()
    super.register()
  }

  override def unregister(): Unit = {
    DeregisterJodaTimeConversionHelpers()
    super.unregister()
  }

}

trait BSONSerializationSupport { self: MongoPersistenceJournalRoot =>

  val actorSystem: ActorSystem

  lazy val bsonSerialization: Option[BSONSerialization] = {
    val ConfigPath = "casbah-journal.bson-serialization"
    if (actorSystem.settings.config.hasPath(ConfigPath)) {
      val extension = Persistence(actorSystem)
      val serializer = actorSystem.settings.config.getString(ConfigPath)
      serializer match {
        case "salat" =>
          Some(new SalatBSONSerialization(extension.system))
        case other =>
          None
      }
    } else {
      None
    }
  }

}