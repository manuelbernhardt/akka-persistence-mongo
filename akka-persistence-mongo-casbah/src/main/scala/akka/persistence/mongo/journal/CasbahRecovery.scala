/**
 *  Copyright (C) 2013-2014 Duncan DeVore. <https://github.com/ironfish/>
 */
package akka.persistence.mongo.journal

import akka.persistence._
import akka.persistence.journal.AsyncRecovery

import com.mongodb.casbah.Imports._

import scala.collection.immutable
import scala.concurrent._

trait CasbahRecovery extends AsyncRecovery with BSONSerializationSupport { this: CasbahJournal ⇒

  implicit lazy val replayDispatcher = context.system.dispatchers.lookup(configReplayDispatcher)

  def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback:
      PersistentRepr ⇒ Unit): Future[Unit] = Future {
    replay(persistenceId, fromSequenceNr, toSequenceNr, max)(replayCallback)
  }

  private def replay(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback:
      PersistentRepr => Unit): Unit = {

    import com.mongodb.casbah.Implicits._

    val coll = collection.find(replayFindStatement(persistenceId, fromSequenceNr, toSequenceNr)).sort(recoverySortStatement)
    val collSorted = immutable.SortedMap(coll.toList.zipWithIndex.groupBy(x => x._1.as[Long](SequenceNrKey)).toSeq: _*)
    collSorted.flatMap { x =>
      if (x._2.headOption.get._2 < max) {
        val entry: Option[PersistentRepr] = x._2.find(_._1.get(MarkerKey) == MarkerAccepted).map(_._1).map {e =>
          if (e.containsField(TypeHintKey)) {
            val typeHint = e.as[String](TypeHintKey)
            bsonSerialization.map { bs =>
              val bson = e.as[MongoDBObject](MessageKey)
              val payload = bs.fromBSON(bson, typeHint)
              PersistentRepr(payload, e.as[Long](SequenceNrKey))
            } getOrElse {
              log.error(s"Persistent message with type hint $typeHint found but no BSON deserialization configured")
              throw new IllegalStateException("Encounterd persistent message encoded as plain BSON object, but no BSON Serialization configured")
            }
          } else {
            val bytes = e.as[Array[Byte]](MessageKey)
            fromBytes[PersistentRepr](bytes)
          }
        }
        val deleted = x._2.exists(_._1.get(MarkerKey) == MarkerDelete)
        val confirms = x._2.filter(_._1.as[String](MarkerKey).substring(0,1) == MarkerConfirmPrefix).map(_._1.as[String](MarkerKey).substring(2)).to[immutable.Seq]
        if (entry.nonEmpty) Some(entry.get.update(deleted = deleted, confirms = confirms)) else None
      } else None
    } map replayCallback
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = Future {
    val cursor = collection.find(snrQueryStatement(persistenceId)).sort(maxSnrSortStatement).limit(1)
    if (cursor.hasNext) cursor.next().getAs[Long](SequenceNrKey).get else 0L
  }
}
