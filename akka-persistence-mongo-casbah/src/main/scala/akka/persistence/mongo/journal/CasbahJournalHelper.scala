/**
 *  Copyright (C) 2013-2014 Duncan DeVore. <https://github.com/ironfish/>
 */
package akka.persistence.mongo.journal

import akka.actor.ActorLogging
import akka.persistence.PersistentRepr
import akka.persistence.mongo.{IndexesSupport, MongoPersistenceJournalRoot}
import com.mongodb.casbah.Imports._

private[mongo] trait CasbahJournalHelper extends MongoPersistenceJournalRoot with IndexesSupport with BSONSerializationSupport {
  mixin : ActorLogging =>
  val PersistenceIdKey = "persistenceId"
  val SequenceNrKey = "sequenceNr"
  val AggIdKey = "_id"
  val AddDetailsKey = "details"
  val MarkerKey = "marker"
  val MessageKey = "message"
  val TypeHintKey = "t"
  val MarkerAccepted = "A"
  val MarkerConfirmPrefix = "C"
  def markerConfirm(cId: String) = s"C-$cId"
  def markerConfirmParsePrefix(cId: String) = cId.substring(0,1)
  def markerConfirmParseSuffix(cId: String) = cId.substring(2)
  val MarkerDelete = "D"
  
  private[this] val idx1 = MongoDBObject(
    "persistenceId"         -> 1,
    "sequenceNr"          -> 1,
    "marker"              -> 1)

  private[this] val idx1Options =
    MongoDBObject("unique" -> true)

  private[this] val idx2 = MongoDBObject(
    "persistenceId"         -> 1,
    "sequenceNr"          -> 1)

  private[this] val idx3 =
    MongoDBObject("sequenceNr" -> 1)

  private[this] val uri = MongoClientURI(configMongoJournalUrl)
  val client =  MongoClient(uri)
  private[this] val db = client(uri.database.getOrElse(throw new Exception("Cannot get database out of the mongodb URI, probably invalid format")))
  val collection = db(uri.collection.getOrElse(throw new Exception("Cannot get collection out of the mongodb URI, probably invalid format")))

  ensure(idx1, idx1Options)(collection)
  ensure(idx2)(collection)
  ensure(idx3)(collection)

  def writeJSON(pId: String, sNr: Long, pr: PersistentRepr) = {
    val builder = MongoDBObject.newBuilder
    builder += PersistenceIdKey -> pId
    builder += SequenceNrKey  -> sNr
    builder += MarkerKey      -> MarkerAccepted

    bsonSerialization.map { bs =>
      builder += TypeHintKey -> pr.payload.getClass.getName
      builder += MessageKey -> bs.toBSON(pr.payload)
    } getOrElse {
        builder += MessageKey -> toBytes(pr)
    }

    builder.result()
  }

  def confirmJSON(pId: String, sNr: Long, cId: String) = {
    val builder = MongoDBObject.newBuilder
    builder += PersistenceIdKey -> pId
    builder += SequenceNrKey  -> sNr
    builder += MarkerKey      -> markerConfirm(cId)
    builder += MessageKey     -> Array.empty[Byte]
    builder.result()
  }

  def deleteMarkJSON(pId: String, sNr: Long) = {
    val builder = MongoDBObject.newBuilder
    builder += PersistenceIdKey -> pId
    builder += SequenceNrKey  -> sNr
    builder += MarkerKey      -> MarkerDelete
    builder += MessageKey     -> Array.empty[Byte]
    builder.result()
  }

  def delStatement(persistenceId: String, sequenceNr: Long): MongoDBObject =
    MongoDBObject(PersistenceIdKey -> persistenceId, SequenceNrKey -> sequenceNr)

  def delToStatement(persistenceId: String, toSequenceNr: Long): MongoDBObject =
    MongoDBObject(
      PersistenceIdKey -> persistenceId,
      SequenceNrKey  -> MongoDBObject("$lte" -> toSequenceNr))

  def delOrStatement(elements: List[MongoDBObject]): MongoDBObject =
    MongoDBObject("$or" -> elements)

  def replayFindStatement(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): MongoDBObject =
    MongoDBObject(
      PersistenceIdKey -> persistenceId,
      SequenceNrKey  -> MongoDBObject("$gte" -> fromSequenceNr, "$lte" -> toSequenceNr))

  def recoverySortStatement = MongoDBObject(
    "persistenceId" -> 1,
    "sequenceNr"  -> 1,
    "marker"      -> 1)

  def snrQueryStatement(persistenceId: String): MongoDBObject =
    MongoDBObject(PersistenceIdKey -> persistenceId)

  def maxSnrSortStatement: MongoDBObject =
    MongoDBObject(SequenceNrKey -> -1)

  def minSnrSortStatement: MongoDBObject =
    MongoDBObject(SequenceNrKey -> 1)

  def casbahJournalWriteConcern: WriteConcern = configMongoJournalWriteConcern
}
