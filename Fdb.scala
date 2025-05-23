import scala.jdk.CollectionConverters.*

import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.{Database, KeySelector, KeyValue}
import zio.*
import zio.stream.*

object Fdb {
  val ApiVersion = 730

  def getRangeBatch(db: Database, subspace: Subspace, batchSize: Int): ZStream[Any, Throwable, KeyValue] = {
    val range = subspace.range()
    val keyEnd = KeySelector.lastLessOrEqual(range.end)

    ZStream.paginateChunkZIO[Any, Throwable, KeyValue, Array[Byte]](range.begin) { currentKey =>
      val keyBegin =
        if currentKey.sameElements(range.begin)
        then KeySelector.firstGreaterOrEqual(currentKey)
        else KeySelector.firstGreaterThan(currentKey)

      for {
        pairs <- ZIO.fromCompletableFuture(db.runAsync { trx =>
          trx.getRange(keyBegin, keyEnd, batchSize).asList()
        })
        seq = pairs.asScala.toSeq
      } yield {
        if (seq.isEmpty) {
          (Chunk.empty, None)
        } else {
          (Chunk.fromIterable(seq), Some(seq.last.getKey))
        }
      }
    }
  }

}
