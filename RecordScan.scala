import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext
import com.apple.foundationdb.record.provider.foundationdb.keyspace.DirectoryLayerDirectory
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace
import com.apple.foundationdb.tuple.ByteArrayUtil
import com.apple.foundationdb.tuple.Tuple
import java.util.concurrent.CompletableFuture
import zio.*
import zio.cli.*

import scala.jdk.CollectionConverters.*

object RecordScan {

  val RootDirName = "anduin"

  private val options =
    Options.text("store-name").alias("n")
      ++ Main.SubCommand.clusterFileFlag
      ++ Main.SubCommand.batchSizeFlag
      ++ Main.SubCommand.showValueFlag
      ++ Main.SubCommand.limitFlag

  val command: Command[Main.SubCommand.RecordScan] =
    Command("record", options)
      .withHelp("scan record io")
      .map(Main.SubCommand.RecordScan.apply)

  private def subspaceForStore(ctx: FDBRecordContext, store: String) = {
    val rootDir = new DirectoryLayerDirectory(RootDirName)
    val keySpace = new KeySpace(rootDir)
    val keyPath = keySpace.path(RootDirName, store + '$')
    keyPath.toSubspace(ctx)
  }

  def scan(args: Main.SubCommand.RecordScan): Task[Unit] = {

    val factory = FDBDatabaseFactory.instance()
    val recordDb = factory.getDatabase(args.clusterFile)

    for {
      subspace <- ZIO.fromCompletableFuture(recordDb.runAsync { ctx =>
        CompletableFuture.completedFuture(
          subspaceForStore(ctx, args.store)
        )
      })
      stream = Fdb.getRangeBatch(recordDb.database, subspace, args.batchSize.intValue)
      _ <- Main.printStream(stream, args)
    } yield ()
  }

}
