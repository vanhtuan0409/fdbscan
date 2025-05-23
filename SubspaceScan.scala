import com.apple.foundationdb.FDB
import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.tuple.{ByteArrayUtil, Tuple}
import zio.*
import zio.cli.*

object SubspaceScan {

  private val options =
    Options.text("subspace-name").alias("n")
      ++ Main.SubCommand.clusterFileFlag
      ++ Main.SubCommand.batchSizeFlag
      ++ Main.SubCommand.showValueFlag
      ++ Main.SubCommand.limitFlag

  val command: Command[Main.SubCommand.SubspaceScan] =
    Command("subspace", options)
      .withHelp("scan subspace for key and value")
      .map(Main.SubCommand.SubspaceScan.apply)

  def scan(args: Main.SubCommand.SubspaceScan): Task[Unit] = {
    val fdb = FDB.selectAPIVersion(Fdb.ApiVersion)
    val db = fdb.open(args.clusterFile)

    val subspace = Subspace(Tuple.from(args.name))
    val stream = Fdb.getRangeBatch(db, subspace, args.batchSize.intValue)
    Main.printStream(stream, args)
  }

}
