//> using repository "https://ossartifacts.jfrog.io/artifactory/fdb-record-layer/"
//> using dep org.foundationdb:fdb-java:7.3.27
//> using dep org.foundationdb:fdb-record-layer-core:4.1.6.0
//> using dep dev.zio::zio:2.1.18
//> using dep dev.zio::zio-streams:2.1.18
//> using dep dev.zio::zio-cli:0.5.0

import com.apple.foundationdb.KeyValue
import com.apple.foundationdb.tuple.{ByteArrayUtil, Tuple}
import zio.*
import zio.cli.*
import zio.stream.ZStream

object Main extends ZIOCliDefault {

  sealed trait SubCommand extends Product with Serializable

  object SubCommand {

    // global flags
    val clusterFileFlag = Options.text("cluster-file").alias("C").withDefault("./fdb.cluster")
    val batchSizeFlag = Options.integer("batch").alias("b").withDefault(BigInt(100))
    val showValueFlag = Options.boolean("show-value").withDefault(false)
    val limitFlag = Options.integer("limit").alias("l").optional

    trait BatchConfig {
      def batchSize: BigInt
      def showValue: Boolean
      def limitOpt: Option[BigInt]
    }

    final case class SubspaceScan(
      name: String,
      clusterFile: String,
      batchSize: BigInt,
      showValue: Boolean,
      limitOpt: Option[BigInt]
    ) extends SubCommand,
          BatchConfig

    final case class RecordScan(
      store: String,
      clusterFile: String,
      batchSize: BigInt,
      showValue: Boolean,
      limitOpt: Option[BigInt]
    ) extends SubCommand,
          BatchConfig

  }

  val cliApp: CliApp[ZIOAppArgs & Scope, Throwable, SubCommand] = CliApp.make(
    name = "fdbscan",
    version = "0.1.0",
    summary = HelpDoc.Span.text("FDB scanning utilities"),
    command = Command("fdbscan").subcommands(SubspaceScan.command, RecordScan.command)
  ) {
    case (args: SubCommand.SubspaceScan) => SubspaceScan.scan(args)
    case (args: SubCommand.RecordScan)   => RecordScan.scan(args)
  }

  def printStream(stream: ZStream[Any, Throwable, KeyValue], args: SubCommand.BatchConfig) = {
    val takenStream = args.limitOpt
      .map(limit => stream.take(limit.longValue))
      .getOrElse(stream)
    takenStream.tap { it =>
      val tuple = Tuple.fromBytes(it.getKey)
      for {
        _ <- Console.printLine(f"=== ${tuple.getItems}")
        _ <- ZIO.when(args.showValue)(Console.printLine(ByteArrayUtil.printable(it.getValue)))
      } yield ()
    }.runDrain
  }

}
