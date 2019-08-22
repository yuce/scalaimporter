import com.pilosa.client.orm.Record
import com.pilosa.client.{Column, ImportOptions, PilosaClient, RecordIterator}


object Main {
  def main(args: Array[String]): Unit = {
    val client = PilosaClient.defaultClient()
    val schema = client.readSchema()
    val index = schema.index("i1")
    val field = index.field("f1")
    client.syncSchema(schema)

    val iterator = new XColumnIterator(300000000, 1000000)
    val importOptions = ImportOptions.builder()
      .setBatchSize(100000)
      .setRoaring(true)
//      .setThreadCount(2)
//      .setStrategy(ImportOptions.Strategy.TIMEOUT)
//      .setTimeoutMs(5)
      .build()
    client.importField(field, iterator, importOptions)
  }
}

class XColumnIterator(var maxID: Long, var maxColumns: Long) extends RecordIterator {
  override def hasNext: Boolean = {
    this.maxColumns > 0
  }

  override def next(): Record = {
    this.maxColumns -= 1
    val rowID = (Math.random() * this.maxID).toLong
    val columnID = (Math.random() * this.maxID).toLong
    Column.create(rowID, columnID)
  }
}