package HBaseUsage

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes

object ListTables {
   def main (args: Array[String]) {

    val conf = new HBaseConfiguration()
    val connection = ConnectionFactory.createConnection(conf)
    val admin = connection.getAdmin()

    // list the tables
    val listtables=admin.listTables()
    listtables.foreach(println)

    // let's insert some data in 'mytable' and get the row

    val table = new HTable(conf, "mytable")

    val theput= new Put(Bytes.toBytes("rowkey1"))

    theput.add(Bytes.toBytes("ids"),Bytes.toBytes("id1"),Bytes.toBytes("one"))
    table.put(theput)

    val theget= new Get(Bytes.toBytes("rowkey1"))
    val result=table.get(theget)
    val value=result.value()
    println(Bytes.toString(value))

  }
}
