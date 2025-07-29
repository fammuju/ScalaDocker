import org.apache.spark.sql.{Dataset, SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.broadcast.Broadcast

object Main {
  case class User(name: Option[String])
  case class Person(name: String, age: Int)
  case class MyData(id: Int, value: Double) extends Serializable

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // 1. val vs var en particiones
    val rdd = spark.sparkContext.parallelize(Seq(1, 2, 3, 4))
    val accumRdd = rdd.mapPartitions { iter =>
      var acc = 0
      iter.map(x => { acc += x; (x, acc) })
    }

    // 2. Uso de Option
    val u = User(None)
    println(getName(u))

    // 3. Serialización ya está en config y case class

    // 4. Broadcast
    val bigDF = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "val")
    val smallDF = Seq((1, "x"), (3, "z")).toDF("id", "aux")
    val joined = bigDF.join(broadcast(smallDF), "id")

    // 5. Shuffle
    val grouped = bigDF.groupBy("val").count()

    // 6. Collect
    val preview = bigDF.limit(2).collect()

    // 7. mapPartitions con recurso
    rdd.mapPartitions { partition =>
      val db = DummyDB.connect()
      val result = partition.map(x => db.query(x))
      db.close()
      result
    }.foreach(println)

    // 8. lazy
    lazy val expensiveDF = spark.read.parquet("/tmp/bigdata") // dummy path

    // 9. RDD vs DF vs DS

    val seq:Seq[String] = Seq("{{'name':'memo','age':30}}")

    val df = spark.read.json(seq.toDS())
    val ds = df.as[Person]
    val rddFromDF = df.rdd

    // 10. Desacoplar lógica
    val adults = df.filter(row => isAdult(row.getAs[Int]("age")))

    // 11. Funciones puras
    val doubled = rdd.map(double)

    // 12. identity
    val identityList = List(1, 2, 3).map(identity)

    // Pattern Matching
    println(patternExample("error"))

    spark.stop()
  }

  def getName(user: User): String = user.name.getOrElse("UNKNOWN").toUpperCase

  def isAdult(age: Int): Boolean = age >= 18

  def double(x: Int): Int = x * 2

  def patternExample(status: String): String = status match {
    case "ok" => "Everything is fine"
    case "error" => "There was an error"
    case other => s"Unknown status: $other"
  }

  object DummyDB {
    def connect(): DummyDB = new DummyDB()
  }

  class DummyDB {
    def query(x: Int): String = s"Result for $x"
    def close(): Unit = println("DB closed")
  }
}
