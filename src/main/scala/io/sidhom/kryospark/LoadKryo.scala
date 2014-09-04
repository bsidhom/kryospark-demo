package io.sidhom.kryospark

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import com.twitter.chill.MeatLocker
import java.io.ByteArrayInputStream
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.serializer.KryoSerializer
import scala.reflect.ClassTag

/**
 * @author bsidhom
 */
object LoadKryo {

  def main(args: Array[String]): Unit = {
    val inputPath = args(0)
    val conf = new SparkConf().setMaster("local").setAppName("kryo deserialization")
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.set("spark.kryo.registrator", classOf[Registrator].getName)
    conf.set("spark.kryo.registrationRequired", "true")
    implicit val sc = new SparkContext(conf)
    //val rdd = sc.objectFile[Vector[Int]](inputPath)
    val rdd = objectFile[Vector[Int]](inputPath)
    val sum = rdd.map(_.sum.toLong).reduce(_ + _)
    println(s"TOTAL SUM: $sum")
    sc.stop()
  }

  def objectFile[T](path: String)(implicit sc: SparkContext, ct: ClassTag[T]): RDD[T] = {
    val confLocker = MeatLocker(sc.getConf)
    sc.sequenceFile(path, classOf[NullWritable], classOf[BytesWritable], 2)
      .mapPartitions({ iter =>
        val k = new org.apache.spark.serializer.KryoSerializer(confLocker.get).newKryo()
        iter.map(x => deserialize[Array[T]](k, x._2.getBytes))
      })
      .flatMap(_.iterator)
  }

  def deserialize[T](k: Kryo, bytes: Array[Byte]): T = {
    val bais = new ByteArrayInputStream(bytes)
    val input = new Input(bais)
    k.readClassAndObject(input).asInstanceOf[T]
  }
}

