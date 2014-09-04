package io.sidhom.kryospark

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import com.twitter.chill.MeatLocker
import java.io.ByteArrayOutputStream
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
object SaveKryo {

  def main(args: Array[String]): Unit = {
    val javaOut = args(0)
    val kryoOut = args(1)
    val conf = new SparkConf().setMaster("local").setAppName("kryo serialization")
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.set("spark.kryo.registrator", classOf[Registrator].getName)
    conf.set("spark.kryo.registrationRequired", "true")
    implicit val sc = new SparkContext(conf)
    val rand = new java.util.Random()
    val input = for {
      _ <- (0 until 1000000).toArray
    } yield {
      (0 until 10).map(_ => rand.nextInt()).toVector
    }
    val rdd = sc.parallelize(input)
    //rdd.saveAsObjectFile(javaOut)
    saveAsObjectFile(rdd, kryoOut)
    sc.stop()
  }

  def saveAsObjectFile[T](rdd: RDD[T], path: String)(implicit ct: ClassTag[T], sc: SparkContext): Unit = {
    val confLocker = MeatLocker(sc.getConf)
    rdd.mapPartitions({ iter =>
      val k = new org.apache.spark.serializer.KryoSerializer(confLocker.get).newKryo()
      iter.grouped(100).map(_.toArray).map(x => (NullWritable.get(), new BytesWritable(serialize(k, x))))
    })
      .saveAsSequenceFile(path)
  }

  def serialize[T](k: Kryo, t: T): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val output = new Output(baos)
    k.writeClassAndObject(output, t)
    output.close()
    baos.toByteArray()
  }
}

