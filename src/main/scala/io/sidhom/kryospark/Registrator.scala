package io.sidhom.kryospark

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

class Registrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[Int]) // bare class (for illustration; not necessary to register Int)
    kryo.register(classOf[Vector[_]]) // takes wild parameter
    kryo.register(classOf[Array[Vector[_]]]) // arrays require explicit first-level type parameter
  }
}
