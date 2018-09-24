package com.ufmg.dodetl.utils

import org.apache.spark.sql.{Encoder, Encoders, Row}

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

object DODETLEncoders extends Serializable {

  implicit def myDataEncoder: org.apache.spark.sql.Encoder[Map[String,Object]] =
    org.apache.spark.sql.Encoders.kryo[Map[String,Object]]

  implicit def myDataEncoder1: org.apache.spark.sql.Encoder[ListBuffer[Map[String, Object]]] =
    org.apache.spark.sql.Encoders.kryo[ListBuffer[Map[String, Object]]]

  implicit def mydataEncoder2: org.apache.spark.sql.Encoder[Object] =
    org.apache.spark.sql.Encoders.kryo[Object]

  implicit def mydataEncoder3: org.apache.spark.sql.Encoder[Row] =
    org.apache.spark.sql.Encoders.kryo[Row]

  implicit def single[A](implicit c: ClassTag[A]): Encoder[A] = Encoders.kryo[A](c)

  implicit def tuple2[A1, A2](
                               implicit e1: Encoder[A1],
                               e2: Encoder[A2]
                             ): Encoder[(A1, A2)] = Encoders.tuple[A1, A2](e1, e2)

}
