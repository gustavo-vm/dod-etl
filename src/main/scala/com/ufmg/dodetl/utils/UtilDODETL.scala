package com.ufmg.dodetl.utils

import java.io.{BufferedReader, InputStreamReader}
import java.util.stream.Collectors

import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods.parse

import scala.io.Source

object UtilDODETL extends Serializable{

  implicit val formats = DefaultFormats

  def loadFile(p_Path: String): String ={
    Source.fromFile(p_Path).getLines.reduce((a,b) => a + "\n" + b)
  }

  def loadFileFromResource(fileName: String): String = {



    val stream = getClass.getResourceAsStream("/"+fileName)
    var content = ""
    if(stream != null) {
      content = scala.io.Source.fromInputStream( stream ).getLines.reduce(_+_)
    }
    content
  }

  def jsonToPOJO[T : Manifest](json: String)={
    parse(json, false).extract[T]
  }

  def jsonToMap(value: String) = {
    parse(value,false).extract[Map[String, Object]]
  }

}
