package com.ufmg.dodetl.config
import com.ufmg.dodetl.utils.UtilDODETL
import org.json4s._

class ConfigLoader() extends Serializable{

  val configFolder = "conf/"

  implicit val formats = DefaultFormats

  def loadConfigStream(configName: String) ={

    var configJson = UtilDODETL.loadFileFromResource(configName)

    if(configJson.isEmpty){
      val configPath = configFolder + configName
      configJson = UtilDODETL.loadFile(configPath)
    }

    configJson
  }


  def loadTableConfigList() ={
    val tableConfigJson = loadConfigStream("tableConfig.json")
    UtilDODETL.jsonToPOJO[List[TableConfig]](tableConfigJson)
  }

  def loadGlobalConfig()={
    val configJson = loadConfigStream("globalConfig.json")
    UtilDODETL.jsonToPOJO[GlobalConfig](configJson)
  }

}
