import java.sql.Timestamp
import java.time.LocalDateTime
import scala.collection.mutable.HashMap

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path,FileChecksum}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Dataset,DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, BooleanType, IntegerType, StringType, DoubleType}


def differs(df1: DataFrame, df2: DataFrame):Boolean={
  if(df1==null || df2==null){
    println("One of the dataframes is null returning true")
    return true
  }
  val df1ColLen = df1.columns.length
  val df2ColLen = df2.columns.length
  if(df2ColLen!=df1ColLen){
    println("Number of columns of the dataframes differ, returning true")
    return true
  }
  val df1WithId = df1.withColumn("source", lit("df1"))
  val df2WithId = df2.withColumn("source", lit("df2"))

  try{
    val joinedDF = df1WithId.union(df2WithId).groupBy(df1.columns.map(col): _*).agg(collect_set("source").alias("sources"))

    val differences = joinedDF.filter(size(col("sources")) =!= 2)
  
    if (differences.isEmpty) {
      println("DataFrames are exactly the same.")
      return false
    } else {
      println("DataFrames are different.")
      //differences.show()
      return true
    }
  }catch{
    case e:org.apache.spark.sql.AnalysisException => {
      println("Analysis exception found with message "+e.getMessage)
      return true
    }
  }
  true
}


def getHourFromFile(fs:FileSystem,filePath:Path):Int={
  val fileStatus = fs.getFileStatus(filePath)
  val modificationTime = fileStatus.getModificationTime
  val timestamp = new Timestamp(modificationTime)
  val localDateTime = timestamp.toLocalDateTime
  val hour = localDateTime.getHour
  hour
}

def str2bool(str:String):java.lang.Boolean={
  if(str==null){
    null
  }else if(str.equalsIgnoreCase("t") || str.equalsIgnoreCase("true")){
    true
  }else if(str.equalsIgnoreCase("f") || str.equalsIgnoreCase("false")){
    false
  } else{
    null
  } 
}


def str2int(str:String):java.lang.Integer={
  try{
    Integer.parseInt(str)
  }catch{
    case e:Exception => null
  }
}

def str2long(str:String):java.lang.Long={
  try{
    str.toLong
  }catch{
    case e:Exception => null
  }
}

def str2float(str:String):java.lang.Float={
  try{
    str.toFloat
  }catch{
    case e:Exception => null
  }
}

def checknull(str:String):String={
  if (str==null || str.equals("\\N")){
    null
  } else{
    str
  }
}

def colcount(inPath:String):Integer={
  val df=spark.read.format("text").load(inPath+"header.tsv").map(lines=>lines(0).toString.split("\t").length)
  df.take(1)(0)
}
  

def pad(x:Int):String={
  if(x<10) "0"+x.toString
  else x.toString
}

val fs = FileSystem.get(new Configuration())

def delete_files(path:Path):Unit={
  val files = fs.globStatus(path).map(_.getPath).filter(fs.isFile)
  if(files.length>0){
    var lastChecksum:FileChecksum = null
    var lastPath:Path = null
    var currentChecksum:FileChecksum = null
    var md5map:HashMap[String,org.apache.hadoop.fs.FileChecksum] = new HashMap()
    var pathMap:HashMap[String,Path] = new HashMap()
    for (file<-files){
      var updateMap = true
      var currentFile = file
      val tokens = currentFile.toString.split("/")
      val day=tokens(tokens.length-1)
      val month = tokens(tokens.length-2)
      val year = tokens(tokens.length-3)
      val table = tokens(tokens.length-4)
      //println(table)
      currentChecksum = fs.getFileChecksum(currentFile)
      if(md5map.contains(table)){
        lastChecksum = md5map(table)
      }
      if(pathMap.contains(table)){
        lastPath = pathMap(table)
      }
      if(currentChecksum==lastChecksum){
        println("Table "+table+", currentFile "+currentFile+" not changed, deleting. "+" Last: "+lastChecksum+" Current: "+currentChecksum)
        fs.delete(currentFile,true)
        updateMap = false
      }else{
        println("Table "+table+", currentFile "+currentFile+" changed, may retain last: "+lastChecksum+" current: "+currentChecksum+ " for stage 1 check")
        println("Last path "+lastPath+" currentPath "+currentFile)
        if(lastPath!=null){
          println("Running stage 2 check")
          val diff = differs(spark.read.format("parquet").load(lastPath.toString),spark.read.format("parquet").load(currentFile.toString))
          if(!diff){
            println("Deleting "+currentFile)
            fs.delete(currentFile)
            updateMap = false
          }
        }
      }
      if(updateMap){
        println("Updating map with current checksum and path")
        md5map+=(table -> currentChecksum)
        pathMap+=(table -> currentFile)
      }
    }
  }
}


def delete_months(path:Path):Unit={
  val dirs = fs.globStatus(path).map(_.getPath).filter(fs.isDirectory)
  if(dirs.length>0){
    for (dir<-dirs){
      val filePath = new Path(dir.toString+"/??.parquet")
      val files = fs.globStatus(filePath).map(_.getPath).filter(fs.isFile)
      if(files.length==0){
        println(filePath.toString+"  empty, deleting.")
        fs.delete(dir,true)
      }
    }
  }
}

def delete_years(path:Path):Unit={
  val dirs = fs.globStatus(path).map(_.getPath).filter(fs.isDirectory)
  if(dirs.length>0){
    for (dir<-dirs){
      val filePath = new Path(dir.toString+"/*.parquet")
      val files = fs.globStatus(filePath).map(_.getPath).filter(fs.isFile)
      val dirPath = new Path(dir.toString+"/??")
      val subdirs = fs.globStatus(dirPath).map(_.getPath).filter(fs.isDirectory)
      if(subdirs.length==0 && files.length==0){
        println(dirPath.toString+"  empty, deleting.")
        fs.delete(dir,true)
      }
    }
  }
}
    
