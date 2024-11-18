import scala.collection.mutable.HashMap
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path,FileChecksum}

:load utilities.scala
:load schemas_partprice.scala
:load schemas_partprice_2.scala


val fs = FileSystem.get(new Configuration())
var md5map:HashMap[String,org.apache.hadoop.fs.FileChecksum] = new HashMap() 

def getHourAndConvert(dirPath:Path):Unit={
  var minHour:Int = 23
  val filePath = new Path(dirPath.toString+"/*.lzo")
  val files = fs.globStatus(filePath).map(_.getPath).filter(fs.isFile)
  var dirUnique:Boolean = false
  var currentChecksum:FileChecksum = null
  var lastChecksum:FileChecksum = null
  for(file<-files){
      val currentHour = getHourFromFile(fs,file)
      if(currentHour<minHour){
        minHour = currentHour
      }
      currentChecksum = fs.getFileChecksum(file)
      val filename:String = file.toString
      val tokens = filename.split("/")
      val rawfilename = tokens(tokens.length-1)
      val tokens2 = rawfilename.split("-")
      val table = tokens2(0)
      if(md5map.contains(table)){
        lastChecksum = md5map(table)
      }else{
        println("First time "+table+" encountered.")
        dirUnique = true
      }
      if(currentChecksum!=lastChecksum){
      //  println("Table "+table+" changed.")
        dirUnique = true
      }else{
        println("Table "+table+" not changed.")
      }
      md5map+=(table->currentChecksum)
  }
  println("Dir uniqueness "+dirUnique)
  if(dirUnique){
    for(file<-files){
       convert(minHour,fs,file)
    }
  }
}
def main(path:Path):Unit={
  val dirs = fs.globStatus(path).map(_.getPath).filter(fs.isDirectory)
  if(dirs.length>0){
    for(dir<-dirs){
      getHourAndConvert(dir)
    }
  }
  //val files = fs.globStatus(path).map(_.getPath).filter(fs.isFile)
//  if(files.length>0){
 //   for (file<-files){
  //    if(spark.read.format("text").load(file.toString).count>0L){
  //      convert(fs,file)
   //   }
    //}
  //} 
}


//val dirpath = new Path("/prod/etl/part_price_feed/2016/07/26")
val dirpath = new Path("/prod/etl/part_price_feed/????/??/??")
main(dirpath)

