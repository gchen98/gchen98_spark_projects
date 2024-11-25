import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path,FileChecksum}
import scala.collection.mutable.HashMap

:load utilities.scala

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
        //println("Table "+table+", currentFile "+currentFile+" not changed, deleting. "+" Last: "+lastChecksum+" Current: "+currentChecksum)
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


def pad(x:Int):String={
  if(x<10) "0"+x.toString
  else x.toString
}

//val path = new Path("parquet_backfill/fc_meta/website/*/????/??/??.parquet")
val path = new Path("parquet_backfill/fc_meta/website/fc_feed_metadata/2024/08/??.parquet")
//val path = new Path("parquet_backfill/cse/gpdb/*/????/??/??.parquet")
//val path = new Path("parquet_backfill/cse/gpdb/*/????/??/??.parquet")
//val path = new Path("parquet_backfill/fcapi/account/????/??/??.parquet")
//val path = new Path("parquet_backfill/iomgmt/*/????/??/??.parquet")
//val path = new Path("parquet_backfill/openx/*/????/??/??.parquet")
println("Checking Path: "+path)
delete_files(path)

