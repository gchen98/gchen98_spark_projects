import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path,FileChecksum}
import scala.collection.mutable.HashMap

val fs = FileSystem.get(new Configuration())

def delete_files(path:Path):Unit={
  val files = fs.globStatus(path).map(_.getPath).filter(fs.isFile)
  if(files.length>0){
    var lastChecksum:FileChecksum = null
    var currentChecksum:FileChecksum = null
    var md5map:HashMap[String,org.apache.hadoop.fs.FileChecksum] = new HashMap()
    for (file<-files){
      val tokens = file.toString.split("/")
      val day=tokens(tokens.length-1)
      val month = tokens(tokens.length-2)
      val year = tokens(tokens.length-3)
      val table = tokens(tokens.length-4)
      //println(table)
      currentChecksum = fs.getFileChecksum(file)
      if(md5map.contains(table)){
        lastChecksum = md5map(table)
      }
      if(currentChecksum==lastChecksum){
        //println("Table "+table+", file "+file+" not changed, deleting. "+" Last: "+lastChecksum+" Current: "+currentChecksum)
        fs.delete(file,true)
      }else{
        println("Table "+table+", file "+file+" changed, retaining. "+" Last: "+lastChecksum+" Current: "+currentChecksum)
      }
      md5map+=(table -> currentChecksum)
    }
  }
}


def pad(x:Int):String={
  if(x<10) "0"+x.toString
  else x.toString
}

//val path = new Path("parquet_backfill/fc_meta/website/*/????/??/??.parquet")
//val path = new Path("parquet_backfill/cse/gpdb/*/????/??/??.parquet")
//val path = new Path("parquet_backfill/fcapi/account/????/??/??.parquet")
//val path = new Path("parquet_backfill/iomgmt/*/????/??/??.parquet")
val path = new Path("parquet_backfill/openx/*/????/??/??.parquet")
println("Checking Path: "+path)
delete_files(path)

