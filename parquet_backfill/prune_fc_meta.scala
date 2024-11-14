import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path,FileChecksum}
import scala.collection.mutable.HashMap

val fs = FileSystem.get(new Configuration())

def delete_dirs(path:Path):Unit={
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
    
def delete_files(path:Path):Unit={
  val files = fs.globStatus(path).map(_.getPath).filter(fs.isFile)
  if(files.length>0){
    var lastChecksum:FileChecksum = null
    var currentChecksum:FileChecksum = null
    var md5map:HashMap[String,org.apache.hadoop.fs.FileChecksum] = new HashMap()
    for (file<-files){
      val tokens = file.toString.split("/")
      val filename=tokens(tokens.length-1)
      val month = tokens(tokens.length-2)
      val year = tokens(tokens.length-3)
      val table = tokens(tokens.length-4)
      println(table)
      currentChecksum = fs.getFileChecksum(file)
      if(md5map.contains(table)){
        lastChecksum = md5map(table)
      }
      if(currentChecksum==lastChecksum){
        println("File "+file+" not changed, deleting. "+" Last: "+lastChecksum+" Current: "+currentChecksum)
        fs.delete(file,true)
      }
      md5map+=(table -> currentChecksum)
    }
  }
}


def pad(x:Int):String={
  if(x<10) "0"+x.toString
  else x.toString
}

val path = new Path("parquet_backfill/fc_meta/website/*/????/??/??.parquet")
println("Checking Path: "+path)
delete_files(path)
//  val path2 = new Path(base_path+"/fc_meta/????/??")
//  delete_dirs(path2)

