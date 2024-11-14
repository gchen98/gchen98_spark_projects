import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path,FileChecksum}

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
    for (file<-files){
      //currentCount = spark.read.format("parquet").load(file.toString).count
      currentChecksum = fs.getFileChecksum(file)
      if(currentChecksum==lastChecksum){
        println("file "+file+" not changed, deleting. "+" Last: "+lastChecksum+" Current: "+currentChecksum)
        fs.delete(file,true)
      }
      lastChecksum = currentChecksum
    }
  }
}

val base_path = "parquet_backfill"
val dims=Array("action_dim","banner_dim","company_dim","country_dim","master_account_file","naics","zone_dim")

def pad(x:Int):String={
  if(x<10) "0"+x.toString
  else x.toString
}

for(dim<-dims){
  val path = new Path(base_path+"/"+dim+"/????/??/??.parquet")
  println("Checking Path: "+path)
  delete_files(path)
  val path2 = new Path(base_path+"/"+dim+"/????/??")
  delete_dirs(path2)
}

