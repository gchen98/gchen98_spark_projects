import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path,FileChecksum}

val newdir = "parquet_backfill"
def convert(fs:FileSystem,file:Path):Unit={
  val filename:String = file.toString
  val basefile:String = filename.substring(0,filename.indexOf("."))
  val newbasefile:String = basefile.replace("hdfs://yarn-cluster/prod/nifi/dw/warehouse",newdir)
  fs.delete(new Path(newbasefile),true)
  println("Converting "+basefile+" to "+newbasefile)

  val df = spark.read.format("avro").load(filename)
  df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  val path = new Path(newbasefile+"/*.parquet")
  val files = fs.globStatus(path).map(_.getPath).filter(fs.isFile)
  println("Renaming "+files(0)+" to "+newbasefile+".parquet")
  fs.rename(files(0),new Path(newbasefile+".parquet"))
  fs.delete(new Path(newbasefile),true)
}

val fs = FileSystem.get(new Configuration())

def delete_dirs(path:Path):Unit={
  val dirs = fs.globStatus(path).map(_.getPath).filter(fs.isDirectory)
  println("DIR: "+path)
  if(dirs.length>0){
    for (dir<-dirs){
      val filePath = new Path(dir.toString+"/??.parquet")
      val files = fs.globStatus(filePath).map(_.getPath).filter(fs.isFile)
      if(files.length==0){
        println(filePath.toString+"  empty")
        fs.delete(dir,true)
      }
    }
  }
}
    
def delete_files(path:Path):Unit={
  val files = fs.globStatus(path).map(_.getPath).filter(fs.isFile)
  if(files.length>0){
    //var lastCount = 0L
    //var currentCount = 0L
    var lastChecksum:FileChecksum = null
    var currentChecksum:FileChecksum = null
    var changed = false
    var firstFile = files(0)
//    var lastmonth = 0
    for (file<-files){
      //currentCount = spark.read.format("parquet").load(file.toString).count
      currentChecksum = fs.getFileChecksum(file)
      if(currentChecksum==lastChecksum){
        println("file "+file+" not changed. "+" Last: "+lastChecksum+" Current: "+currentChecksum)
         //convert(fs,file)
        fs.delete(file,true)
      }
      //lastCount = currentCount
      lastChecksum = currentChecksum
//      lastmonth = month
    }
  }
}

val base_path = "parquet_backfill"
val dims=Array("action_dim")
//val dims=Array("banner_dim","company_dim","country_dim","master_account_file","naics","zone_dim")
//val dims=Array("action_dim","banner_dim","company_dim","country_dim","master_account_file","naics","zone_dim")

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

