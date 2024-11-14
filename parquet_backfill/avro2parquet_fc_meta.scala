import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path,FileChecksum}
import scala.collection.mutable.HashMap

def convert(fs:FileSystem,file:Path):Unit={
  //hdfs://yarn-cluster/prod/nifi/fc_meta/2023/06/27/old_fc_feed_metadata.avro
  println("Full path "+file.toString)
  val filename = file.toString
  val tokens = file.toString.split("/")
  val tableext = tokens(tokens.length-1)
  val table = tableext.substring(0,tableext.indexOf("."))
  val day = tokens(tokens.length-2)
  val month  = tokens(tokens.length-3)
  val year  = tokens(tokens.length-4)

  val basepath:String = filename.substring(0,filename.indexOf("."))
  val newfilename = "parquet_backfill/fc_meta/website/"+table+"/"+year+"/"+month+"/"+day
  println("Converting "+file.toString+" to "+newfilename)
  fs.delete(new Path(newfilename),true)
  val df = spark.read.format("avro").load(filename)
  df.repartition(1).write.mode("overwrite").parquet(newfilename)
  val path = new Path(newfilename+"/*.parquet")
  val files = fs.globStatus(path).map(_.getPath).filter(fs.isFile)
  println("Renaming "+files(0)+" to "+newfilename+".parquet")
  fs.rename(files(0),new Path(newfilename+".parquet"))
  fs.delete(new Path(newfilename),true)
}

val fs = FileSystem.get(new Configuration())
def main(path:Path):Unit={
  val files = fs.globStatus(path).map(_.getPath).filter(fs.isFile)
  if(files.length>0){
    var currentChecksum:FileChecksum = null
    var changed = false
    var md5map:HashMap[String,org.apache.hadoop.fs.FileChecksum] = new HashMap() 
    for (file<-files){
      convert(fs,file)
    }
  }
}

val base_path = "/prod/nifi/fc_meta"

def pad(x:Int):String={
  if(x<10) "0"+x.toString
  else x.toString
}

val path = new Path("/prod/nifi/fc_meta/????/??/??/*.avro")
println("Checking Path: "+path)
main(path)

