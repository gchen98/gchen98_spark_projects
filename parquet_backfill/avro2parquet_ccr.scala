import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path,FileChecksum}

val newdir = "parquet_backfill"
def convert(fs:FileSystem,file:Path):Unit={
  val filename:String = file.toString
  val tokens = filename.split("/")
  val table = tokens(tokens.length-1).replace(".avro","")
  val day = tokens(tokens.length-2)
  val month  = tokens(tokens.length-3)
  val year = tokens(tokens.length-4)
  val newbasefile:String = "parquet_backfill/ccr/"+table+"/"+year+"/"+month+"/"+day
  println("Converting "+filename+" to "+newbasefile)
  fs.delete(new Path(newbasefile),true)
  val df = spark.read.format("avro").load(filename)
  df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  val path = new Path(newbasefile+"/*.parquet")
  val files = fs.globStatus(path).map(_.getPath).filter(fs.isFile)
  println("Renaming "+files(0)+" to "+newbasefile+".parquet")
  fs.rename(files(0),new Path(newbasefile+".parquet"))
  fs.delete(new Path(newbasefile),true)
}

val fs = FileSystem.get(new Configuration())
def main(path:Path):Unit={
  val files = fs.globStatus(path).map(_.getPath).filter(fs.isFile)
  if(files.length>0){
    for (file<-files){
      convert(fs,file)
    }
  }
}

val path = new Path("/prod/nifi/ccr/????/??/??/*.avro")
main(path)

