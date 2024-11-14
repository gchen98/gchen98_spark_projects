import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path,FileChecksum}

val newdir = "parquet_backfill"
def convert(fs:FileSystem,file:Path):Unit={
  val filename:String = file.toString
  val basefile:String = filename.substring(0,filename.indexOf("."))
  val newbasefile:String = basefile.replace("hdfs://yarn-cluster/prod/nifi",newdir)
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
def main(path:Path):Unit={
  val files = fs.globStatus(path).map(_.getPath).filter(fs.isFile)
  if(files.length>0){
    for (file<-files){
      convert(fs,file)
    }
  }
}

val base_path = "/prod/nifi/fc_meta/website/*"
def pad(x:Int):String={
  if(x<10) "0"+x.toString
  else x.toString
}

for(year<-2024 to 2024){
  for(month<-3 to 12){
   val path = new Path(base_path+"/"+pad(year)+"/"+pad(month)+"/??.avro")
   println("Checking Path: "+path)
   main(path)
  }
}

