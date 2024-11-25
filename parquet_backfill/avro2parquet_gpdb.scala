import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path,FileChecksum}

:load utilities.scala

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

val path = new Path("/prod/nifi/cse/GPDB/*/????/??/??.avro")
//main(path)
//val file_path = new Path("parquet_backfill/cse/GPDB/*/????/??/??.parquet")
//delete_files(file_path)
delete_months(new Path("parquet_backfill/cse/GPDB/*/????/??"))
delete_years(new Path("parquet_backfill/cse/GPDB/*/????"))
