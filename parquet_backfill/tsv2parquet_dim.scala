import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path,FileChecksum}

:load utilities.scala
:load schemas.scala

val newdir = "parquet_backfill"
def convert(dim:String, fs:FileSystem,file:Path):Unit={
  val filename:String = file.toString
  val basefile:String = filename.substring(0,filename.indexOf("."))
  var newbasefile:String = basefile.replace("hdfs://yarn-cluster/prod/etl/dim",newdir)
  newbasefile = newbasefile.replace("/data","")
  fs.delete(new Path(newbasefile),true)
  println("Converting "+basefile+" to "+newbasefile)
  if(dim.equals("action_dim")){
    val df = convert_action_dim(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if(dim.equals("banner_dim")){
    val df = convert_banner_dim(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if(dim.equals("company_dim")){
    val df = convert_company_dim(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if(dim.equals("country_dim")){
    val df = convert_country_dim(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if(dim.equals("zone_dim")){
    val df = convert_zone_dim(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }
  //val df = spark.read.format("text").load(filename)
  val path = new Path(newbasefile+"/*.parquet")
  val files = fs.globStatus(path).map(_.getPath).filter(fs.isFile)
  println("Renaming "+files(0)+" to "+newbasefile+".parquet")
  fs.rename(files(0),new Path(newbasefile+".parquet"))
  fs.delete(new Path(newbasefile),true)
}

val fs = FileSystem.get(new Configuration())
def main(dim:String,path:Path):Unit={
  val files = fs.globStatus(path).map(_.getPath).filter(fs.isFile)
  if(files.length>0){
    var lastCount = 0L
    var currentCount = 0L
    var lastChecksum:FileChecksum = null
    var currentChecksum:FileChecksum = null
    var changed = false
    var firstFile = files(0)
    for (file<-files){
      currentCount = spark.read.format("text").load(file.toString).count
      currentChecksum = fs.getFileChecksum(file)
      if(lastCount!=currentCount || lastChecksum!=currentChecksum){
        println("file "+file+" changed. "+" Last: "+lastChecksum+" Current: "+currentChecksum)
        convert(dim,fs,file)
        changed = true
      }
      lastCount = currentCount
      lastChecksum = currentChecksum
    }
  } 
}

val base_path = "/prod/etl/dim"
val dims=Array("action_dim","banner_dim","company_dim","country_dim","zone_dim")
def pad(x:Int):String={
  if(x<10) "0"+x.toString
  else x.toString
}

for (dim<-dims){
  for(year<-2013 to 2024){
    for(month<-1 to 12){
     val path = new Path(base_path+"/"+dim+"/"+pad(year)+"/"+pad(month)+"/??/data.tsv")
     println("Checking Path: "+path)
     main(dim,path)
    }
  }
}
