import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path,FileChecksum}
import scala.collection.mutable.HashMap

val fs = FileSystem.get(new Configuration())

def main(path:Path):Unit={
  val files = fs.globStatus(path).map(_.getPath).filter(fs.isFile)
  if(files.length>0){
    var structMap:HashMap[String,org.apache.spark.sql.types.StructType] = new HashMap()
    for (file<-files){
      val tokens = file.toString.split("/")
      val table = tokens(tokens.length-1).replace(".avro","")
      val day = tokens(tokens.length-2)
      val month = tokens(tokens.length-3)
      val year = tokens(tokens.length-4)
      //println(table)
      if(!structMap.contains(table)){
        val schema = spark.read.format("avro").load(file.toString).schema
        structMap+=(table -> schema)
      }
    }
    for ((key, value) <- structMap) {
      var s = value.toString
      val s2 = s.substring(11,s.length-1)
      println(s"$key\n$s2\n")
    }
  }
}


def pad(x:Int):String={
  if(x<10) "0"+x.toString
  else x.toString
}

val path = new Path("/prod/nifi/ccr/????/??/??/*.avro")
println("Checking Path: "+path)
main(path)
//  val path2 = new Path(base_path+"/fc_meta/????/??")
//  delete_dirs(path2)

