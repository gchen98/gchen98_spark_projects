import scala.collection.mutable.HashMap
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path,FileChecksum}
import scala.reflect.runtime.{universe=>ru}

:load utilities.scala
:load caseclasses_partprice.scala
:load schemas_partprice.scala


val fs = FileSystem.get(new Configuration())
var md5map:HashMap[String,org.apache.hadoop.fs.FileChecksum] = new HashMap() 

def convert(minHour:Int,fs:FileSystem,file:Path):Unit={
  val filename:String = file.toString
  val tokens = filename.split("/")
  val rawfilename = tokens(tokens.length-1)
  val tokens2 = rawfilename.split("-")
  val table = tokens2(0)
  val day = tokens(tokens.length-2)
  val month = tokens(tokens.length-3)
  val year = tokens(tokens.length-4)
  var newbasefile:String = "parquet_backfill/fc_feeds/"+table+"/"+year+"/"+month+"/"+day+"/"+pad(minHour)
  if(fs.exists(new Path(newbasefile+".parquet"))){
    println(newbasefile+".parquet exists already. Skipping")
    return
  }else{
    if(!converterMap.contains(table)){
      println("No converter defined for "+table)
    }else{
      println("Converting "+filename+" to "+newbasefile)
      fs.delete(new Path(newbasefile),true)
      //val className = table+"_converter"  // Class name as a string
      //    try {
      // Load the class using reflection
      //val clazz = this.getClass.getClassLoader.loadClass(className)
      //val clazz = Class.forName(className)
      // Instantiate the class (assuming no-arg constructor)
      //val instance = clazz.getDeclaredConstructor().newInstance()
      val instance = converterMap(table)
      // Cast the object to the expected type if necessary
      //val myClassInstance = obj.asInstanceOf[MyClass]
      // Use the instance
      //val df:DataFrame = obj.convert(filename)
      //val df:DataFrame = myClassInstance.convert(filename)
      //} catch {
      // case e: Exception => println(s"Error: ${e.getMessage}")
      //}
      //val instance = new Schemas()
      val mirror = ru.runtimeMirror(instance.getClass.getClassLoader)
      val classSymbol = mirror.classSymbol(instance.getClass)
      val instanceMirror = mirror.reflect(instance)
      //try{
      //println("Looking for method "+"convert_"+table)
      val methodSymbol = classSymbol.toType.decl(ru.TermName("convert")).asMethod
      //val methodSymbol = classSymbol.toType.decl(ru.TermName("convert_"+table)).asMethod
      //println("Got method symbol")
      val method = instanceMirror.reflectMethod(methodSymbol)
      //println("Got method")
      val df:DataFrame = method(filename).asInstanceOf[DataFrame]
      //println("Got df")
      df.repartition(1).write.mode("overwrite").parquet(newbasefile)
      //println("wrote")
      val path = new Path(newbasefile+"/*.parquet")
      //println("new path "+path.toString)
      val files = fs.globStatus(path).map(_.getPath).filter(fs.isFile)
      //println("got files")
      if(files.length>0){
        println("Renaming "+files(0)+" to "+newbasefile+".parquet")
        fs.rename(files(0),new Path(newbasefile+".parquet"))
        fs.delete(new Path(newbasefile),true)
      }else{
        println("No parquet output files were written")
      }
    }
  }
}

def getHourAndConvert(dirPath:Path):Unit={
  var minHour:Int = 23
  val filePath = new Path(dirPath.toString+"/*.lzo")
  val files = fs.globStatus(filePath).map(_.getPath).filter(fs.isFile)
  var dirUnique:Boolean = false
  var currentChecksum:FileChecksum = null
  var lastChecksum:FileChecksum = null
  for(file<-files){
      val currentHour = getHourFromFile(fs,file)
      if(currentHour<minHour){
        minHour = currentHour
      }
      currentChecksum = fs.getFileChecksum(file)
      val filename:String = file.toString
      val tokens = filename.split("/")
      val rawfilename = tokens(tokens.length-1)
      val tokens2 = rawfilename.split("-")
      val table = tokens2(0)
      if(md5map.contains(table)){
        lastChecksum = md5map(table)
      }else{
        println("First time "+table+" encountered.")
        dirUnique = true
      }
      if(currentChecksum!=lastChecksum){
      //  println("Table "+table+" changed.")
        dirUnique = true
      }else{
        println("Table "+table+" not changed.")
      }
      md5map+=(table->currentChecksum)
  }
  println("Dir uniqueness "+dirUnique)
  if(dirUnique){
    for(file<-files){
       convert(minHour,fs,file)
    }
  }
}
def main(path:Path):Unit={
  val dirs = fs.globStatus(path).map(_.getPath).filter(fs.isDirectory)
  if(dirs.length>0){
    for(dir<-dirs){
      getHourAndConvert(dir)
    }
  }
}


//val dirpath = new Path("/prod/etl/part_price_feed/2017/07/20")
val dirpath = new Path("/prod/etl/part_price_feed/????/??/??")
main(dirpath)


