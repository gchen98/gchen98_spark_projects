import org.apache.spark.sql.types.{StructType, StructField, BooleanType, IntegerType, StringType, DoubleType}
import org.apache.spark.sql.Dataset

def str2bool(str:String):java.lang.Boolean={
  if(str==null){
    null
  }else if(str.equalsIgnoreCase("t") || str.equalsIgnoreCase("true")){
    true
  }else if(str.equalsIgnoreCase("f") || str.equalsIgnoreCase("false")){
    false
  } else{
    null
  } 
}


def str2int(str:String):java.lang.Integer={
  try{
    Integer.parseInt(str)
  }catch{
    case e:Exception => null
  }
}

def str2long(str:String):java.lang.Long={
  try{
    str.toLong
  }catch{
    case e:Exception => null
  }
}

def checknull(str:String):String={
  if (str==null || str.equals("\\N")){
    null
  } else{
    str
  }
}

def colcount(inPath:String):Integer={
  val df=spark.read.format("text").load(inPath+"header.tsv").map(lines=>lines(0).toString.split("\t").length)
  df.take(1)(0)
}
  

