import org.apache.spark.sql.types.{StructType, StructField, BooleanType, IntegerType, StringType, DoubleType}
import org.apache.spark.sql.Dataset

def str2bool(str:String):Boolean={
  if(str.equalsIgnoreCase("t") || str.equalsIgnoreCase("true")) {
    true
  }
  false
}

def str2int(str:String):Integer={
  try{
    Integer.parseInt(str)
  }catch{
    case e:Exception => null
  }
}
  

