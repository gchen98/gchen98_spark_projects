import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path,FileChecksum}
import scala.collection.mutable.HashMap

val fs = FileSystem.get(new Configuration())

def delete_months(path:Path):Unit={
  val dirs = fs.globStatus(path).map(_.getPath).filter(fs.isDirectory)
  if(dirs.length>0){
    for (dir<-dirs){
      val filePath = new Path(dir.toString+"/??.parquet")
      val files = fs.globStatus(filePath).map(_.getPath).filter(fs.isFile)
      if(files.length==0){
        println(filePath.toString+"  empty, deleting.")
        fs.delete(dir,true)
      }
    }
  }
}

def delete_years(path:Path):Unit={
  val dirs = fs.globStatus(path).map(_.getPath).filter(fs.isDirectory)
  if(dirs.length>0){
    for (dir<-dirs){
      val dirPath = new Path(dir.toString+"/??")
      val subdirs = fs.globStatus(dirPath).map(_.getPath).filter(fs.isDirectory)
      if(subdirs.length==0){
        println(dirPath.toString+"  empty, deleting.")
        fs.delete(dir,true)
      }
    }
  }
}
    

val path_year = new Path("parquet_backfill/fc_meta/website/*/????")
val path_month = new Path("parquet_backfill/fc_meta/website/*/????/??")
//val path_month = new Path("parquet_backfill/cse/GPDB/*/????/??")
//val path_month = new Path("parquet_backfill/fcapi/account/????/??")
//val path_month = new Path("parquet_backfill/iomgmt/*/????/??")
//val path_month = new Path("parquet_backfill/openx/*/????/??")
//delete_months(path_month)
delete_years(path_year)

