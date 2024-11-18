import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path,FileChecksum}
import scala.collection.mutable.HashMap

val fs = FileSystem.get(new Configuration())

def delete_dirs(path:Path):Unit={
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
    

//val path2 = new Path("parquet_backfill/cse/GPDB/*/????/??")
//val path2 = new Path("parquet_backfill/fcapi/account/????/??")
//val path2 = new Path("parquet_backfill/iomgmt/*/????/??")
val path2 = new Path("parquet_backfill/openx/*/????/??")
delete_dirs(path2)

