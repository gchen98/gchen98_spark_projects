import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path,FileChecksum}

:load utilities.scala
:load schemas_ccr.scala

val newdir = "parquet_backfill/ccr"
def convert(fs:FileSystem,file:Path):Unit={
  val filename:String = file.toString
  val tokens = filename.split("/")
  val table = tokens(tokens.length-1).replace(".tsv","")
  val day = tokens(tokens.length-2)
  val month = tokens(tokens.length-3)
  val year = tokens(tokens.length-4)
  var newbasefile:String = newdir+"/"+table+"/"+year+"/"+month+"/"+day
  fs.delete(new Path(newbasefile),true)
  println("Converting "+filename+" to "+newbasefile)
  if (table.equals("util_stats_by_manufacturer")){
    val df = convert_util_stats_by_manufacturer(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("linecards")){
    val df = convert_linecards(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("lu_alias_context")){
    val df = convert_lu_alias_context(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("reference_design")){
    val df = convert_reference_design(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("product_family")){
    val df = convert_product_family(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("lu_pb_free")){
    val df = convert_lu_pb_free(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("taxonomy_paths")){
    val df = convert_taxonomy_paths(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("taxonomy")){
    val df = convert_taxonomy(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("company")){
    val df = convert_company(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("rd_bom_line")){
    val df = convert_rd_bom_line(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("display_property_value")){
    val df = convert_display_property_value(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("util_job_log")){
    val df = convert_util_job_log(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("mfr_package")){
    val df = convert_mfr_package(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("alias")){
    val df = convert_alias(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("part_alt_codes")){
    val df = convert_part_alt_codes(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("display_property")){
    val df = convert_display_property(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("lu_rd_type")){
    val df = convert_lu_rd_type(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("lu_rohs")){
    val df = convert_lu_rohs(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("lu_language")){
    val df = convert_lu_language(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("design_tool_bak")){
    val df = convert_design_tool_bak(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("util_doc_uids_to_check")){
    val df = convert_util_doc_uids_to_check(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("lu_relationship_context")){
    val df = convert_lu_relationship_context(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("lu_company_type")){
    val df = convert_lu_company_type(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("lu_product_lifecycle")){
    val df = convert_lu_product_lifecycle(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("part_sep")){
    val df = convert_part_sep(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("util_ccr_manager_users")){
    val df = convert_util_ccr_manager_users(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("util_attribute_value_map")){
    val df = convert_util_attribute_value_map(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("part_resistor")){
    val df = convert_part_resistor(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("design_tool")){
    val df = convert_design_tool(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("util_company_product_area")){
    val df = convert_util_company_product_area(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("lu_relationships_allowed")){
    val df = convert_lu_relationships_allowed(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("lu_url_status")){
    val df = convert_lu_url_status(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("util_user")){
    val df = convert_util_user(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("sep")){
    val df = convert_sep(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("dist_mfr_import")){
    val df = convert_dist_mfr_import(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("document")){
    val df = convert_document(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("util_ccr_manager_history")){
    val df = convert_util_ccr_manager_history(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("lu_doc_update_reason")){
    val df = convert_lu_doc_update_reason(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("util_datasheet_regex")){
    val df = convert_util_datasheet_regex(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("lu_alias_visibility")){
    val df = convert_lu_alias_visibility(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("lu_content_status")){
    val df = convert_lu_content_status(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("part")){
    val df = convert_part(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("part_overwrites")){
    val df = convert_part_overwrites(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("part_main")){
    val df = convert_part_main(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("sep_buy_urls")){
    val df = convert_sep_buy_urls(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("evaluation_kit")){
    val df = convert_evaluation_kit(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("lu_file_format")){
    val df = convert_lu_file_format(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("lu_opn_lifecycle")){
    val df = convert_lu_opn_lifecycle(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("lu_rd_component_type")){
    val df = convert_lu_rd_component_type(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("lu_content_type")){
    val df = convert_lu_content_type(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("part_bom2buy")){
    val df = convert_part_bom2buy(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("lu_company_status")){
    val df = convert_lu_company_status(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("education")){
    val df = convert_education(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("lu_product_areas")){
    val df = convert_lu_product_areas(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("rd_bom")){
    val df = convert_rd_bom(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("util_category_map")){
    val df = convert_util_category_map(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("part_ti")){
    val df = convert_part_ti(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("lu_country")){
    val df = convert_lu_country(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("image")){
    val df = convert_image(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("lu_image_file_format")){
    val df = convert_lu_image_file_format(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("relationship")){
    val df = convert_relationship(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("util_manufacturer_map")){
    val df = convert_util_manufacturer_map(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("part_capacitor")){
    val df = convert_part_capacitor(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else{
    return
  }
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
      if(spark.read.format("text").load(file.toString).count>0L){
        convert(fs,file)
      }
    }
  } 
}

val path = new Path("/ccr/????/??/??/*.tsv")
main(path)
