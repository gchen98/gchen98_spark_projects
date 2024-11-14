case class class_util_stats_by_manufacturer(mfr_uid:Long,date:String,opn:Integer,pf:Integer,ds:Integer,an:Integer,doc:Integer,rd:Integer,dt:Integer,opn_ds_dir:Integer,opn_ds_ihs:Integer,pf_ds_dir:Integer,opn_url:Integer,pf_url:Integer,opn_pf:Integer,opn_sample:Integer,batch:String)
def convert_util_stats_by_manufacturer(inpath:String):Dataset[class_util_stats_by_manufacturer]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<17) fields = oldfields.padTo(17,null)
    class_util_stats_by_manufacturer(str2long(fields(0)),checknull(fields(1)),str2int(fields(2)),str2int(fields(3)),str2int(fields(4)),str2int(fields(5)),str2int(fields(6)),str2int(fields(7)),str2int(fields(8)),str2int(fields(9)),str2int(fields(10)),str2int(fields(11)),str2int(fields(12)),str2int(fields(13)),str2int(fields(14)),str2int(fields(15)),checknull(fields(16)))})
  df
}
case class class_linecards(distributor_uid:Long,manufacturer_uid:Long,created_on:String,modified_on:String)
def convert_linecards(inpath:String):Dataset[class_linecards]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<4) fields = oldfields.padTo(4,null)
    class_linecards(str2long(fields(0)),str2long(fields(1)),checknull(fields(2)),checknull(fields(3)))})
  df
}
case class class_lu_alias_context(code:String,description:String)
def convert_lu_alias_context(inpath:String):Dataset[class_lu_alias_context]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<2) fields = oldfields.padTo(2,null)
    class_lu_alias_context(checknull(fields(0)),checknull(fields(1)))})
  df
}
case class class_reference_design(uid:Long,language_code:String,user_id:Long,source_content_uid:String,category_id:Long,owner_id:Long,name:String,description:String,keywords:String,source_url:String,source_url_status_code:String,source_url_status_check_date:String,link_tags:String,status_code:String,created_on:String,modified_on:String,internal_notes:String,orderable_part_number:String,payload:String,rd_categories:String,owner_rd_type:String)
def convert_reference_design(inpath:String):Dataset[class_reference_design]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<21) fields = oldfields.padTo(21,null)
    class_reference_design(str2long(fields(0)),checknull(fields(1)),str2long(fields(2)),checknull(fields(3)),str2long(fields(4)),str2long(fields(5)),checknull(fields(6)),checknull(fields(7)),checknull(fields(8)),checknull(fields(9)),checknull(fields(10)),checknull(fields(11)),checknull(fields(12)),checknull(fields(13)),checknull(fields(14)),checknull(fields(15)),checknull(fields(16)),checknull(fields(17)),checknull(fields(18)),checknull(fields(19)),checknull(fields(20)))})
  df
}
case class class_product_family(uid:Long,language_code:String,user_id:Long,source_content_uid:String,category_id:Long,owner_id:Long,name:String,description:String,keywords:String,source_url:String,source_url_status_code:String,source_url_status_check_date:String,link_tags:String,status_code:String,created_on:String,modified_on:String,internal_notes:String,brand_name:String,features:String,parametrics:String,series_designators:String,product_lifecycle_code:String,current_datasheet_url:String,part_category_id:Long,applications:String,generic_base_number:String)
def convert_product_family(inpath:String):Dataset[class_product_family]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<26) fields = oldfields.padTo(26,null)
    class_product_family(str2long(fields(0)),checknull(fields(1)),str2long(fields(2)),checknull(fields(3)),str2long(fields(4)),str2long(fields(5)),checknull(fields(6)),checknull(fields(7)),checknull(fields(8)),checknull(fields(9)),checknull(fields(10)),checknull(fields(11)),checknull(fields(12)),checknull(fields(13)),checknull(fields(14)),checknull(fields(15)),checknull(fields(16)),checknull(fields(17)),checknull(fields(18)),checknull(fields(19)),checknull(fields(20)),checknull(fields(21)),checknull(fields(22)),str2long(fields(23)),checknull(fields(24)),checknull(fields(25)))})
  df
}
case class class_lu_pb_free(code:String,description:String)
def convert_lu_pb_free(inpath:String):Dataset[class_lu_pb_free]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<2) fields = oldfields.padTo(2,null)
    class_lu_pb_free(checknull(fields(0)),checknull(fields(1)))})
  df
}
case class class_taxonomy_paths(category_id:Long,category_path:String)
def convert_taxonomy_paths(inpath:String):Dataset[class_taxonomy_paths]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<2) fields = oldfields.padTo(2,null)
    class_taxonomy_paths(str2long(fields(0)),checknull(fields(1)))})
  df
}
case class class_taxonomy(uid:Long,owner_id:Long,parent_id:Long,name:String,full_name:String,description:String,keywords:String,visible:Boolean,is_virtual:Boolean,table_name:String,created_on:String,modified_on:String,internal_notes:String,status_code:String)
def convert_taxonomy(inpath:String):Dataset[class_taxonomy]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<14) fields = oldfields.padTo(14,null)
    class_taxonomy(str2long(fields(0)),str2long(fields(1)),str2long(fields(2)),checknull(fields(3)),checknull(fields(4)),checknull(fields(5)),checknull(fields(6)),str2bool(fields(7)),str2bool(fields(8)),checknull(fields(9)),checknull(fields(10)),checknull(fields(11)),checknull(fields(12)),checknull(fields(13)))})
  df
}
case class class_company(uid:Long,language_code:String,user_id:Long,source_content_uid:String,category_id:Long,owner_id:Long,name:String,description:String,keywords:String,source_url:String,source_url_status_code:String,source_url_status_check_date:String,link_tags:String,status_code:String,created_on:String,modified_on:String,internal_notes:String,company_status_code:String,company_type_code:String,parent_company_id:Long,address_1:String,address_2:String,address_3:String,city:String,state_province:String,postal_zip:String,country_code:String,main_phone_number:String,toll_free_phone_number:String,fax_number:String,email_address_1:String,email_address_2:String,feed_enabled:Boolean,products:String)
def convert_company(inpath:String):Dataset[class_company]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<34) fields = oldfields.padTo(34,null)
    class_company(str2long(fields(0)),checknull(fields(1)),str2long(fields(2)),checknull(fields(3)),str2long(fields(4)),str2long(fields(5)),checknull(fields(6)),checknull(fields(7)),checknull(fields(8)),checknull(fields(9)),checknull(fields(10)),checknull(fields(11)),checknull(fields(12)),checknull(fields(13)),checknull(fields(14)),checknull(fields(15)),checknull(fields(16)),checknull(fields(17)),checknull(fields(18)),str2long(fields(19)),checknull(fields(20)),checknull(fields(21)),checknull(fields(22)),checknull(fields(23)),checknull(fields(24)),checknull(fields(25)),checknull(fields(26)),checknull(fields(27)),checknull(fields(28)),checknull(fields(29)),checknull(fields(30)),checknull(fields(31)),str2bool(fields(32)),checknull(fields(33)))})
  df
}
case class class_rd_bom_line(rd_bom_uid:Long,item_no:Integer,qty:Integer,reference_designators:String,description:String,mfr_name:String,mfr_part_no:String,relevance:Integer)
def convert_rd_bom_line(inpath:String):Dataset[class_rd_bom_line]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<8) fields = oldfields.padTo(8,null)
    class_rd_bom_line(str2long(fields(0)),str2int(fields(1)),str2int(fields(2)),checknull(fields(3)),checknull(fields(4)),checknull(fields(5)),checknull(fields(6)),str2int(fields(7)))})
  df
}
case class class_display_property_value(part_supplier_uid:Integer,display_property_uid:Integer,value:String,created_on:String,modified_on:String)
def convert_display_property_value(inpath:String):Dataset[class_display_property_value]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<5) fields = oldfields.padTo(5,null)
    class_display_property_value(str2int(fields(0)),str2int(fields(1)),checknull(fields(2)),checknull(fields(3)),checknull(fields(4)))})
  df
}
case class class_util_job_log(job_name:String,job_id:String,job_run_date_time:String,summary:String,automation:Boolean)
def convert_util_job_log(inpath:String):Dataset[class_util_job_log]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<5) fields = oldfields.padTo(5,null)
    class_util_job_log(checknull(fields(0)),checknull(fields(1)),checknull(fields(2)),checknull(fields(3)),str2bool(fields(4)))})
  df
}
case class class_mfr_package(owner_id:Long,package_code:String,part_number_indicator:String,generic_package_code:String,package_description:String,pin_count:Integer,pitch_mm:Float,max_width_mm:Float,max_length_mm:Float,max_height_mm:Float,industry_package:String,jedec_package:String,eia_package:String,pro_electron_package:String,drawing_url:String)
def convert_mfr_package(inpath:String):Dataset[class_mfr_package]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<15) fields = oldfields.padTo(15,null)
    class_mfr_package(str2long(fields(0)),checknull(fields(1)),checknull(fields(2)),checknull(fields(3)),checknull(fields(4)),str2int(fields(5)),str2float(fields(6)),str2float(fields(7)),str2float(fields(8)),str2float(fields(9)),checknull(fields(10)),checknull(fields(11)),checknull(fields(12)),checknull(fields(13)),checknull(fields(14)))})
  df
}
case class class_alias(content_id:Long,language_code:String,value:String,context_code:String,visibility_code:String,created_on:String,modified_on:String)
def convert_alias(inpath:String):Dataset[class_alias]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<7) fields = oldfields.padTo(7,null)
    class_alias(str2long(fields(0)),checknull(fields(1)),checknull(fields(2)),checknull(fields(3)),checknull(fields(4)),checknull(fields(5)),checknull(fields(6)))})
  df
}
case class class_part_alt_codes(owner_id:Long,name:String,sf_direct_alt_group:String,sf_similar_alt_group:String,description:String)
def convert_part_alt_codes(inpath:String):Dataset[class_part_alt_codes]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<5) fields = oldfields.padTo(5,null)
    class_part_alt_codes(str2long(fields(0)),checknull(fields(1)),checknull(fields(2)),checknull(fields(3)),checknull(fields(4)))})
  df
}
case class class_display_property(uid:Integer,name:String,java_class:String,created_on:String,modified_on:String,ordinal:Integer)
def convert_display_property(inpath:String):Dataset[class_display_property]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<6) fields = oldfields.padTo(6,null)
    class_display_property(str2int(fields(0)),checknull(fields(1)),checknull(fields(2)),checknull(fields(3)),checknull(fields(4)),str2int(fields(5)))})
  df
}
case class class_lu_rd_type(code:String,description:String)
def convert_lu_rd_type(inpath:String):Dataset[class_lu_rd_type]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<2) fields = oldfields.padTo(2,null)
    class_lu_rd_type(checknull(fields(0)),checknull(fields(1)))})
  df
}
case class class_lu_rohs(code:String,description:String)
def convert_lu_rohs(inpath:String):Dataset[class_lu_rohs]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<2) fields = oldfields.padTo(2,null)
    class_lu_rohs(checknull(fields(0)),checknull(fields(1)))})
  df
}
case class class_lu_language(code:String,description:String,focus:Boolean)
def convert_lu_language(inpath:String):Dataset[class_lu_language]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<3) fields = oldfields.padTo(3,null)
    class_lu_language(checknull(fields(0)),checknull(fields(1)),str2bool(fields(2)))})
  df
}
case class class_design_tool_bak(uid:Long,language_code:String,user_id:Long,source_content_uid:String,category_id:Long,owner_id:Long,name:String,description:String,keywords:String,source_url:String,source_url_status_code:String,source_url_status_check_date:String,link_tags:String,status_code:String,created_on:String,modified_on:String,internal_notes:String,orderable_part_number:String,publication_date:String,source_file_url:String,source_file_url_status_code:String,source_file_url_check_date:String,source_file_extension:String,source_file_size:Long,source_file_format:String,source_file_format_version:String,reference_design_component_type:String,source_file_name:String)
def convert_design_tool_bak(inpath:String):Dataset[class_design_tool_bak]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<28) fields = oldfields.padTo(28,null)
    class_design_tool_bak(str2long(fields(0)),checknull(fields(1)),str2long(fields(2)),checknull(fields(3)),str2long(fields(4)),str2long(fields(5)),checknull(fields(6)),checknull(fields(7)),checknull(fields(8)),checknull(fields(9)),checknull(fields(10)),checknull(fields(11)),checknull(fields(12)),checknull(fields(13)),checknull(fields(14)),checknull(fields(15)),checknull(fields(16)),checknull(fields(17)),checknull(fields(18)),checknull(fields(19)),checknull(fields(20)),checknull(fields(21)),checknull(fields(22)),str2long(fields(23)),checknull(fields(24)),checknull(fields(25)),checknull(fields(26)),checknull(fields(27)))})
  df
}
case class class_util_doc_uids_to_check(uid:Long)
def convert_util_doc_uids_to_check(inpath:String):Dataset[class_util_doc_uids_to_check]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<1) fields = oldfields.padTo(1,null)
    class_util_doc_uids_to_check(str2long(fields(0)))})
  df
}
case class class_lu_relationship_context(code:String,description:String)
def convert_lu_relationship_context(inpath:String):Dataset[class_lu_relationship_context]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<2) fields = oldfields.padTo(2,null)
    class_lu_relationship_context(checknull(fields(0)),checknull(fields(1)))})
  df
}
case class class_lu_company_type(code:String,description:String)
def convert_lu_company_type(inpath:String):Dataset[class_lu_company_type]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<2) fields = oldfields.padTo(2,null)
    class_lu_company_type(checknull(fields(0)),checknull(fields(1)))})
  df
}
case class class_lu_product_lifecycle(code:String,description:String)
def convert_lu_product_lifecycle(inpath:String):Dataset[class_lu_product_lifecycle]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<2) fields = oldfields.padTo(2,null)
    class_lu_product_lifecycle(checknull(fields(0)),checknull(fields(1)))})
  df
}
case class class_part_sep(uid:Long,buy_now_url:String)
def convert_part_sep(inpath:String):Dataset[class_part_sep]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<2) fields = oldfields.padTo(2,null)
    class_part_sep(str2long(fields(0)),checknull(fields(1)))})
  df
}
case class class_util_ccr_manager_users(uid:String,password:String,email:String,permissions:String)
def convert_util_ccr_manager_users(inpath:String):Dataset[class_util_ccr_manager_users]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<4) fields = oldfields.padTo(4,null)
    class_util_ccr_manager_users(checknull(fields(0)),checknull(fields(1)),checknull(fields(2)),checknull(fields(3)))})
  df
}
case class class_util_attribute_value_map(source:String,attribute:String,source_value:String,sf_value:String)
def convert_util_attribute_value_map(inpath:String):Dataset[class_util_attribute_value_map]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<4) fields = oldfields.padTo(4,null)
    class_util_attribute_value_map(checknull(fields(0)),checknull(fields(1)),checknull(fields(2)),checknull(fields(3)))})
  df
}
case class class_part_resistor(uid:Long,language_code:String,user_id:Long,source_content_uid:String,category_id:Long,owner_id:Long,name:String,description:String,keywords:String,source_url:String,source_url_status_code:String,source_url_status_check_date:String,link_tags:String,status_code:String,created_on:String,modified_on:String,internal_notes:String,brand_name:String,pb_free_code:String,rohs_code:String,parametrics:String,part_lifecycle_code:String,parent_family_id:Long,current_datasheet_url:String,source_sample_url:String,ihs_functional_group:String,ihs_fff_group:String,ihs_manufacturer:String,ihs_datasheet_url:String,ihs_class:String,ihs_category:String,ihs_object_id:Long,base_number:String,sf_description:String,detailed_description:String,mil_spec:Boolean,package_code:String,ihs_package_description:String,locked:Boolean,pin_count:String,clean_description:String,mfr_package_code:String,jedec_package_code:String,disti_descriptions:String,image_name:String,rohs_code_china:String,reach_compliance_code:String,ihs_direct_alt_group:String,ihs_similar_alt_group:String,automotive_approvals:String,sf_category:String,sf_class:String,lifecycle_last_update:String,sf_alternates_direct_group:String,sf_alternates_similar_group:String,country_of_origin:String,eccn_code:String,hsn_code:String,schedule_b_code:String,mfr_category:String,factory_lead_time:Long,date_of_intro:String)
def convert_part_resistor(inpath:String):Dataset[class_part_resistor]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<62) fields = oldfields.padTo(62,null)
    class_part_resistor(str2long(fields(0)),checknull(fields(1)),str2long(fields(2)),checknull(fields(3)),str2long(fields(4)),str2long(fields(5)),checknull(fields(6)),checknull(fields(7)),checknull(fields(8)),checknull(fields(9)),checknull(fields(10)),checknull(fields(11)),checknull(fields(12)),checknull(fields(13)),checknull(fields(14)),checknull(fields(15)),checknull(fields(16)),checknull(fields(17)),checknull(fields(18)),checknull(fields(19)),checknull(fields(20)),checknull(fields(21)),str2long(fields(22)),checknull(fields(23)),checknull(fields(24)),checknull(fields(25)),checknull(fields(26)),checknull(fields(27)),checknull(fields(28)),checknull(fields(29)),checknull(fields(30)),str2long(fields(31)),checknull(fields(32)),checknull(fields(33)),checknull(fields(34)),str2bool(fields(35)),checknull(fields(36)),checknull(fields(37)),str2bool(fields(38)),checknull(fields(39)),checknull(fields(40)),checknull(fields(41)),checknull(fields(42)),checknull(fields(43)),checknull(fields(44)),checknull(fields(45)),checknull(fields(46)),checknull(fields(47)),checknull(fields(48)),checknull(fields(49)),checknull(fields(50)),checknull(fields(51)),checknull(fields(52)),checknull(fields(53)),checknull(fields(54)),checknull(fields(55)),checknull(fields(56)),checknull(fields(57)),checknull(fields(58)),checknull(fields(59)),str2long(fields(60)),checknull(fields(61)))})
  df
}
case class class_design_tool(uid:Long,language_code:String,user_id:Long,source_content_uid:String,category_id:Long,owner_id:Long,name:String,description:String,keywords:String,source_url:String,source_url_status_code:String,source_url_status_check_date:String,link_tags:String,status_code:String,created_on:String,modified_on:String,internal_notes:String,orderable_part_number:String,publication_date:String,source_file_url:String,source_file_url_status_code:String,source_file_url_check_date:String,source_file_extension:String,source_file_size:Long,source_file_format:String,source_file_format_version:String,reference_design_component_type:String,source_file_name:String)
def convert_design_tool(inpath:String):Dataset[class_design_tool]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<28) fields = oldfields.padTo(28,null)
    class_design_tool(str2long(fields(0)),checknull(fields(1)),str2long(fields(2)),checknull(fields(3)),str2long(fields(4)),str2long(fields(5)),checknull(fields(6)),checknull(fields(7)),checknull(fields(8)),checknull(fields(9)),checknull(fields(10)),checknull(fields(11)),checknull(fields(12)),checknull(fields(13)),checknull(fields(14)),checknull(fields(15)),checknull(fields(16)),checknull(fields(17)),checknull(fields(18)),checknull(fields(19)),checknull(fields(20)),checknull(fields(21)),checknull(fields(22)),str2long(fields(23)),checknull(fields(24)),checknull(fields(25)),checknull(fields(26)),checknull(fields(27)))})
  df
}
case class class_util_company_product_area(company_uid:Long,product_area_id:Long)
def convert_util_company_product_area(inpath:String):Dataset[class_util_company_product_area]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<2) fields = oldfields.padTo(2,null)
    class_util_company_product_area(str2long(fields(0)),str2long(fields(1)))})
  df
}
case class class_lu_relationships_allowed(from_type:String,to_type:String)
def convert_lu_relationships_allowed(inpath:String):Dataset[class_lu_relationships_allowed]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<2) fields = oldfields.padTo(2,null)
    class_lu_relationships_allowed(checknull(fields(0)),checknull(fields(1)))})
  df
}
case class class_lu_url_status(code:String,description:String)
def convert_lu_url_status(inpath:String):Dataset[class_lu_url_status]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<2) fields = oldfields.padTo(2,null)
    class_lu_url_status(checknull(fields(0)),checknull(fields(1)))})
  df
}
case class class_util_user(uid:Integer,name:String)
def convert_util_user(inpath:String):Dataset[class_util_user]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<2) fields = oldfields.padTo(2,null)
    class_util_user(str2int(fields(0)),checknull(fields(1)))})
  df
}
case class class_sep(part_nbr:String,part_uid:Long,description:String,mfr_name:String,mfr_uid:Long,sep_source_url:String,sep_sample_url:String,advertiser_uid:Long,status_code:String,rohs_code:String,category_id:Long)
def convert_sep(inpath:String):Dataset[class_sep]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<11) fields = oldfields.padTo(11,null)
    class_sep(checknull(fields(0)),str2long(fields(1)),checknull(fields(2)),checknull(fields(3)),str2long(fields(4)),checknull(fields(5)),checknull(fields(6)),str2long(fields(7)),checknull(fields(8)),checknull(fields(9)),str2long(fields(10)))})
  df
}
case class class_dist_mfr_import(distributor_uid:String,manufacturer_uid:Long,alias:String,name:String)
def convert_dist_mfr_import(inpath:String):Dataset[class_dist_mfr_import]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<4) fields = oldfields.padTo(4,null)
    class_dist_mfr_import(checknull(fields(0)),str2long(fields(1)),checknull(fields(2)),checknull(fields(3)))})
  df
}
case class class_document(uid:Long,language_code:String,user_id:Long,source_content_uid:String,category_id:Long,owner_id:Long,name:String,description:String,keywords:String,source_url:String,source_url_status_code:String,source_url_status_check_date:String,link_tags:String,status_code:String,created_on:String,modified_on:String,internal_notes:String,authors:String,revision:String,publication_date:String,owner_document_uid:String,source_file_url:String,source_file_url_status_code:String,source_file_url_check_date:String,source_file_format_code:String,source_file_size:Long,uc_md5:String,update_reason_code:String,uc_source_file_url_final:String,uc_internal_file_url:String,uc_redirect_count:Integer,uc_state:String)
def convert_document(inpath:String):Dataset[class_document]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<32) fields = oldfields.padTo(32,null)
    class_document(str2long(fields(0)),checknull(fields(1)),str2long(fields(2)),checknull(fields(3)),str2long(fields(4)),str2long(fields(5)),checknull(fields(6)),checknull(fields(7)),checknull(fields(8)),checknull(fields(9)),checknull(fields(10)),checknull(fields(11)),checknull(fields(12)),checknull(fields(13)),checknull(fields(14)),checknull(fields(15)),checknull(fields(16)),checknull(fields(17)),checknull(fields(18)),checknull(fields(19)),checknull(fields(20)),checknull(fields(21)),checknull(fields(22)),checknull(fields(23)),checknull(fields(24)),str2long(fields(25)),checknull(fields(26)),checknull(fields(27)),checknull(fields(28)),checknull(fields(29)),str2int(fields(30)),checknull(fields(31)))})
  df
}
case class class_util_ccr_manager_history(object_id:Long,object_type:String,changes_made:String,created_on:String,user_id:String,object_name:String)
def convert_util_ccr_manager_history(inpath:String):Dataset[class_util_ccr_manager_history]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<6) fields = oldfields.padTo(6,null)
    class_util_ccr_manager_history(str2long(fields(0)),checknull(fields(1)),checknull(fields(2)),checknull(fields(3)),checknull(fields(4)),checknull(fields(5)))})
  df
}
case class class_lu_doc_update_reason(code:String,description:String)
def convert_lu_doc_update_reason(inpath:String):Dataset[class_lu_doc_update_reason]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<2) fields = oldfields.padTo(2,null)
    class_lu_doc_update_reason(checknull(fields(0)),checknull(fields(1)))})
  df
}
case class class_util_datasheet_regex(owner_id:Long,datasheet_url:String,regex:String)
def convert_util_datasheet_regex(inpath:String):Dataset[class_util_datasheet_regex]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<3) fields = oldfields.padTo(3,null)
    class_util_datasheet_regex(str2long(fields(0)),checknull(fields(1)),checknull(fields(2)))})
  df
}
case class class_lu_alias_visibility(code:String,description:String)
def convert_lu_alias_visibility(inpath:String):Dataset[class_lu_alias_visibility]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<2) fields = oldfields.padTo(2,null)
    class_lu_alias_visibility(checknull(fields(0)),checknull(fields(1)))})
  df
}
case class class_lu_content_status(code:String,description:String)
def convert_lu_content_status(inpath:String):Dataset[class_lu_content_status]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<2) fields = oldfields.padTo(2,null)
    class_lu_content_status(checknull(fields(0)),checknull(fields(1)))})
  df
}
case class class_part(uid:Long,language_code:String,user_id:Long,source_content_uid:String,category_id:Long,owner_id:Long,name:String,description:String,keywords:String,source_url:String,source_url_status_code:String,source_url_status_check_date:String,link_tags:String,status_code:String,created_on:String,modified_on:String,internal_notes:String,brand_name:String,pb_free_code:String,rohs_code:String,parametrics:String,part_lifecycle_code:String,parent_family_id:Long,current_datasheet_url:String,source_sample_url:String,ihs_functional_group:String,ihs_fff_group:String,ihs_manufacturer:String,ihs_datasheet_url:String,ihs_class:String,ihs_category:String,ihs_object_id:Long,base_number:String,sf_description:String,detailed_description:String,mil_spec:Boolean,package_code:String,ihs_package_description:String,locked:Boolean,pin_count:String,clean_description:String,mfr_package_code:String,jedec_package_code:String,disti_descriptions:String,image_name:String,rohs_code_china:String,reach_compliance_code:String,ihs_direct_alt_group:String,ihs_similar_alt_group:String,automotive_approvals:String,sf_category:String,sf_class:String,lifecycle_last_update:String,sf_alternates_direct_group:String,sf_alternates_similar_group:String,country_of_origin:String,eccn_code:String,hsn_code:String,schedule_b_code:String,mfr_category:String,factory_lead_time:Long,date_of_intro:String)
def convert_part(inpath:String):Dataset[class_part]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<62) fields = oldfields.padTo(62,null)
    class_part(str2long(fields(0)),checknull(fields(1)),str2long(fields(2)),checknull(fields(3)),str2long(fields(4)),str2long(fields(5)),checknull(fields(6)),checknull(fields(7)),checknull(fields(8)),checknull(fields(9)),checknull(fields(10)),checknull(fields(11)),checknull(fields(12)),checknull(fields(13)),checknull(fields(14)),checknull(fields(15)),checknull(fields(16)),checknull(fields(17)),checknull(fields(18)),checknull(fields(19)),checknull(fields(20)),checknull(fields(21)),str2long(fields(22)),checknull(fields(23)),checknull(fields(24)),checknull(fields(25)),checknull(fields(26)),checknull(fields(27)),checknull(fields(28)),checknull(fields(29)),checknull(fields(30)),str2long(fields(31)),checknull(fields(32)),checknull(fields(33)),checknull(fields(34)),str2bool(fields(35)),checknull(fields(36)),checknull(fields(37)),str2bool(fields(38)),checknull(fields(39)),checknull(fields(40)),checknull(fields(41)),checknull(fields(42)),checknull(fields(43)),checknull(fields(44)),checknull(fields(45)),checknull(fields(46)),checknull(fields(47)),checknull(fields(48)),checknull(fields(49)),checknull(fields(50)),checknull(fields(51)),checknull(fields(52)),checknull(fields(53)),checknull(fields(54)),checknull(fields(55)),checknull(fields(56)),checknull(fields(57)),checknull(fields(58)),checknull(fields(59)),str2long(fields(60)),checknull(fields(61)))})
  df
}
case class class_part_overwrites(uid:Long,language_code:String,user_id:Long,source_content_uid:String,category_id:Long,owner_id:Long,name:String,description:String,keywords:String,source_url:String,source_url_status_code:String,source_url_status_check_date:String,link_tags:String,status_code:String,created_on:String,modified_on:String,internal_notes:String,brand_name:String,pb_free_code:String,rohs_code:String,parametrics:String,part_lifecycle_code:String,parent_family_id:Long,current_datasheet_url:String,source_sample_url:String,ihs_functional_group:String,ihs_fff_group:String,ihs_manufacturer:String,ihs_datasheet_url:String,ihs_class:String,ihs_category:String,ihs_object_id:Long,base_number:String,sf_description:String,detailed_description:String,mil_spec:Boolean,package_code:String,ihs_package_description:String,locked:Boolean,pin_count:String,clean_description:String,mfr_package_code:String,jedec_package_code:String,disti_descriptions:String,image_name:String,rohs_code_china:String,reach_compliance_code:String,ihs_direct_alt_group:String,ihs_similar_alt_group:String)
def convert_part_overwrites(inpath:String):Dataset[class_part_overwrites]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<49) fields = oldfields.padTo(49,null)
    class_part_overwrites(str2long(fields(0)),checknull(fields(1)),str2long(fields(2)),checknull(fields(3)),str2long(fields(4)),str2long(fields(5)),checknull(fields(6)),checknull(fields(7)),checknull(fields(8)),checknull(fields(9)),checknull(fields(10)),checknull(fields(11)),checknull(fields(12)),checknull(fields(13)),checknull(fields(14)),checknull(fields(15)),checknull(fields(16)),checknull(fields(17)),checknull(fields(18)),checknull(fields(19)),checknull(fields(20)),checknull(fields(21)),str2long(fields(22)),checknull(fields(23)),checknull(fields(24)),checknull(fields(25)),checknull(fields(26)),checknull(fields(27)),checknull(fields(28)),checknull(fields(29)),checknull(fields(30)),str2long(fields(31)),checknull(fields(32)),checknull(fields(33)),checknull(fields(34)),str2bool(fields(35)),checknull(fields(36)),checknull(fields(37)),str2bool(fields(38)),checknull(fields(39)),checknull(fields(40)),checknull(fields(41)),checknull(fields(42)),checknull(fields(43)),checknull(fields(44)),checknull(fields(45)),checknull(fields(46)),checknull(fields(47)),checknull(fields(48)))})
  df
}
case class class_part_main(uid:Long,language_code:String,user_id:Long,source_content_uid:String,category_id:Long,owner_id:Long,name:String,description:String,keywords:String,source_url:String,source_url_status_code:String,source_url_status_check_date:String,link_tags:String,status_code:String,created_on:String,modified_on:String,internal_notes:String,brand_name:String,pb_free_code:String,rohs_code:String,parametrics:String,part_lifecycle_code:String,parent_family_id:Long,current_datasheet_url:String,source_sample_url:String,ihs_functional_group:String,ihs_fff_group:String,ihs_manufacturer:String,ihs_datasheet_url:String,ihs_class:String,ihs_category:String,ihs_object_id:Long,base_number:String,sf_description:String,detailed_description:String,mil_spec:Boolean,package_code:String,ihs_package_description:String,locked:Boolean,pin_count:String,clean_description:String,mfr_package_code:String,jedec_package_code:String,disti_descriptions:String,image_name:String,rohs_code_china:String,reach_compliance_code:String,ihs_direct_alt_group:String,ihs_similar_alt_group:String,automotive_approvals:String,sf_category:String,sf_class:String,lifecycle_last_update:String,sf_alternates_direct_group:String,sf_alternates_similar_group:String,country_of_origin:String,eccn_code:String,hsn_code:String,schedule_b_code:String,mfr_category:String,factory_lead_time:Long,date_of_intro:String)
def convert_part_main(inpath:String):Dataset[class_part_main]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<62) fields = oldfields.padTo(62,null)
    class_part_main(str2long(fields(0)),checknull(fields(1)),str2long(fields(2)),checknull(fields(3)),str2long(fields(4)),str2long(fields(5)),checknull(fields(6)),checknull(fields(7)),checknull(fields(8)),checknull(fields(9)),checknull(fields(10)),checknull(fields(11)),checknull(fields(12)),checknull(fields(13)),checknull(fields(14)),checknull(fields(15)),checknull(fields(16)),checknull(fields(17)),checknull(fields(18)),checknull(fields(19)),checknull(fields(20)),checknull(fields(21)),str2long(fields(22)),checknull(fields(23)),checknull(fields(24)),checknull(fields(25)),checknull(fields(26)),checknull(fields(27)),checknull(fields(28)),checknull(fields(29)),checknull(fields(30)),str2long(fields(31)),checknull(fields(32)),checknull(fields(33)),checknull(fields(34)),str2bool(fields(35)),checknull(fields(36)),checknull(fields(37)),str2bool(fields(38)),checknull(fields(39)),checknull(fields(40)),checknull(fields(41)),checknull(fields(42)),checknull(fields(43)),checknull(fields(44)),checknull(fields(45)),checknull(fields(46)),checknull(fields(47)),checknull(fields(48)),checknull(fields(49)),checknull(fields(50)),checknull(fields(51)),checknull(fields(52)),checknull(fields(53)),checknull(fields(54)),checknull(fields(55)),checknull(fields(56)),checknull(fields(57)),checknull(fields(58)),checknull(fields(59)),str2long(fields(60)),checknull(fields(61)))})
  df
}
case class class_sep_buy_urls(uid:Long,buy_now_url:String)
def convert_sep_buy_urls(inpath:String):Dataset[class_sep_buy_urls]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<2) fields = oldfields.padTo(2,null)
    class_sep_buy_urls(str2long(fields(0)),checknull(fields(1)))})
  df
}
case class class_evaluation_kit(uid:Long,language_code:String,user_id:Long,source_content_uid:String,category_id:Long,owner_id:Long,name:String,description:String,keywords:String,source_url:String,source_url_status_code:String,source_url_status_check_date:String,link_tags:String,status_code:String,created_on:String,modified_on:String,internal_notes:String,orderable_part_number:String,payload:String,ek_categories:String,owner_ek_type:String,owner_status:String)
def convert_evaluation_kit(inpath:String):Dataset[class_evaluation_kit]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<22) fields = oldfields.padTo(22,null)
    class_evaluation_kit(str2long(fields(0)),checknull(fields(1)),str2long(fields(2)),checknull(fields(3)),str2long(fields(4)),str2long(fields(5)),checknull(fields(6)),checknull(fields(7)),checknull(fields(8)),checknull(fields(9)),checknull(fields(10)),checknull(fields(11)),checknull(fields(12)),checknull(fields(13)),checknull(fields(14)),checknull(fields(15)),checknull(fields(16)),checknull(fields(17)),checknull(fields(18)),checknull(fields(19)),checknull(fields(20)),checknull(fields(21)))})
  df
}
case class class_lu_file_format(code:String,description:String)
def convert_lu_file_format(inpath:String):Dataset[class_lu_file_format]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<2) fields = oldfields.padTo(2,null)
    class_lu_file_format(checknull(fields(0)),checknull(fields(1)))})
  df
}
case class class_lu_opn_lifecycle(code:String,description:String)
def convert_lu_opn_lifecycle(inpath:String):Dataset[class_lu_opn_lifecycle]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<2) fields = oldfields.padTo(2,null)
    class_lu_opn_lifecycle(checknull(fields(0)),checknull(fields(1)))})
  df
}
case class class_lu_rd_component_type(code:String,description:String)
def convert_lu_rd_component_type(inpath:String):Dataset[class_lu_rd_component_type]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<2) fields = oldfields.padTo(2,null)
    class_lu_rd_component_type(checknull(fields(0)),checknull(fields(1)))})
  df
}
case class class_lu_content_type(code:String,description:String)
def convert_lu_content_type(inpath:String):Dataset[class_lu_content_type]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<2) fields = oldfields.padTo(2,null)
    class_lu_content_type(checknull(fields(0)),checknull(fields(1)))})
  df
}
case class class_part_bom2buy(owner_id:Long,manufacturer_name:String,part_number:String,description:String)
def convert_part_bom2buy(inpath:String):Dataset[class_part_bom2buy]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<4) fields = oldfields.padTo(4,null)
    class_part_bom2buy(str2long(fields(0)),checknull(fields(1)),checknull(fields(2)),checknull(fields(3)))})
  df
}
case class class_lu_company_status(code:String,description:String)
def convert_lu_company_status(inpath:String):Dataset[class_lu_company_status]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<2) fields = oldfields.padTo(2,null)
    class_lu_company_status(checknull(fields(0)),checknull(fields(1)))})
  df
}
case class class_education(uid:Long,language_code:String,user_id:Long,source_content_uid:String,category_id:Long,owner_id:Long,name:String,description:String,keywords:String,source_url:String,source_url_status_code:String,source_url_status_check_date:String,link_tags:String,status_code:String,created_on:String,modified_on:String,internal_notes:String,run_time:Long,presenters:String,pub_event_date_time:String,registration_required:Boolean,registration_url:String,registration_end_date:String,source_file_url:String,source_file_url_status_code:String,source_file_url_check_date:String,source_file_format_code:String,source_file_size:Long)
def convert_education(inpath:String):Dataset[class_education]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<28) fields = oldfields.padTo(28,null)
    class_education(str2long(fields(0)),checknull(fields(1)),str2long(fields(2)),checknull(fields(3)),str2long(fields(4)),str2long(fields(5)),checknull(fields(6)),checknull(fields(7)),checknull(fields(8)),checknull(fields(9)),checknull(fields(10)),checknull(fields(11)),checknull(fields(12)),checknull(fields(13)),checknull(fields(14)),checknull(fields(15)),checknull(fields(16)),str2long(fields(17)),checknull(fields(18)),checknull(fields(19)),str2bool(fields(20)),checknull(fields(21)),checknull(fields(22)),checknull(fields(23)),checknull(fields(24)),checknull(fields(25)),checknull(fields(26)),str2long(fields(27)))})
  df
}
case class class_lu_product_areas(id:Long,level_1:String,level_2:String)
def convert_lu_product_areas(inpath:String):Dataset[class_lu_product_areas]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<3) fields = oldfields.padTo(3,null)
    class_lu_product_areas(str2long(fields(0)),checknull(fields(1)),checknull(fields(2)))})
  df
}
case class class_rd_bom(uid:Long,owner_id:Long,name:String,description:String,version:String,publication_date:String,source_content_uid:String)
def convert_rd_bom(inpath:String):Dataset[class_rd_bom]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<7) fields = oldfields.padTo(7,null)
    class_rd_bom(str2long(fields(0)),str2long(fields(1)),checknull(fields(2)),checknull(fields(3)),checknull(fields(4)),checknull(fields(5)),checknull(fields(6)))})
  df
}
case class class_util_category_map(source_uid:Long,source_category:String,sf_category_id:Long,created_on:String,modified_on:String)
def convert_util_category_map(inpath:String):Dataset[class_util_category_map]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<5) fields = oldfields.padTo(5,null)
    class_util_category_map(str2long(fields(0)),checknull(fields(1)),str2long(fields(2)),checknull(fields(3)),checknull(fields(4)))})
  df
}
case class class_part_ti(uid:Long,language_code:String,user_id:Long,source_content_uid:String,category_id:Long,owner_id:Long,name:String,description:String,keywords:String,source_url:String,source_url_status_code:String,source_url_status_check_date:String,link_tags:String,status_code:String,created_on:String,modified_on:String,internal_notes:String,brand_name:String,pb_free_code:String,rohs_code:String,parametrics:String,part_lifecycle_code:String,parent_family_id:Long,current_datasheet_url:String,source_sample_url:String,ihs_functional_group:String,ihs_fff_group:String,ihs_manufacturer:String,ihs_datasheet_url:String,ihs_class:String,ihs_category:String,ihs_object_id:Long,base_number:String,sf_description:String,detailed_description:String,mil_spec:Boolean,package_code:String,ihs_package_description:String,locked:Boolean,pin_count:String,clean_description:String,mfr_package_code:String,jedec_package_code:String,disti_descriptions:String,image_name:String,rohs_code_china:String,reach_compliance_code:String,ihs_direct_alt_group:String,ihs_similar_alt_group:String,automotive_approvals:String,sf_category:String,sf_class:String,lifecycle_last_update:String,sf_alternates_direct_group:String,sf_alternates_similar_group:String,country_of_origin:String,eccn_code:String,hsn_code:String,schedule_b_code:String,mfr_category:String)
def convert_part_ti(inpath:String):Dataset[class_part_ti]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<60) fields = oldfields.padTo(60,null)
    class_part_ti(str2long(fields(0)),checknull(fields(1)),str2long(fields(2)),checknull(fields(3)),str2long(fields(4)),str2long(fields(5)),checknull(fields(6)),checknull(fields(7)),checknull(fields(8)),checknull(fields(9)),checknull(fields(10)),checknull(fields(11)),checknull(fields(12)),checknull(fields(13)),checknull(fields(14)),checknull(fields(15)),checknull(fields(16)),checknull(fields(17)),checknull(fields(18)),checknull(fields(19)),checknull(fields(20)),checknull(fields(21)),str2long(fields(22)),checknull(fields(23)),checknull(fields(24)),checknull(fields(25)),checknull(fields(26)),checknull(fields(27)),checknull(fields(28)),checknull(fields(29)),checknull(fields(30)),str2long(fields(31)),checknull(fields(32)),checknull(fields(33)),checknull(fields(34)),str2bool(fields(35)),checknull(fields(36)),checknull(fields(37)),str2bool(fields(38)),checknull(fields(39)),checknull(fields(40)),checknull(fields(41)),checknull(fields(42)),checknull(fields(43)),checknull(fields(44)),checknull(fields(45)),checknull(fields(46)),checknull(fields(47)),checknull(fields(48)),checknull(fields(49)),checknull(fields(50)),checknull(fields(51)),checknull(fields(52)),checknull(fields(53)),checknull(fields(54)),checknull(fields(55)),checknull(fields(56)),checknull(fields(57)),checknull(fields(58)),checknull(fields(59)))})
  df
}
case class class_lu_country(code:String,description:String,focus:Boolean)
def convert_lu_country(inpath:String):Dataset[class_lu_country]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<3) fields = oldfields.padTo(3,null)
    class_lu_country(checknull(fields(0)),checknull(fields(1)),str2bool(fields(2)))})
  df
}
case class class_image(uid:Long,language_code:String,user_id:Long,source_content_uid:String,category_id:Long,owner_id:Long,name:String,description:String,keywords:String,source_url:String,source_url_status_code:String,source_url_status_check_date:String,link_tags:String,status_code:String,created_on:String,modified_on:String,internal_notes:String,image_width:Integer,image_height:Integer,alt_text:String,source_file_url:String,source_file_url_status_code:String,source_file_url_check_date:String,source_file_format_code:String,source_file_size:Long)
def convert_image(inpath:String):Dataset[class_image]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<25) fields = oldfields.padTo(25,null)
    class_image(str2long(fields(0)),checknull(fields(1)),str2long(fields(2)),checknull(fields(3)),str2long(fields(4)),str2long(fields(5)),checknull(fields(6)),checknull(fields(7)),checknull(fields(8)),checknull(fields(9)),checknull(fields(10)),checknull(fields(11)),checknull(fields(12)),checknull(fields(13)),checknull(fields(14)),checknull(fields(15)),checknull(fields(16)),str2int(fields(17)),str2int(fields(18)),checknull(fields(19)),checknull(fields(20)),checknull(fields(21)),checknull(fields(22)),checknull(fields(23)),str2long(fields(24)))})
  df
}
case class class_lu_image_file_format(code:String,description:String)
def convert_lu_image_file_format(inpath:String):Dataset[class_lu_image_file_format]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<2) fields = oldfields.padTo(2,null)
    class_lu_image_file_format(checknull(fields(0)),checknull(fields(1)))})
  df
}
case class class_relationship(from_id:Long,from_content_type_code:String,to_id:Long,to_content_type_code:String,context_code:String,created_on:String,modified_on:String)
def convert_relationship(inpath:String):Dataset[class_relationship]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<7) fields = oldfields.padTo(7,null)
    class_relationship(str2long(fields(0)),checknull(fields(1)),str2long(fields(2)),checknull(fields(3)),checknull(fields(4)),checknull(fields(5)),checknull(fields(6)))})
  df
}
case class class_util_manufacturer_map(source_id:String,source_mfr_id:String,source_mfr_name:String,sfm_id:Long,association_type:Integer,created_on:String,modified_on:String)
def convert_util_manufacturer_map(inpath:String):Dataset[class_util_manufacturer_map]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<7) fields = oldfields.padTo(7,null)
    class_util_manufacturer_map(checknull(fields(0)),checknull(fields(1)),checknull(fields(2)),str2long(fields(3)),str2int(fields(4)),checknull(fields(5)),checknull(fields(6)))})
  df
}
case class class_part_capacitor(uid:Long,language_code:String,user_id:Long,source_content_uid:String,category_id:Long,owner_id:Long,name:String,description:String,keywords:String,source_url:String,source_url_status_code:String,source_url_status_check_date:String,link_tags:String,status_code:String,created_on:String,modified_on:String,internal_notes:String,brand_name:String,pb_free_code:String,rohs_code:String,parametrics:String,part_lifecycle_code:String,parent_family_id:Long,current_datasheet_url:String,source_sample_url:String,ihs_functional_group:String,ihs_fff_group:String,ihs_manufacturer:String,ihs_datasheet_url:String,ihs_class:String,ihs_category:String,ihs_object_id:Long,base_number:String,sf_description:String,detailed_description:String,mil_spec:Boolean,package_code:String,ihs_package_description:String,locked:Boolean,pin_count:String,clean_description:String,mfr_package_code:String,jedec_package_code:String,disti_descriptions:String,image_name:String,rohs_code_china:String,reach_compliance_code:String,ihs_direct_alt_group:String,ihs_similar_alt_group:String,automotive_approvals:String,sf_category:String,sf_class:String,lifecycle_last_update:String,sf_alternates_direct_group:String,sf_alternates_similar_group:String,country_of_origin:String,eccn_code:String,hsn_code:String,schedule_b_code:String,mfr_category:String,factory_lead_time:Long,date_of_intro:String)
def convert_part_capacitor(inpath:String):Dataset[class_part_capacitor]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<62) fields = oldfields.padTo(62,null)
    class_part_capacitor(str2long(fields(0)),checknull(fields(1)),str2long(fields(2)),checknull(fields(3)),str2long(fields(4)),str2long(fields(5)),checknull(fields(6)),checknull(fields(7)),checknull(fields(8)),checknull(fields(9)),checknull(fields(10)),checknull(fields(11)),checknull(fields(12)),checknull(fields(13)),checknull(fields(14)),checknull(fields(15)),checknull(fields(16)),checknull(fields(17)),checknull(fields(18)),checknull(fields(19)),checknull(fields(20)),checknull(fields(21)),str2long(fields(22)),checknull(fields(23)),checknull(fields(24)),checknull(fields(25)),checknull(fields(26)),checknull(fields(27)),checknull(fields(28)),checknull(fields(29)),checknull(fields(30)),str2long(fields(31)),checknull(fields(32)),checknull(fields(33)),checknull(fields(34)),str2bool(fields(35)),checknull(fields(36)),checknull(fields(37)),str2bool(fields(38)),checknull(fields(39)),checknull(fields(40)),checknull(fields(41)),checknull(fields(42)),checknull(fields(43)),checknull(fields(44)),checknull(fields(45)),checknull(fields(46)),checknull(fields(47)),checknull(fields(48)),checknull(fields(49)),checknull(fields(50)),checknull(fields(51)),checknull(fields(52)),checknull(fields(53)),checknull(fields(54)),checknull(fields(55)),checknull(fields(56)),checknull(fields(57)),checknull(fields(58)),checknull(fields(59)),str2long(fields(60)),checknull(fields(61)))})
  df
}
