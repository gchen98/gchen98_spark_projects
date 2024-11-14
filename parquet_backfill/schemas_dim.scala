case class class_action_dim(action_key:Integer,fact_type:String,is_openx:Boolean,is_sf_click_content:Boolean,is_fc_click_disti:Boolean,is_generic:Boolean,action:String,created_on:String,notes:String,is_click:Boolean,is_frameit:Boolean,is_encoded:Boolean,is_using_fc_content:Boolean,is_fc_aggr:Boolean)
def convert_action_dim(inpath:String):Dataset[class_action_dim]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<14) fields = oldfields.padTo(14,null)
    class_action_dim(str2int(fields(0)),checknull(fields(1)),str2bool(fields(2)),str2bool(fields(3)),str2bool(fields(4)),str2bool(fields(5)),checknull(fields(6)),checknull(fields(7)),checknull(fields(8)),str2bool(fields(9)),str2bool(fields(10)),str2bool(fields(11)),str2bool(fields(12)),str2bool(fields(13)))})
  df
}
case class class_banner_dim(banner_key:Integer,version_key:Integer,bannerid:Integer,campaignid:Integer,description:String,created_on:String,modified_on:String,contenttype:String,width:Integer,height:Integer,bannertext:String,imageurl:String,htmltemplate:String,company_key:Integer,group_key:Integer,part_key:Integer,category_key:Integer,part_family_key:Integer,campaignname:String,commitment_type_key:Integer,search_field:String,campaign_start:String,campaign_end:String,campaign_commitment:Integer,order_type_key:Integer,clientid:Integer,clientname:String,ad_type_key:Integer,ad_focus_key:Integer,ad_label_key:Integer,geotarget:String,compiledlimitation:String,geotarget_complement:Boolean,part_description:String,banner_type:String,partnering_manufacturer:String,call_to_action:String)
def convert_banner_dim(inpath:String):Dataset[class_banner_dim]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<37) fields = oldfields.padTo(37,null)
    class_banner_dim(str2int(fields(0)),str2int(fields(1)),str2int(fields(2)),str2int(fields(3)),checknull(fields(4)),checknull(fields(5)),checknull(fields(6)),checknull(fields(7)),str2int(fields(8)),str2int(fields(9)),checknull(fields(10)),checknull(fields(11)),checknull(fields(12)),str2int(fields(13)),str2int(fields(14)),str2int(fields(15)),str2int(fields(16)),str2int(fields(17)),checknull(fields(18)),str2int(fields(19)),checknull(fields(20)),checknull(fields(21)),checknull(fields(22)),str2int(fields(23)),str2int(fields(24)),str2int(fields(25)),checknull(fields(26)),str2int(fields(27)),str2int(fields(28)),str2int(fields(29)),checknull(fields(30)),checknull(fields(31)),str2bool(fields(32)),checknull(fields(33)),checknull(fields(34)),checknull(fields(35)),checknull(fields(36)))})
  df
}
case class class_company_dim(company_key:Long,name:String,company_id:Long,modified_on:String,created_on:String,v2_id:Long,manufacturer_key:Integer,distributor_key:Integer,is_distributor:Boolean,is_manufacturer:Boolean,is_active:Boolean,is_ox_client:Boolean,is_sep:Boolean,is_non_franchised:Boolean,fc_short_name:String,clientid:Integer,parent_company_id:Long,catalog_size:String,market_segment:String,product_specialty:String)
def convert_company_dim(inpath:String):Dataset[class_company_dim]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<20) fields = oldfields.padTo(20,null)
    class_company_dim(str2long(fields(0)),checknull(fields(1)),str2long(fields(2)),checknull(fields(3)),checknull(fields(4)),str2long(fields(5)),str2int(fields(6)),str2int(fields(7)),str2bool(fields(8)),str2bool(fields(9)),str2bool(fields(10)),str2bool(fields(11)),str2bool(fields(12)),str2bool(fields(13)),checknull(fields(14)),str2int(fields(15)),str2long(fields(16)),checknull(fields(17)),checknull(fields(18)),checknull(fields(19)))})
  df
}
case class class_country_dim(country_key:Integer,alpha2:String,name:String,continent:String,alpha3:String,region:String,sub_region:String,created_on:String,modified_on:String)
def convert_country_dim(inpath:String):Dataset[class_country_dim]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<9) fields = oldfields.padTo(9,null)
    class_country_dim(str2int(fields(0)),checknull(fields(1)),checknull(fields(2)),checknull(fields(3)),checknull(fields(4)),checknull(fields(5)),checknull(fields(6)),checknull(fields(7)),checknull(fields(8)))})
  df
}
case class class_naics(master_account_id:Integer,master_account_name:String,source_account_id:Integer,source_account_name:String,source_type:String,created_on:String,updated_on:String,market_segment:String,catalog_size:String,classification:String,product_specialty:String,digital_maturity:String,naics_code:Integer,naics_description:String,sic_code:String,sic_description:String)
def convert_naics(inpath:String):Dataset[class_naics]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<16) fields = oldfields.padTo(16,null)
    class_naics(str2int(fields(0)),checknull(fields(1)),str2int(fields(2)),checknull(fields(3)),checknull(fields(4)),checknull(fields(5)),checknull(fields(6)),checknull(fields(7)),checknull(fields(8)),checknull(fields(9)),checknull(fields(10)),checknull(fields(11)),str2int(fields(12)),checknull(fields(13)),checknull(fields(14)),checknull(fields(15)))})
  df
}
case class class_zone_dim(zone_key:Integer,version_key:Integer,zoneid:Integer,zonename:String,description:String,created_on:String,modified_on:String,organicid:String,affiliateid:Integer,website_name:String,website_key:Integer,website_tag:String,ad_type_key:Integer,ad_product_key:Integer,product:String,domain:String,webapp:String,webapp_key:Integer)
def convert_zone_dim(inpath:String):Dataset[class_zone_dim]={
  val lines = spark.read.format("text").option("delimiter","\t").load(inpath)
  val df = lines.map(line=>{
    val oldfields = line(0).toString.split("\t")
    var fields = oldfields
    if(oldfields.length<18) fields = oldfields.padTo(18,null)
    class_zone_dim(str2int(fields(0)),str2int(fields(1)),str2int(fields(2)),checknull(fields(3)),checknull(fields(4)),checknull(fields(5)),checknull(fields(6)),checknull(fields(7)),str2int(fields(8)),checknull(fields(9)),str2int(fields(10)),checknull(fields(11)),str2int(fields(12)),str2int(fields(13)),checknull(fields(14)),checknull(fields(15)),checknull(fields(16)),str2int(fields(17)))})
  df
}
