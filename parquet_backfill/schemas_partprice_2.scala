import scala.collection.mutable.HashMap
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path,FileChecksum}

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
  }
  println("Converting "+filename+" to "+newbasefile)
  fs.delete(new Path(newbasefile),true)
  if (table.equals("fc_feed_bristol")){
    val df = convert_fc_feed_bristol(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_standardelec")){
    val df = convert_fc_feed_standardelec(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chip1stop_apac")){
    val df = convert_fc_feed_chip1stop_apac(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_detailtech_cn")){
    val df = convert_fc_feed_detailtech_cn(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_classic")){
    val df = convert_fc_feed_classic(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_bnl_jan_2021")){
    val df = convert_fc_feed_farnell_bnl_jan_2021(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_heilind_oems")){
    val df = convert_fc_feed_heilind_oems(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_element14_rp_bnl")){
    val df = convert_fc_feed_element14_rp_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_mautronics")){
    val df = convert_fc_feed_mautronics(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_fr")){
    val df = convert_fc_feed_farnell_fr(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_newark_us_bnl2")){
    val df = convert_fc_feed_newark_us_bnl2(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_origparts")){
    val df = convert_fc_feed_origparts(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_tme_bnl_fr")){
    val df = convert_fc_feed_tme_bnl_fr(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_kedlielec")){
    val df = convert_fc_feed_kedlielec(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_fi")){
    val df = convert_fc_feed_farnell_fi(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rochester_bnl")){
    val df = convert_fc_feed_rochester_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_olc")){
    val df = convert_fc_feed_olc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_galco")){
    val df = convert_fc_feed_galco(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_computer_controls")){
    val df = convert_fc_feed_computer_controls(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_bnl")){
    val df = convert_fc_feed_farnell_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_metaverse")){
    val df = convert_fc_feed_metaverse(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_icsoso")){
    val df = convert_fc_feed_icsoso(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_xituo")){
    val df = convert_fc_feed_xituo(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chip1stop_usd")){
    val df = convert_fc_feed_chip1stop_usd(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_ro_findchips")){
    val df = convert_fc_feed_distrelec_ro_findchips(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chip1stop_cny")){
    val df = convert_fc_feed_chip1stop_cny(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_comsit_fc_it")){
    val df = convert_fc_feed_comsit_fc_it(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_nl_findchips")){
    val df = convert_fc_feed_distrelec_nl_findchips(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_future_china_fc")){
    val df = convert_fc_feed_future_china_fc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_sierraic")){
    val df = convert_fc_feed_sierraic(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_sense_elec_fc")){
    val df = convert_fc_feed_sense_elec_fc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_tme_bnl_fi")){
    val df = convert_fc_feed_tme_bnl_fi(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_moseley")){
    val df = convert_fc_feed_moseley(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_element14_ph")){
    val df = convert_fc_feed_element14_ph(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_tr")){
    val df = convert_fc_feed_rs_components_tr(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_comsit_fc_de")){
    val df = convert_fc_feed_comsit_fc_de(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_microworks")){
    val df = convert_fc_feed_microworks(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_pl_oemstrade")){
    val df = convert_fc_feed_distrelec_pl_oemstrade(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_comsit_fc_in")){
    val df = convert_fc_feed_comsit_fc_in(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chip1stop_cny_multi")){
    val df = convert_fc_feed_chip1stop_cny_multi(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_dk")){
    val df = convert_fc_feed_farnell_dk(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_xinghuan")){
    val df = convert_fc_feed_xinghuan(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_analog_devices_precision_adc")){
    val df = convert_fc_feed_analog_devices_precision_adc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_jak")){
    val df = convert_fc_feed_jak(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_iccomp")){
    val df = convert_fc_feed_iccomp(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_northstar")){
    val df = convert_fc_feed_northstar(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ameya_fc_bnl")){
    val df = convert_fc_feed_ameya_fc_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_cz_mft_bnl")){
    val df = convert_fc_feed_rs_components_cz_mft_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_comsit_fc_us")){
    val df = convert_fc_feed_comsit_fc_us(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_compexpert")){
    val df = convert_fc_feed_compexpert(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ickey_bnl")){
    val df = convert_fc_feed_ickey_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_hu_mft_bnl")){
    val df = convert_fc_feed_rs_components_hu_mft_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_americas_bnl_now_stocking")){
    val df = convert_fc_feed_rs_americas_bnl_now_stocking(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_peigenesis")){
    val df = convert_fc_feed_peigenesis(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_peigenesis_004_bnl")){
    val df = convert_fc_feed_peigenesis_004_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_quotebeam")){
    val df = convert_fc_feed_quotebeam(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_apac")){
    val df = convert_fc_feed_rs_components_apac(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_jgm")){
    val df = convert_fc_feed_jgm(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chip1stop_usd_oems_3")){
    val df = convert_fc_feed_chip1stop_usd_oems_3(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_future_bnl_optimized")){
    val df = convert_fc_feed_future_bnl_optimized(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_sg")){
    val df = convert_fc_feed_rs_components_sg(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_hklilin")){
    val df = convert_fc_feed_hklilin(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rspro_ch")){
    val df = convert_fc_feed_rspro_ch(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ti")){
    val df = convert_fc_feed_ti(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rspro_cz")){
    val df = convert_fc_feed_rspro_cz(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rochester_cn_ti_bnl")){
    val df = convert_fc_feed_rochester_cn_ti_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_no")){
    val df = convert_fc_feed_farnell_no(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_semtke")){
    val df = convert_fc_feed_semtke(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rapid")){
    val df = convert_fc_feed_rapid(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_digikey_bnl")){
    val df = convert_fc_feed_digikey_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_avnet_europe")){
    val df = convert_fc_feed_avnet_europe(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_splendent")){
    val df = convert_fc_feed_splendent(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_digikey_oems_bnl")){
    val df = convert_fc_feed_digikey_oems_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_digikey_uk")){
    val df = convert_fc_feed_digikey_uk(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_breizelec_oems")){
    val df = convert_fc_feed_breizelec_oems(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_be")){
    val df = convert_fc_feed_rs_components_be(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_b2b_rs_components")){
    val df = convert_fc_feed_b2b_rs_components(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_futuretech")){
    val df = convert_fc_feed_futuretech(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rutronik_us")){
    val df = convert_fc_feed_rutronik_us(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rspro_se")){
    val df = convert_fc_feed_rspro_se(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_element14_b2b")){
    val df = convert_fc_feed_element14_b2b(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ardusimple_fc")){
    val df = convert_fc_feed_ardusimple_fc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_rohde_bnl_fr")){
    val df = convert_fc_feed_farnell_rohde_bnl_fr(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_tme_bnl_es")){
    val df = convert_fc_feed_tme_bnl_es(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_pl_mft_bnl")){
    val df = convert_fc_feed_rs_components_pl_mft_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_integ")){
    val df = convert_fc_feed_integ(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_es")){
    val df = convert_fc_feed_farnell_es(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_element14_hp_bnl")){
    val df = convert_fc_feed_element14_hp_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_pl_findchips")){
    val df = convert_fc_feed_distrelec_pl_findchips(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_sager")){
    val df = convert_fc_feed_sager(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_tme_bnl_de")){
    val df = convert_fc_feed_tme_bnl_de(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ryx")){
    val df = convert_fc_feed_ryx(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ampheo")){
    val df = convert_fc_feed_ampheo(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rutronik_euro")){
    val df = convert_fc_feed_rutronik_euro(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chip1stop_eur_oems")){
    val df = convert_fc_feed_chip1stop_eur_oems(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_oneyac_fc_china")){
    val df = convert_fc_feed_oneyac_fc_china(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_newark_bnl_littelfuse")){
    val df = convert_fc_feed_newark_bnl_littelfuse(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chip1stop_eur")){
    val df = convert_fc_feed_chip1stop_eur(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_hengshi")){
    val df = convert_fc_feed_hengshi(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_directcomp")){
    val df = convert_fc_feed_directcomp(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_asc")){
    val df = convert_fc_feed_asc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_utmel_fc")){
    val df = convert_fc_feed_utmel_fc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ezkey_fc")){
    val df = convert_fc_feed_ezkey_fc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_stmicro")){
    val df = convert_fc_feed_stmicro(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_uk")){
    val df = convert_fc_feed_rs_components_uk(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_silitech")){
    val df = convert_fc_feed_silitech(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rochester_jp_ti_bnl")){
    val df = convert_fc_feed_rochester_jp_ti_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_fi")){
    val df = convert_fc_feed_rs_components_fi(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chipmall_fc")){
    val df = convert_fc_feed_chipmall_fc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ozdisan")){
    val df = convert_fc_feed_ozdisan(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_winshare")){
    val df = convert_fc_feed_winshare(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_mygroup")){
    val df = convert_fc_feed_mygroup(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ysonix")){
    val df = convert_fc_feed_ysonix(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ti_bnl_dlp")){
    val df = convert_fc_feed_ti_bnl_dlp(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_testequity_ukie")){
    val df = convert_fc_feed_testequity_ukie(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_newadantage_bnl")){
    val df = convert_fc_feed_newadantage_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_transtector")){
    val df = convert_fc_feed_transtector(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_techdesign_oems")){
    val df = convert_fc_feed_techdesign_oems(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_heilind_bnl_te")){
    val df = convert_fc_feed_heilind_bnl_te(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_fr")){
    val df = convert_fc_feed_rs_components_fr(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_kruse_oems")){
    val df = convert_fc_feed_kruse_oems(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_emea")){
    val df = convert_fc_feed_rs_components_emea(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_electronicount")){
    val df = convert_fc_feed_electronicount(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_se_mft_bnl")){
    val df = convert_fc_feed_rs_components_se_mft_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_nacsemi_bnl")){
    val df = convert_fc_feed_nacsemi_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rspro_ru")){
    val df = convert_fc_feed_rspro_ru(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_de")){
    val df = convert_fc_feed_farnell_de(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_shikues")){
    val df = convert_fc_feed_shikues(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ibuyxs_bnl")){
    val df = convert_fc_feed_ibuyxs_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_ee_findchips")){
    val df = convert_fc_feed_distrelec_ee_findchips(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_sielectronics")){
    val df = convert_fc_feed_sielectronics(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_stockers")){
    val df = convert_fc_feed_stockers(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_j2")){
    val df = convert_fc_feed_j2(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_sense_elec")){
    val df = convert_fc_feed_sense_elec(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_bdrtech")){
    val df = convert_fc_feed_bdrtech(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_superman")){
    val df = convert_fc_feed_superman(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_lcsc")){
    val df = convert_fc_feed_lcsc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_si")){
    val df = convert_fc_feed_farnell_si(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_global")){
    val df = convert_fc_feed_rs_global(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_peigenesis_us")){
    val df = convert_fc_feed_peigenesis_us(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_b2b_comsit")){
    val df = convert_fc_feed_b2b_comsit(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_hengfeng")){
    val df = convert_fc_feed_hengfeng(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_nexgen")){
    val df = convert_fc_feed_nexgen(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_no")){
    val df = convert_fc_feed_rs_components_no(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chip_digger")){
    val df = convert_fc_feed_chip_digger(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_it_mft_bnl")){
    val df = convert_fc_feed_rs_components_it_mft_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_digikey_bnl_lookup")){
    val df = convert_fc_feed_digikey_bnl_lookup(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_plcdirect")){
    val df = convert_fc_feed_plcdirect(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_master_oems")){
    val df = convert_fc_feed_master_oems(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chipchip")){
    val df = convert_fc_feed_chipchip(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_lt")){
    val df = convert_fc_feed_farnell_lt(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_hongte")){
    val df = convert_fc_feed_hongte(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_verical_bnl")){
    val df = convert_fc_feed_verical_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_bg")){
    val df = convert_fc_feed_farnell_bg(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rspro_sk")){
    val df = convert_fc_feed_rspro_sk(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_walker")){
    val df = convert_fc_feed_walker(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chip1stop_jpy_oems_2")){
    val df = convert_fc_feed_chip1stop_jpy_oems_2(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_electronictreasures")){
    val df = convert_fc_feed_electronictreasures(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_ie")){
    val df = convert_fc_feed_farnell_ie(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_lt_oemstrade")){
    val df = convert_fc_feed_distrelec_lt_oemstrade(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_bisco_bnl_2022")){
    val df = convert_fc_feed_bisco_bnl_2022(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_at")){
    val df = convert_fc_feed_farnell_at(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rspro_ro")){
    val df = convert_fc_feed_rspro_ro(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_digikey_de_bnl")){
    val df = convert_fc_feed_digikey_de_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_vigor")){
    val df = convert_fc_feed_vigor(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_microchip_usa_oems")){
    val df = convert_fc_feed_microchip_usa_oems(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_nl")){
    val df = convert_fc_feed_farnell_nl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_no_mft_bnl")){
    val df = convert_fc_feed_rs_components_no_mft_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_dk")){
    val df = convert_fc_feed_rs_components_dk(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_yicintl")){
    val df = convert_fc_feed_yicintl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_newadvantage_oems_us")){
    val df = convert_fc_feed_newadvantage_oems_us(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_lcomus")){
    val df = convert_fc_feed_lcomus(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_bisco")){
    val df = convert_fc_feed_bisco(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ysyelec")){
    val df = convert_fc_feed_ysyelec(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_hk_keep_booming")){
    val df = convert_fc_feed_hk_keep_booming(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_fr_findchips")){
    val df = convert_fc_feed_distrelec_fr_findchips(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rochester_kr_ti_bnl")){
    val df = convert_fc_feed_rochester_kr_ti_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_fairview")){
    val df = convert_fc_feed_fairview(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_centum")){
    val df = convert_fc_feed_centum(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ibuyxs")){
    val df = convert_fc_feed_ibuyxs(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ichunt_fc")){
    val df = convert_fc_feed_ichunt_fc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_fdh_electronics")){
    val df = convert_fc_feed_fdh_electronics(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_europe_bnl_it")){
    val df = convert_fc_feed_rs_europe_bnl_it(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_peigenesis_005")){
    val df = convert_fc_feed_peigenesis_005(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_es")){
    val df = convert_fc_feed_rs_components_es(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_tme")){
    val df = convert_fc_feed_tme(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_lcsc_oems")){
    val df = convert_fc_feed_lcsc_oems(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_pasternack_us")){
    val df = convert_fc_feed_pasternack_us(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_element14_sg")){
    val df = convert_fc_feed_element14_sg(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_es_mft_bnl")){
    val df = convert_fc_feed_rs_components_es_mft_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_ee_oemstrade")){
    val df = convert_fc_feed_distrelec_ee_oemstrade(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ti_bnl_epd")){
    val df = convert_fc_feed_ti_bnl_epd(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_de")){
    val df = convert_fc_feed_rs_components_de(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_digikey_cn_bnl")){
    val df = convert_fc_feed_digikey_cn_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_digikey_bnl_emea_test")){
    val df = convert_fc_feed_digikey_bnl_emea_test(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rochester_cn")){
    val df = convert_fc_feed_rochester_cn(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_k1tech")){
    val df = convert_fc_feed_k1tech(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_lt_findchips")){
    val df = convert_fc_feed_distrelec_lt_findchips(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_hu")){
    val df = convert_fc_feed_farnell_hu(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ti_bnl_epd2")){
    val df = convert_fc_feed_ti_bnl_epd2(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_btc_oemstrade")){
    val df = convert_fc_feed_btc_oemstrade(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_breizelec_fc")){
    val df = convert_fc_feed_breizelec_fc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_fcpro_apac")){
    val df = convert_fc_feed_farnell_fcpro_apac(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_icc")){
    val df = convert_fc_feed_icc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_waytek_bnl")){
    val df = convert_fc_feed_waytek_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_rohde_bnl_de")){
    val df = convert_fc_feed_farnell_rohde_bnl_de(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ikeyparts")){
    val df = convert_fc_feed_ikeyparts(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_link")){
    val df = convert_fc_feed_link(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_comsit_fc_mx")){
    val df = convert_fc_feed_comsit_fc_mx(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_jingyaorun")){
    val df = convert_fc_feed_jingyaorun(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_pt_mft_bnl")){
    val df = convert_fc_feed_rs_components_pt_mft_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_renesas_digikey")){
    val df = convert_fc_feed_renesas_digikey(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_lcom")){
    val df = convert_fc_feed_lcom(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_shirakaba")){
    val df = convert_fc_feed_shirakaba(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rspro_pt")){
    val df = convert_fc_feed_rspro_pt(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_comsit_oems_fr")){
    val df = convert_fc_feed_comsit_oems_fr(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ayelectronics")){
    val df = convert_fc_feed_ayelectronics(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rochester_ti_bnl")){
    val df = convert_fc_feed_rochester_ti_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_dicchip")){
    val df = convert_fc_feed_dicchip(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ti_bnl_hval")){
    val df = convert_fc_feed_ti_bnl_hval(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_dk_mft_bnl")){
    val df = convert_fc_feed_rs_components_dk_mft_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rutronik_cn")){
    val df = convert_fc_feed_rutronik_cn(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rgelek")){
    val df = convert_fc_feed_rgelek(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_peigenesis_001_bnl")){
    val df = convert_fc_feed_peigenesis_001_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_mouser_bnl_apac")){
    val df = convert_fc_feed_mouser_bnl_apac(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_future_china")){
    val df = convert_fc_feed_future_china(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ibs")){
    val df = convert_fc_feed_ibs(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_global_sourcing")){
    val df = convert_fc_feed_global_sourcing(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_newark_ca")){
    val df = convert_fc_feed_newark_ca(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_nl")){
    val df = convert_fc_feed_rs_components_nl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_adafruit")){
    val df = convert_fc_feed_adafruit(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_drex")){
    val df = convert_fc_feed_drex(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_nacsemi_oems_us")){
    val df = convert_fc_feed_nacsemi_oems_us(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_hkcinty")){
    val df = convert_fc_feed_hkcinty(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chip1cloud")){
    val df = convert_fc_feed_chip1cloud(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_esino")){
    val df = convert_fc_feed_esino(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_detailtech")){
    val df = convert_fc_feed_detailtech(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_it_oemstrade")){
    val df = convert_fc_feed_distrelec_it_oemstrade(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_tme_bnl_it")){
    val df = convert_fc_feed_tme_bnl_it(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_btc_findchips")){
    val df = convert_fc_feed_btc_findchips(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_oneyac")){
    val df = convert_fc_feed_oneyac(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_olc_bnl_kilovac")){
    val df = convert_fc_feed_olc_bnl_kilovac(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_jp")){
    val df = convert_fc_feed_farnell_jp(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_superchip")){
    val df = convert_fc_feed_superchip(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_cgelec")){
    val df = convert_fc_feed_cgelec(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_my")){
    val df = convert_fc_feed_rs_components_my(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_grandpower")){
    val df = convert_fc_feed_grandpower(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_element14_au")){
    val df = convert_fc_feed_element14_au(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_tw")){
    val df = convert_fc_feed_rs_components_tw(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_comsit_fc_cn")){
    val df = convert_fc_feed_comsit_fc_cn(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_shengyu")){
    val df = convert_fc_feed_shengyu(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_opulent")){
    val df = convert_fc_feed_opulent(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_at")){
    val df = convert_fc_feed_rs_components_at(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ezkey_oems")){
    val df = convert_fc_feed_ezkey_oems(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_comsit_fc_br")){
    val df = convert_fc_feed_comsit_fc_br(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_nl_mft_bnl")){
    val df = convert_fc_feed_rs_components_nl_mft_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_avnet_europe_abacus")){
    val df = convert_fc_feed_avnet_europe_abacus(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_cuidevices")){
    val df = convert_fc_feed_cuidevices(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chip1stop_usd_oems")){
    val df = convert_fc_feed_chip1stop_usd_oems(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_burklin")){
    val df = convert_fc_feed_burklin(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_sehot")){
    val df = convert_fc_feed_sehot(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_tpsglobal")){
    val df = convert_fc_feed_tpsglobal(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_cicmaster")){
    val df = convert_fc_feed_cicmaster(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_digikey_kr")){
    val df = convert_fc_feed_digikey_kr(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_dasenic")){
    val df = convert_fc_feed_dasenic(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_americanmicro")){
    val df = convert_fc_feed_americanmicro(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_rp_bnl")){
    val df = convert_fc_feed_farnell_rp_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_newadvantage_fc_us")){
    val df = convert_fc_feed_newadvantage_fc_us(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_frontview")){
    val df = convert_fc_feed_frontview(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_tme_lookup_bnl")){
    val df = convert_fc_feed_tme_lookup_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_compelec")){
    val df = convert_fc_feed_compelec(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_saiaosi")){
    val df = convert_fc_feed_saiaosi(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_us")){
    val df = convert_fc_feed_rs_components_us(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_lantana")){
    val df = convert_fc_feed_lantana(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rspro_be")){
    val df = convert_fc_feed_rspro_be(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_appelec")){
    val df = convert_fc_feed_appelec(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_it")){
    val df = convert_fc_feed_farnell_it(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_icc_bnl")){
    val df = convert_fc_feed_icc_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_verical_global")){
    val df = convert_fc_feed_verical_global(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_molex_bnl")){
    val df = convert_fc_feed_rs_components_molex_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_findfpga")){
    val df = convert_fc_feed_findfpga(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_arrow_ads")){
    val df = convert_fc_feed_arrow_ads(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_americas_bnl_siemens")){
    val df = convert_fc_feed_rs_americas_bnl_siemens(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_powell")){
    val df = convert_fc_feed_powell(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_hu")){
    val df = convert_fc_feed_rs_components_hu(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_digikey_de")){
    val df = convert_fc_feed_digikey_de(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_futuretech_fc")){
    val df = convert_fc_feed_futuretech_fc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_be_oemstrade")){
    val df = convert_fc_feed_distrelec_be_oemstrade(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_coilcraft")){
    val df = convert_fc_feed_coilcraft(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_kr")){
    val df = convert_fc_feed_rs_components_kr(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_pui")){
    val df = convert_fc_feed_pui(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_southchip")){
    val df = convert_fc_feed_southchip(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_megastar")){
    val df = convert_fc_feed_megastar(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ameya_fc")){
    val df = convert_fc_feed_ameya_fc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_bestcomp")){
    val df = convert_fc_feed_bestcomp(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_heilind_world")){
    val df = convert_fc_feed_heilind_world(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_tme_bnl_si")){
    val df = convert_fc_feed_tme_bnl_si(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_perfectparts_oems")){
    val df = convert_fc_feed_perfectparts_oems(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chuanghan")){
    val df = convert_fc_feed_chuanghan(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_testequity")){
    val df = convert_fc_feed_testequity(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chip_digger_fc")){
    val df = convert_fc_feed_chip_digger_fc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_fr_mft_bnl")){
    val df = convert_fc_feed_rs_components_fr_mft_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rutronik_global")){
    val df = convert_fc_feed_rutronik_global(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_greenchips_fc")){
    val df = convert_fc_feed_greenchips_fc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_heisener")){
    val df = convert_fc_feed_heisener(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_aicreer")){
    val df = convert_fc_feed_aicreer(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_avnet_americas")){
    val df = convert_fc_feed_avnet_americas(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_peigenesis_005_bnl")){
    val df = convert_fc_feed_peigenesis_005_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_teconn_dynamic_bnl")){
    val df = convert_fc_feed_teconn_dynamic_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_de_mft_bnl")){
    val df = convert_fc_feed_rs_components_de_mft_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_jdcomp")){
    val df = convert_fc_feed_jdcomp(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_wuhan")){
    val df = convert_fc_feed_wuhan(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_anterwell")){
    val df = convert_fc_feed_anterwell(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rochester_ru")){
    val df = convert_fc_feed_rochester_ru(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_besttech")){
    val df = convert_fc_feed_besttech(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_cplus")){
    val df = convert_fc_feed_cplus(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_tme_bnl_pl")){
    val df = convert_fc_feed_tme_bnl_pl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rspro_uk")){
    val df = convert_fc_feed_rspro_uk(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_powersignal")){
    val df = convert_fc_feed_powersignal(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_resistortoday")){
    val df = convert_fc_feed_resistortoday(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_besatech")){
    val df = convert_fc_feed_besatech(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_corestaff")){
    val df = convert_fc_feed_corestaff(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_fudatonghe")){
    val df = convert_fc_feed_fudatonghe(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_greenchips")){
    val df = convert_fc_feed_greenchips(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_pl")){
    val df = convert_fc_feed_farnell_pl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_dst")){
    val df = convert_fc_feed_dst(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rochester_br_bnl")){
    val df = convert_fc_feed_rochester_br_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_elfaro")){
    val df = convert_fc_feed_elfaro(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_it_findchips")){
    val df = convert_fc_feed_distrelec_it_findchips(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_th")){
    val df = convert_fc_feed_rs_components_th(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chipone_fc")){
    val df = convert_fc_feed_chipone_fc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_jp")){
    val df = convert_fc_feed_rs_components_jp(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ntemall")){
    val df = convert_fc_feed_ntemall(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chipspulse_fc")){
    val df = convert_fc_feed_chipspulse_fc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rspro_tr")){
    val df = convert_fc_feed_rspro_tr(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_spartelec")){
    val df = convert_fc_feed_spartelec(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_peigenesis_004")){
    val df = convert_fc_feed_peigenesis_004(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_newark_rp_bnl")){
    val df = convert_fc_feed_newark_rp_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_at_oemstrade")){
    val df = convert_fc_feed_distrelec_at_oemstrade(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_lv")){
    val df = convert_fc_feed_farnell_lv(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_corestaff_findchips")){
    val df = convert_fc_feed_corestaff_findchips(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_heilind_americas")){
    val df = convert_fc_feed_heilind_americas(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_comsit_fc")){
    val df = convert_fc_feed_comsit_fc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_ch_mft_bnl")){
    val df = convert_fc_feed_rs_components_ch_mft_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_americas_bnl")){
    val df = convert_fc_feed_rs_americas_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_flychips")){
    val df = convert_fc_feed_flychips(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_dovecomp")){
    val df = convert_fc_feed_dovecomp(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_unibetter")){
    val df = convert_fc_feed_unibetter(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_it")){
    val df = convert_fc_feed_rs_components_it(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_arrow_bnl_3")){
    val df = convert_fc_feed_arrow_bnl_3(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_peigenesis_010")){
    val df = convert_fc_feed_peigenesis_010(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_comsit_fc_ru")){
    val df = convert_fc_feed_comsit_fc_ru(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_lanka")){
    val df = convert_fc_feed_lanka(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_comsit_oems_es")){
    val df = convert_fc_feed_comsit_oems_es(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_future")){
    val df = convert_fc_feed_future(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_macroquest")){
    val df = convert_fc_feed_macroquest(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_compsearch")){
    val df = convert_fc_feed_compsearch(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_heqingelec_fc")){
    val df = convert_fc_feed_heqingelec_fc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_lv_oemstrade")){
    val df = convert_fc_feed_distrelec_lv_oemstrade(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_burklin_findchips")){
    val df = convert_fc_feed_burklin_findchips(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_unicom")){
    val df = convert_fc_feed_unicom(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_be_findchips")){
    val df = convert_fc_feed_distrelec_be_findchips(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_hp_bnl")){
    val df = convert_fc_feed_farnell_hp_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_comsit_oems_de")){
    val df = convert_fc_feed_comsit_oems_de(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_xinyixin")){
    val df = convert_fc_feed_xinyixin(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_perfectparts")){
    val df = convert_fc_feed_perfectparts(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_tasnme")){
    val df = convert_fc_feed_tasnme(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chip1stop_usd_oems_2")){
    val df = convert_fc_feed_chip1stop_usd_oems_2(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_za_mft_bnl")){
    val df = convert_fc_feed_rs_components_za_mft_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_b2b_etron")){
    val df = convert_fc_feed_b2b_etron(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_b2b_corestaff")){
    val df = convert_fc_feed_b2b_corestaff(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_karmieltech")){
    val df = convert_fc_feed_karmieltech(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_avnet_europe_bnl")){
    val df = convert_fc_feed_avnet_europe_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_analog_devices")){
    val df = convert_fc_feed_analog_devices(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chipstock")){
    val df = convert_fc_feed_chipstock(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_xidaelec")){
    val df = convert_fc_feed_xidaelec(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_no_oemstrade")){
    val df = convert_fc_feed_distrelec_no_oemstrade(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_megastar_fc")){
    val df = convert_fc_feed_megastar_fc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_oneyac_global")){
    val df = convert_fc_feed_oneyac_global(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_tgmicro")){
    val df = convert_fc_feed_tgmicro(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_szlcsc")){
    val df = convert_fc_feed_szlcsc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_newadvantage")){
    val df = convert_fc_feed_newadvantage(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_minamikaze")){
    val df = convert_fc_feed_minamikaze(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_lixinc")){
    val df = convert_fc_feed_lixinc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_iceasy")){
    val df = convert_fc_feed_iceasy(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_arrow_apac_capacitors_bnl")){
    val df = convert_fc_feed_arrow_apac_capacitors_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_cz")){
    val df = convert_fc_feed_farnell_cz(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_comsit_oems_us")){
    val df = convert_fc_feed_comsit_oems_us(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_teledyne_e2v")){
    val df = convert_fc_feed_teledyne_e2v(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_at_findchips")){
    val df = convert_fc_feed_distrelec_at_findchips(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ztz")){
    val df = convert_fc_feed_ztz(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_wanlianxin")){
    val df = convert_fc_feed_wanlianxin(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_avnet_europe_silica")){
    val df = convert_fc_feed_avnet_europe_silica(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_ch")){
    val df = convert_fc_feed_farnell_ch(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_peigenesis_006_bnl")){
    val df = convert_fc_feed_peigenesis_006_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_icsoeasy")){
    val df = convert_fc_feed_icsoeasy(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_nacsemi_oems")){
    val df = convert_fc_feed_nacsemi_oems(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_global_solutions")){
    val df = convert_fc_feed_global_solutions(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_at_mft_bnl")){
    val df = convert_fc_feed_rs_components_at_mft_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_avnet_europe_ebv")){
    val df = convert_fc_feed_avnet_europe_ebv(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chip1stop_eur_oems_3")){
    val df = convert_fc_feed_chip1stop_eur_oems_3(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_comsit_oems_in")){
    val df = convert_fc_feed_comsit_oems_in(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_esonic")){
    val df = convert_fc_feed_esonic(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rspro_es")){
    val df = convert_fc_feed_rspro_es(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_tme_bnl_cz")){
    val df = convert_fc_feed_tme_bnl_cz(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_fi_oemstrade")){
    val df = convert_fc_feed_distrelec_fi_oemstrade(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_pl")){
    val df = convert_fc_feed_rs_components_pl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_ch_oemstrade")){
    val df = convert_fc_feed_distrelec_ch_oemstrade(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_velocity")){
    val df = convert_fc_feed_velocity(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_componentsolutions")){
    val df = convert_fc_feed_componentsolutions(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_macroship")){
    val df = convert_fc_feed_macroship(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_avnet_japan")){
    val df = convert_fc_feed_avnet_japan(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ibuyxs_fc")){
    val df = convert_fc_feed_ibuyxs_fc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chipspulse_bnl")){
    val df = convert_fc_feed_chipspulse_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_lv_findchips")){
    val df = convert_fc_feed_distrelec_lv_findchips(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_element14_my")){
    val df = convert_fc_feed_element14_my(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_fcpro_emea")){
    val df = convert_fc_feed_farnell_fcpro_emea(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_spectrum")){
    val df = convert_fc_feed_spectrum(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chip1stop_hk_tw_kr")){
    val df = convert_fc_feed_chip1stop_hk_tw_kr(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_de_oemstrade")){
    val df = convert_fc_feed_distrelec_de_oemstrade(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rxelec")){
    val df = convert_fc_feed_rxelec(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_ie")){
    val df = convert_fc_feed_rs_components_ie(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_peigenesis_007")){
    val df = convert_fc_feed_peigenesis_007(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_commoditycomp")){
    val df = convert_fc_feed_commoditycomp(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_nxp")){
    val df = convert_fc_feed_nxp(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chip1cloud_fc")){
    val df = convert_fc_feed_chip1cloud_fc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_symmetry_fc")){
    val df = convert_fc_feed_symmetry_fc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_heilind_bnl")){
    val df = convert_fc_feed_heilind_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_melchioni")){
    val df = convert_fc_feed_melchioni(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_intercard")){
    val df = convert_fc_feed_intercard(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_egbtech")){
    val df = convert_fc_feed_egbtech(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_greenlight")){
    val df = convert_fc_feed_greenlight(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_semix")){
    val df = convert_fc_feed_semix(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_tobyelec")){
    val df = convert_fc_feed_tobyelec(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_de_findchips")){
    val df = convert_fc_feed_distrelec_de_findchips(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rspro_nl")){
    val df = convert_fc_feed_rspro_nl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_bnl_2")){
    val df = convert_fc_feed_farnell_bnl_2(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_teconn_bnl")){
    val df = convert_fc_feed_teconn_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_element14_th")){
    val df = convert_fc_feed_element14_th(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_peigenesis_001")){
    val df = convert_fc_feed_peigenesis_001(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rjcomp")){
    val df = convert_fc_feed_rjcomp(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_no_findchips")){
    val df = convert_fc_feed_distrelec_no_findchips(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_cn")){
    val df = convert_fc_feed_rs_components_cn(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_avnet_bnl_americas")){
    val df = convert_fc_feed_avnet_bnl_americas(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_visioncomp")){
    val df = convert_fc_feed_visioncomp(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chip1stop_usd_multi")){
    val df = convert_fc_feed_chip1stop_usd_multi(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_gangbo")){
    val df = convert_fc_feed_gangbo(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_wenorca")){
    val df = convert_fc_feed_wenorca(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_semihouse")){
    val df = convert_fc_feed_semihouse(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_sekorm")){
    val df = convert_fc_feed_sekorm(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_winsource")){
    val df = convert_fc_feed_winsource(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_sitime")){
    val df = convert_fc_feed_sitime(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_converge")){
    val df = convert_fc_feed_converge(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_element14_kr")){
    val df = convert_fc_feed_element14_kr(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_americas_dynamic")){
    val df = convert_fc_feed_rs_americas_dynamic(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_future_bnl")){
    val df = convert_fc_feed_future_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_digikey_cn_b2b")){
    val df = convert_fc_feed_digikey_cn_b2b(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_ch_findchips")){
    val df = convert_fc_feed_distrelec_ch_findchips(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_ru")){
    val df = convert_fc_feed_farnell_ru(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chipmh")){
    val df = convert_fc_feed_chipmh(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_linkinv")){
    val df = convert_fc_feed_linkinv(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_kenawang")){
    val df = convert_fc_feed_kenawang(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rspro_fi")){
    val df = convert_fc_feed_rspro_fi(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_zxd")){
    val df = convert_fc_feed_zxd(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rspro_fr")){
    val df = convert_fc_feed_rspro_fr(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_avnet_asia")){
    val df = convert_fc_feed_avnet_asia(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_sensible")){
    val df = convert_fc_feed_sensible(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_hongxinwei")){
    val df = convert_fc_feed_hongxinwei(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_amplechip")){
    val df = convert_fc_feed_amplechip(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_fi_findchips")){
    val df = convert_fc_feed_distrelec_fi_findchips(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_escomp")){
    val df = convert_fc_feed_escomp(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_hkdcy")){
    val df = convert_fc_feed_hkdcy(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_inventorymp")){
    val df = convert_fc_feed_inventorymp(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_za")){
    val df = convert_fc_feed_rs_components_za(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_nacsemi")){
    val df = convert_fc_feed_nacsemi(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_vanda")){
    val df = convert_fc_feed_vanda(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ti_bnl_epd1")){
    val df = convert_fc_feed_ti_bnl_epd1(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_finestock")){
    val df = convert_fc_feed_finestock(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_comsit_oems")){
    val df = convert_fc_feed_comsit_oems(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_sk")){
    val df = convert_fc_feed_farnell_sk(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_wpg_americas")){
    val df = convert_fc_feed_wpg_americas(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_global_oemstrade")){
    val df = convert_fc_feed_distrelec_global_oemstrade(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_greentree")){
    val df = convert_fc_feed_greentree(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rspro_dk")){
    val df = convert_fc_feed_rspro_dk(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_richelec")){
    val df = convert_fc_feed_richelec(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_ro")){
    val df = convert_fc_feed_farnell_ro(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_arrow")){
    val df = convert_fc_feed_arrow(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_master")){
    val df = convert_fc_feed_master(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_globaltek")){
    val df = convert_fc_feed_globaltek(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_etchips")){
    val df = convert_fc_feed_etchips(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_xsource")){
    val df = convert_fc_feed_xsource(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_cz")){
    val df = convert_fc_feed_rs_components_cz(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_nacsemi_fc")){
    val df = convert_fc_feed_nacsemi_fc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_ch")){
    val df = convert_fc_feed_rs_components_ch(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_comsit_oems_it")){
    val df = convert_fc_feed_comsit_oems_it(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_ie_mft_bnl")){
    val df = convert_fc_feed_rs_components_ie_mft_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chip1stop_eur_multi")){
    val df = convert_fc_feed_chip1stop_eur_multi(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_avnet_americas_bnl")){
    val df = convert_fc_feed_avnet_americas_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ti_bnl_app")){
    val df = convert_fc_feed_ti_bnl_app(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rspro_no")){
    val df = convert_fc_feed_rspro_no(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_nz")){
    val df = convert_fc_feed_rs_components_nz(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_peigenesis_us_bnl")){
    val df = convert_fc_feed_peigenesis_us_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_newstrength")){
    val df = convert_fc_feed_newstrength(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_peerless")){
    val df = convert_fc_feed_peerless(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_dgttech")){
    val df = convert_fc_feed_dgttech(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chip1stop_jpy_oems")){
    val df = convert_fc_feed_chip1stop_jpy_oems(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_ro_mft_bnl")){
    val df = convert_fc_feed_rs_components_ro_mft_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_kstcomp")){
    val df = convert_fc_feed_kstcomp(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_forward")){
    val df = convert_fc_feed_forward(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_weyland")){
    val df = convert_fc_feed_weyland(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_littletech")){
    val df = convert_fc_feed_littletech(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_molex")){
    val df = convert_fc_feed_molex(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_oneyac_oems")){
    val df = convert_fc_feed_oneyac_oems(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_zhongkaixin")){
    val df = convert_fc_feed_zhongkaixin(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chipspulse")){
    val df = convert_fc_feed_chipspulse(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chip1stop")){
    val df = convert_fc_feed_chip1stop(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_se_oemstrade")){
    val df = convert_fc_feed_distrelec_se_oemstrade(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_bnl_littelfuse")){
    val df = convert_fc_feed_farnell_bnl_littelfuse(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_netsight")){
    val df = convert_fc_feed_netsight(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_btwelec")){
    val df = convert_fc_feed_btwelec(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_heilind_europe")){
    val df = convert_fc_feed_heilind_europe(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_allied_findchips_us")){
    val df = convert_fc_feed_allied_findchips_us(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_relayspec")){
    val df = convert_fc_feed_relayspec(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_electroshield")){
    val df = convert_fc_feed_electroshield(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_cz_oemstrade")){
    val df = convert_fc_feed_distrelec_cz_oemstrade(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ameya_oems")){
    val df = convert_fc_feed_ameya_oems(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_techdesign")){
    val df = convert_fc_feed_techdesign(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_teamgene")){
    val df = convert_fc_feed_teamgene(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_macroquest_fc")){
    val df = convert_fc_feed_macroquest_fc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_oems")){
    val df = convert_fc_feed_distrelec_oems(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_neutron")){
    val df = convert_fc_feed_neutron(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_amh")){
    val df = convert_fc_feed_amh(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_globx")){
    val df = convert_fc_feed_globx(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_touchstone_systems")){
    val df = convert_fc_feed_touchstone_systems(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_lukelec")){
    val df = convert_fc_feed_lukelec(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_dk_findchips")){
    val df = convert_fc_feed_distrelec_dk_findchips(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_waytek_fc")){
    val df = convert_fc_feed_waytek_fc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rspro_it")){
    val df = convert_fc_feed_rspro_it(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_b2b_cecport")){
    val df = convert_fc_feed_b2b_cecport(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_erize")){
    val df = convert_fc_feed_erize(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_murata_cn")){
    val df = convert_fc_feed_murata_cn(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chiefent")){
    val df = convert_fc_feed_chiefent(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ecco")){
    val df = convert_fc_feed_ecco(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_icpartner")){
    val df = convert_fc_feed_icpartner(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_element14_tw")){
    val df = convert_fc_feed_element14_tw(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rspro_de")){
    val df = convert_fc_feed_rspro_de(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_digikey")){
    val df = convert_fc_feed_digikey(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_vyrian")){
    val df = convert_fc_feed_vyrian(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_electrocraft")){
    val df = convert_fc_feed_electrocraft(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_global_findchips")){
    val df = convert_fc_feed_distrelec_global_findchips(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_vast_global")){
    val df = convert_fc_feed_vast_global(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_arrow_bnl_smp")){
    val df = convert_fc_feed_arrow_bnl_smp(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ichunt")){
    val df = convert_fc_feed_ichunt(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_avnet_japan_bnl")){
    val df = convert_fc_feed_avnet_japan_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_element14_in")){
    val df = convert_fc_feed_element14_in(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_electroent")){
    val df = convert_fc_feed_electroent(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rochester")){
    val df = convert_fc_feed_rochester(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_uk_mft_bnl")){
    val df = convert_fc_feed_rs_components_uk_mft_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_digikey_cn")){
    val df = convert_fc_feed_digikey_cn(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_newideas")){
    val df = convert_fc_feed_newideas(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_se")){
    val df = convert_fc_feed_farnell_se(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_shenghuayuan")){
    val df = convert_fc_feed_shenghuayuan(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_directcomp_fc")){
    val df = convert_fc_feed_directcomp_fc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_comsit")){
    val df = convert_fc_feed_comsit(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_flipelec")){
    val df = convert_fc_feed_flipelec(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_element14_cn")){
    val df = convert_fc_feed_element14_cn(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_winsource_fc")){
    val df = convert_fc_feed_winsource_fc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_comsit_available")){
    val df = convert_fc_feed_comsit_available(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_comsit_oems_cn")){
    val df = convert_fc_feed_comsit_oems_cn(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_icchipshop")){
    val df = convert_fc_feed_icchipshop(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_venatech")){
    val df = convert_fc_feed_venatech(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_future_cn_bnl")){
    val df = convert_fc_feed_future_cn_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_incielcom")){
    val df = convert_fc_feed_incielcom(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_tlc_electronics")){
    val df = convert_fc_feed_tlc_electronics(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_pasternack")){
    val df = convert_fc_feed_pasternack(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_b2b_chip1stop")){
    val df = convert_fc_feed_b2b_chip1stop(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_hongyinwei")){
    val df = convert_fc_feed_hongyinwei(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_sk")){
    val df = convert_fc_feed_rs_components_sk(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_cdmelec")){
    val df = convert_fc_feed_cdmelec(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_se_findchips")){
    val df = convert_fc_feed_distrelec_se_findchips(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_newadvantage_fc")){
    val df = convert_fc_feed_newadvantage_fc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chip_germany")){
    val df = convert_fc_feed_chip_germany(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_nacsemi_us")){
    val df = convert_fc_feed_nacsemi_us(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_semour")){
    val df = convert_fc_feed_semour(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_newark_hp_bnl")){
    val df = convert_fc_feed_newark_hp_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_allied_oemstrade_us")){
    val df = convert_fc_feed_allied_oemstrade_us(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_fi_mft_bnl")){
    val df = convert_fc_feed_rs_components_fi_mft_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_ro")){
    val df = convert_fc_feed_rs_components_ro(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_hk")){
    val df = convert_fc_feed_rs_components_hk(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_flip_fc")){
    val df = convert_fc_feed_flip_fc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_polyphaser")){
    val df = convert_fc_feed_polyphaser(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_flywing")){
    val df = convert_fc_feed_flywing(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_flyking")){
    val df = convert_fc_feed_flyking(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_component_stockers")){
    val df = convert_fc_feed_component_stockers(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_peigenesis_003_bnl")){
    val df = convert_fc_feed_peigenesis_003_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chip1stop_cny_oems_3")){
    val df = convert_fc_feed_chip1stop_cny_oems_3(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_kruse_fc")){
    val df = convert_fc_feed_kruse_fc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ystchips")){
    val df = convert_fc_feed_ystchips(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_cxda")){
    val df = convert_fc_feed_cxda(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rspro_ie")){
    val df = convert_fc_feed_rspro_ie(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_americas_bnl_rs_pro")){
    val df = convert_fc_feed_rs_americas_bnl_rs_pro(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_dynamic_fc")){
    val df = convert_fc_feed_dynamic_fc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rspro_at")){
    val df = convert_fc_feed_rspro_at(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_comsit_oems_br")){
    val df = convert_fc_feed_comsit_oems_br(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_heilind_asia_oems")){
    val df = convert_fc_feed_heilind_asia_oems(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_jrh")){
    val df = convert_fc_feed_jrh(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_vrgcomp")){
    val df = convert_fc_feed_vrgcomp(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_newark_us")){
    val df = convert_fc_feed_newark_us(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_springtech")){
    val df = convert_fc_feed_springtech(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_cz_findchips")){
    val df = convert_fc_feed_distrelec_cz_findchips(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_waytek")){
    val df = convert_fc_feed_waytek(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_be")){
    val df = convert_fc_feed_farnell_be(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_viczone")){
    val df = convert_fc_feed_viczone(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_be_mft_bnl")){
    val df = convert_fc_feed_rs_components_be_mft_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_aepetsche")){
    val df = convert_fc_feed_aepetsche(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_bitfoic")){
    val df = convert_fc_feed_bitfoic(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_newadvantage_oems")){
    val df = convert_fc_feed_newadvantage_oems(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_aps")){
    val df = convert_fc_feed_aps(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell")){
    val df = convert_fc_feed_farnell(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_westcoast")){
    val df = convert_fc_feed_westcoast(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rspro_hu")){
    val df = convert_fc_feed_rspro_hu(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_microchip_usa")){
    val df = convert_fc_feed_microchip_usa(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_ee")){
    val df = convert_fc_feed_farnell_ee(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_vyrian_fc")){
    val df = convert_fc_feed_vyrian_fc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_samno")){
    val df = convert_fc_feed_samno(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_tme_oems")){
    val df = convert_fc_feed_tme_oems(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_avnet_dp_nmi_bnl")){
    val df = convert_fc_feed_avnet_dp_nmi_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_dbroberts")){
    val df = convert_fc_feed_dbroberts(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chip1stop_jpy_oems_3")){
    val df = convert_fc_feed_chip1stop_jpy_oems_3(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_wbchips")){
    val df = convert_fc_feed_wbchips(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_allied_oemstrade_ex")){
    val df = convert_fc_feed_allied_oemstrade_ex(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_diverse")){
    val df = convert_fc_feed_diverse(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rochester_discounted_bnl")){
    val df = convert_fc_feed_rochester_discounted_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_samtec")){
    val df = convert_fc_feed_samtec(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_briocean")){
    val df = convert_fc_feed_briocean(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_kronex")){
    val df = convert_fc_feed_kronex(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_circular")){
    val df = convert_fc_feed_circular(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_uk")){
    val df = convert_fc_feed_farnell_uk(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_statemotor")){
    val df = convert_fc_feed_statemotor(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_b2b_wpg")){
    val df = convert_fc_feed_b2b_wpg(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_hu_bnl")){
    val df = convert_fc_feed_farnell_hu_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_waytek_feed_bnl")){
    val df = convert_fc_feed_waytek_feed_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_element14_bnl_littelfuse")){
    val df = convert_fc_feed_element14_bnl_littelfuse(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_nacsemi_findchips_bnl")){
    val df = convert_fc_feed_nacsemi_findchips_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chipmall")){
    val df = convert_fc_feed_chipmall(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_suishen_oems")){
    val df = convert_fc_feed_suishen_oems(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_heilind_europe_oems")){
    val df = convert_fc_feed_heilind_europe_oems(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_iconline")){
    val df = convert_fc_feed_iconline(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_assetgreen")){
    val df = convert_fc_feed_assetgreen(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rfpd")){
    val df = convert_fc_feed_rfpd(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_nacsemi_fc_us")){
    val df = convert_fc_feed_nacsemi_fc_us(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_teconn")){
    val df = convert_fc_feed_teconn(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_ph")){
    val df = convert_fc_feed_rs_components_ph(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_dynamic")){
    val df = convert_fc_feed_dynamic(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_element14_nz")){
    val df = convert_fc_feed_element14_nz(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_schukat_fc")){
    val df = convert_fc_feed_schukat_fc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rutronik")){
    val df = convert_fc_feed_rutronik(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_se")){
    val df = convert_fc_feed_rs_components_se(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_comsit_fc_es")){
    val df = convert_fc_feed_comsit_fc_es(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_maritex")){
    val df = convert_fc_feed_maritex(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_southelec")){
    val df = convert_fc_feed_southelec(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_newadvantage_bnl")){
    val df = convert_fc_feed_newadvantage_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_microchip")){
    val df = convert_fc_feed_microchip(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ti_bnl_asc")){
    val df = convert_fc_feed_ti_bnl_asc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_walkerind")){
    val df = convert_fc_feed_walkerind(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_libra")){
    val df = convert_fc_feed_libra(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_sk_oemstrade")){
    val df = convert_fc_feed_distrelec_sk_oemstrade(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_mvcomp")){
    val df = convert_fc_feed_mvcomp(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_arrow_bnl_2")){
    val df = convert_fc_feed_arrow_bnl_2(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_element14")){
    val df = convert_fc_feed_element14(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_shengyu_fc")){
    val df = convert_fc_feed_shengyu_fc(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_peigenesis_003")){
    val df = convert_fc_feed_peigenesis_003(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_terapart")){
    val df = convert_fc_feed_terapart(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_element14_vn")){
    val df = convert_fc_feed_element14_vn(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_jameco")){
    val df = convert_fc_feed_jameco(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_symmetry_oems")){
    val df = convert_fc_feed_symmetry_oems(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec")){
    val df = convert_fc_feed_distrelec(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_gallop")){
    val df = convert_fc_feed_gallop(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_heilind_americas_asia")){
    val df = convert_fc_feed_heilind_americas_asia(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_comsit_oems_mx")){
    val df = convert_fc_feed_comsit_oems_mx(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_ru")){
    val df = convert_fc_feed_rs_components_ru(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_peigenesis_006")){
    val df = convert_fc_feed_peigenesis_006(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_oneyac_fc_global")){
    val df = convert_fc_feed_oneyac_fc_global(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_digikey_hk")){
    val df = convert_fc_feed_digikey_hk(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_acds")){
    val df = convert_fc_feed_acds(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_sk_mft_bnl")){
    val df = convert_fc_feed_rs_components_sk_mft_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_sk_findchips")){
    val df = convert_fc_feed_distrelec_sk_findchips(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_tedss")){
    val df = convert_fc_feed_tedss(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_toponebuy")){
    val df = convert_fc_feed_toponebuy(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_arrow_china")){
    val df = convert_fc_feed_arrow_china(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chipone")){
    val df = convert_fc_feed_chipone(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_glyn")){
    val df = convert_fc_feed_glyn(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_enteermall")){
    val df = convert_fc_feed_enteermall(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rcelec")){
    val df = convert_fc_feed_rcelec(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_utmel")){
    val df = convert_fc_feed_utmel(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_avnet_asia_bnl")){
    val df = convert_fc_feed_avnet_asia_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_inductors")){
    val df = convert_fc_feed_inductors(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_tencell")){
    val df = convert_fc_feed_tencell(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_acmechip_oems")){
    val df = convert_fc_feed_acmechip_oems(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rspro_nz")){
    val df = convert_fc_feed_rspro_nz(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_mps")){
    val df = convert_fc_feed_mps(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_digikey_uk_bnl")){
    val df = convert_fc_feed_digikey_uk_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_era")){
    val df = convert_fc_feed_era(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ameya_fc_cn")){
    val df = convert_fc_feed_ameya_fc_cn(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rochester_kr")){
    val df = convert_fc_feed_rochester_kr(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ukserfala")){
    val df = convert_fc_feed_ukserfala(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_coilcraft_bnl")){
    val df = convert_fc_feed_coilcraft_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_cytech")){
    val df = convert_fc_feed_cytech(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_apvm")){
    val df = convert_fc_feed_apvm(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chip1stop_cny_oems")){
    val df = convert_fc_feed_chip1stop_cny_oems(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_tr")){
    val df = convert_fc_feed_farnell_tr(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_pt")){
    val df = convert_fc_feed_rs_components_pt(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_taprobain")){
    val df = convert_fc_feed_taprobain(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_hu_oemstrade")){
    val df = convert_fc_feed_distrelec_hu_oemstrade(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_comsit_oems_ru")){
    val df = convert_fc_feed_comsit_oems_ru(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_maxim_bnl")){
    val df = convert_fc_feed_rs_components_maxim_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_comsit_fc_fr")){
    val df = convert_fc_feed_comsit_fc_fr(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_allied_findchips_ex")){
    val df = convert_fc_feed_allied_findchips_ex(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_icsole")){
    val df = convert_fc_feed_icsole(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_shortec")){
    val df = convert_fc_feed_shortec(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_cisemiconductors")){
    val df = convert_fc_feed_cisemiconductors(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_il")){
    val df = convert_fc_feed_farnell_il(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_heilind_asia")){
    val df = convert_fc_feed_heilind_asia(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_supertronic")){
    val df = convert_fc_feed_supertronic(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_ro_oemstrade")){
    val df = convert_fc_feed_distrelec_ro_oemstrade(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_heqingelec")){
    val df = convert_fc_feed_heqingelec(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_hsictrading")){
    val df = convert_fc_feed_hsictrading(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_arrow_bnl_pemco")){
    val df = convert_fc_feed_arrow_bnl_pemco(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_goodiclink")){
    val df = convert_fc_feed_goodiclink(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_hsmelect")){
    val df = convert_fc_feed_hsmelect(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ti_bnl")){
    val df = convert_fc_feed_ti_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_smrelec")){
    val df = convert_fc_feed_smrelec(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rochester_jp")){
    val df = convert_fc_feed_rochester_jp(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_winsource_cse")){
    val df = convert_fc_feed_winsource_cse(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_lvy")){
    val df = convert_fc_feed_lvy(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chip1stop_jpy_multi")){
    val df = convert_fc_feed_chip1stop_jpy_multi(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_hu_findchips")){
    val df = convert_fc_feed_distrelec_hu_findchips(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_fanco")){
    val df = convert_fc_feed_fanco(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rs_components_au")){
    val df = convert_fc_feed_rs_components_au(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_eve")){
    val df = convert_fc_feed_eve(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_teltek")){
    val df = convert_fc_feed_teltek(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_technitool")){
    val df = convert_fc_feed_technitool(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ardusimple_oems")){
    val df = convert_fc_feed_ardusimple_oems(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_acmechip")){
    val df = convert_fc_feed_acmechip(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ntd")){
    val df = convert_fc_feed_ntd(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_kailiyuan")){
    val df = convert_fc_feed_kailiyuan(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_handchain")){
    val df = convert_fc_feed_handchain(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_farnell_rohde_bnl_uk")){
    val df = convert_fc_feed_farnell_rohde_bnl_uk(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_conrad")){
    val df = convert_fc_feed_conrad(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chip1stop_jpy")){
    val df = convert_fc_feed_chip1stop_jpy(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_distrelec_nl_oemstrade")){
    val df = convert_fc_feed_distrelec_nl_oemstrade(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rspro_pl")){
    val df = convert_fc_feed_rspro_pl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_olc_oems")){
    val df = convert_fc_feed_olc_oems(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_newark_mx")){
    val df = convert_fc_feed_newark_mx(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_liangxin")){
    val df = convert_fc_feed_liangxin(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_suntronic")){
    val df = convert_fc_feed_suntronic(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_cdi")){
    val df = convert_fc_feed_cdi(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chip1stop_usd_bnl_test")){
    val df = convert_fc_feed_chip1stop_usd_bnl_test(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_element14_hk")){
    val df = convert_fc_feed_element14_hk(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_chip1stop_cny_oems_2")){
    val df = convert_fc_feed_chip1stop_cny_oems_2(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_venkel")){
    val df = convert_fc_feed_venkel(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_smartpioneer")){
    val df = convert_fc_feed_smartpioneer(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_interine")){
    val df = convert_fc_feed_interine(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_ickey_china")){
    val df = convert_fc_feed_ickey_china(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_rochester_mx_bnl")){
    val df = convert_fc_feed_rochester_mx_bnl(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_teconn_bnl_2")){
    val df = convert_fc_feed_teconn_bnl_2(filename)
    df.repartition(1).write.mode("overwrite").parquet(newbasefile)
  }else if (table.equals("fc_feed_techdesign_bnl_winbond")){
    val df = convert_fc_feed_techdesign_bnl_winbond(filename)
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
