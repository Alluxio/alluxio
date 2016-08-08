package alluxio;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


/**
 * Created by shayne on 8/5/16
 */
public class TypedConfiguration {

  static class InnerConfiguration {
    public static volatile String china_bin_china = "0/dirURL/1";
    public static volatile String china_gene_india = "1/dirURL/1";
    public static volatile String china_chaomin_japan = "2/dirURL/1";
    public static volatile String china_pei_korea = "3/dirURL/1";
    public static volatile String china_lei_england = "4/dirURL/1";
    public static volatile String china_jiri_france = "5/dirURL/1";
    public static volatile String china_andrew_thailand = "6/dirURL/1";
    public static volatile String china_amelia_canada = "7/dirURL/1";
    public static volatile String china_bin_brazil = "8/dirURL/1";
    public static volatile String china_gene_malaysia = "9/dirURL/1";
    public static volatile String china_chaomin_vietnam = "10/dirURL/1";
    public static volatile String china_pei_egypt = "11/dirURL/1";
    public static volatile String china_lei_germany = "12/dirURL/1";
    public static volatile String china_jiri_spanish = "13/dirURL/1";
    public static volatile String china_andrew_america = "14/dirURL/1";
    public static volatile String china_amelia_china = "15/dirURL/1";
    public static volatile String india_bin_india = "0/dirURL/2";
    public static volatile String india_gene_japan = "1/dirURL/2";
    public static volatile String india_chaomin_korea = "2/dirURL/2";
    public static volatile String india_pei_england = "3/dirURL/2";
    public static volatile String india_lei_france = "4/dirURL/2";
    public static volatile String india_jiri_thailand = "5/dirURL/2";
    public static volatile String india_andrew_canada = "6/dirURL/2";
    public static volatile String india_amelia_brazil = "7/dirURL/2";
    public static volatile String india_bin_malaysia = "8/dirURL/2";
    public static volatile String india_gene_vietnam = "9/dirURL/2";
    public static volatile String india_chaomin_egypt = "10/dirURL/2";
    public static volatile String india_pei_germany = "11/dirURL/2";
    public static volatile String india_lei_spanish = "12/dirURL/2";
    public static volatile String india_jiri_america = "13/dirURL/2";
    public static volatile String india_andrew_china = "14/dirURL/2";
    public static volatile String india_amelia_india = "15/dirURL/2";
    public static volatile String japan_bin_japan = "0/dirURL/3";
    public static volatile String japan_gene_korea = "1/dirURL/3";
    public static volatile String japan_chaomin_england = "2/dirURL/3";
    public static volatile String japan_pei_france = "3/dirURL/3";
    public static volatile String japan_lei_thailand = "4/dirURL/3";
    public static volatile String japan_jiri_canada = "5/dirURL/3";
    public static volatile String japan_andrew_brazil = "6/dirURL/3";
    public static volatile String japan_amelia_malaysia = "7/dirURL/3";
    public static volatile String japan_bin_vietnam = "8/dirURL/3";
    public static volatile String japan_gene_egypt = "9/dirURL/3";
    public static volatile String japan_chaomin_germany = "10/dirURL/3";
    public static volatile String japan_pei_spanish = "11/dirURL/3";
    public static volatile String japan_lei_america = "12/dirURL/3";
    public static volatile String japan_jiri_china = "13/dirURL/3";
    public static volatile String japan_andrew_india = "14/dirURL/3";
    public static volatile String japan_amelia_japan = "15/dirURL/3";
    public static volatile String korea_bin_korea = "0/dirURL/4";
    public static volatile String korea_gene_england = "1/dirURL/4";
    public static volatile String korea_chaomin_france = "2/dirURL/4";
    public static volatile String korea_pei_thailand = "3/dirURL/4";
    public static volatile String korea_lei_canada = "4/dirURL/4";
    public static volatile String korea_jiri_brazil = "5/dirURL/4";
    public static volatile String korea_andrew_malaysia = "6/dirURL/4";
    public static volatile String korea_amelia_vietnam = "7/dirURL/4";
    public static volatile String korea_bin_egypt = "8/dirURL/4";
    public static volatile String korea_gene_germany = "9/dirURL/4";
    public static volatile String korea_chaomin_spanish = "10/dirURL/4";
    public static volatile String korea_pei_america = "11/dirURL/4";
    public static volatile String korea_lei_china = "12/dirURL/4";
    public static volatile String korea_jiri_india = "13/dirURL/4";
    public static volatile String korea_andrew_japan = "14/dirURL/4";
    public static volatile String korea_amelia_korea = "15/dirURL/4";
    public static volatile String england_bin_england = "0/dirURL/5";
    public static volatile String england_gene_france = "1/dirURL/5";
    public static volatile String england_chaomin_thailand = "2/dirURL/5";
    public static volatile String england_pei_canada = "3/dirURL/5";
    public static volatile String england_lei_brazil = "4/dirURL/5";
    public static volatile String england_jiri_malaysia = "5/dirURL/5";
    public static volatile String england_andrew_vietnam = "6/dirURL/5";
    public static volatile String england_amelia_egypt = "7/dirURL/5";
    public static volatile String england_bin_germany = "8/dirURL/5";
    public static volatile String england_gene_spanish = "9/dirURL/5";
    public static volatile String england_chaomin_america = "10/dirURL/5";
    public static volatile String england_pei_china = "11/dirURL/5";
    public static volatile String england_lei_india = "12/dirURL/5";
    public static volatile String england_jiri_japan = "13/dirURL/5";
    public static volatile String england_andrew_korea = "14/dirURL/5";
    public static volatile String england_amelia_england = "15/dirURL/5";
    public static volatile String france_bin_france = "0/dirURL/6";
    public static volatile String france_gene_thailand = "1/dirURL/6";
    public static volatile String france_chaomin_canada = "2/dirURL/6";
    public static volatile String france_pei_brazil = "3/dirURL/6";
    public static volatile String france_lei_malaysia = "4/dirURL/6";
    public static volatile String france_jiri_vietnam = "5/dirURL/6";
    public static volatile String france_andrew_egypt = "6/dirURL/6";
    public static volatile String france_amelia_germany = "7/dirURL/6";
    public static volatile String france_bin_spanish = "8/dirURL/6";
    public static volatile String france_gene_america = "9/dirURL/6";
    public static volatile String france_chaomin_china = "10/dirURL/6";
    public static volatile String france_pei_india = "11/dirURL/6";
    public static volatile String france_lei_japan = "12/dirURL/6";
    public static volatile String france_jiri_korea = "13/dirURL/6";
    public static volatile String france_andrew_england = "14/dirURL/6";
    public static volatile String france_amelia_france = "15/dirURL/6";
    public static volatile String thailand_bin_thailand = "0/dirURL/7";
    public static volatile String thailand_gene_canada = "1/dirURL/7";
    public static volatile String thailand_chaomin_brazil = "2/dirURL/7";
    public static volatile String thailand_pei_malaysia = "3/dirURL/7";
    public static volatile String thailand_lei_vietnam = "4/dirURL/7";
    public static volatile String thailand_jiri_egypt = "5/dirURL/7";
    public static volatile String thailand_andrew_germany = "6/dirURL/7";
    public static volatile String thailand_amelia_spanish = "7/dirURL/7";
    public static volatile String thailand_bin_america = "8/dirURL/7";
    public static volatile String thailand_gene_china = "9/dirURL/7";
    public static volatile String thailand_chaomin_india = "10/dirURL/7";
    public static volatile String thailand_pei_japan = "11/dirURL/7";
    public static volatile String thailand_lei_korea = "12/dirURL/7";
    public static volatile String thailand_jiri_england = "13/dirURL/7";
    public static volatile String thailand_andrew_france = "14/dirURL/7";
    public static volatile String thailand_amelia_thailand = "15/dirURL/7";
    public static volatile String canada_bin_canada = "0/dirURL/8";
    public static volatile String canada_gene_brazil = "1/dirURL/8";
    public static volatile String canada_chaomin_malaysia = "2/dirURL/8";
    public static volatile String canada_pei_vietnam = "3/dirURL/8";
    public static volatile String canada_lei_egypt = "4/dirURL/8";
    public static volatile String canada_jiri_germany = "5/dirURL/8";
    public static volatile String canada_andrew_spanish = "6/dirURL/8";
    public static volatile String canada_amelia_america = "7/dirURL/8";
    public static volatile String canada_bin_china = "8/dirURL/8";
    public static volatile String canada_gene_india = "9/dirURL/8";
    public static volatile String canada_chaomin_japan = "10/dirURL/8";
    public static volatile String canada_pei_korea = "11/dirURL/8";
    public static volatile String canada_lei_england = "12/dirURL/8";
    public static volatile String canada_jiri_france = "13/dirURL/8";
    public static volatile String canada_andrew_thailand = "14/dirURL/8";
    public static volatile String canada_amelia_canada = "15/dirURL/8";
    public static volatile String brazil_bin_brazil = "0/dirURL/9";
    public static volatile String brazil_gene_malaysia = "1/dirURL/9";
    public static volatile String brazil_chaomin_vietnam = "2/dirURL/9";
    public static volatile String brazil_pei_egypt = "3/dirURL/9";
    public static volatile String brazil_lei_germany = "4/dirURL/9";
    public static volatile String brazil_jiri_spanish = "5/dirURL/9";
    public static volatile String brazil_andrew_america = "6/dirURL/9";
    public static volatile String brazil_amelia_china = "7/dirURL/9";
    public static volatile String brazil_bin_india = "8/dirURL/9";
    public static volatile String brazil_gene_japan = "9/dirURL/9";
    public static volatile String brazil_chaomin_korea = "10/dirURL/9";
    public static volatile String brazil_pei_england = "11/dirURL/9";
    public static volatile String brazil_lei_france = "12/dirURL/9";
    public static volatile String brazil_jiri_thailand = "13/dirURL/9";
    public static volatile String brazil_andrew_canada = "14/dirURL/9";
    public static volatile String brazil_amelia_brazil = "15/dirURL/9";
    public static volatile String malaysia_bin_malaysia = "0/dirURL/10";
    public static volatile String malaysia_gene_vietnam = "1/dirURL/10";
    public static volatile String malaysia_chaomin_egypt = "2/dirURL/10";
    public static volatile String malaysia_pei_germany = "3/dirURL/10";
    public static volatile String malaysia_lei_spanish = "4/dirURL/10";
    public static volatile String malaysia_jiri_america = "5/dirURL/10";
    public static volatile String malaysia_andrew_china = "6/dirURL/10";
    public static volatile String malaysia_amelia_india = "7/dirURL/10";
    public static volatile String malaysia_bin_japan = "8/dirURL/10";
    public static volatile String malaysia_gene_korea = "9/dirURL/10";
    public static volatile String malaysia_chaomin_england = "10/dirURL/10";
    public static volatile String malaysia_pei_france = "11/dirURL/10";
    public static volatile String malaysia_lei_thailand = "12/dirURL/10";
    public static volatile String malaysia_jiri_canada = "13/dirURL/10";
    public static volatile String malaysia_andrew_brazil = "14/dirURL/10";
    public static volatile String malaysia_amelia_malaysia = "15/dirURL/10";
    public static volatile String vietnam_bin_vietnam = "0/dirURL/11";
    public static volatile String vietnam_gene_egypt = "1/dirURL/11";
    public static volatile String vietnam_chaomin_germany = "2/dirURL/11";
    public static volatile String vietnam_pei_spanish = "3/dirURL/11";
    public static volatile String vietnam_lei_america = "4/dirURL/11";
    public static volatile String vietnam_jiri_china = "5/dirURL/11";
    public static volatile String vietnam_andrew_india = "6/dirURL/11";
    public static volatile String vietnam_amelia_japan = "7/dirURL/11";
    public static volatile String vietnam_bin_korea = "8/dirURL/11";
    public static volatile String vietnam_gene_england = "9/dirURL/11";
    public static volatile String vietnam_chaomin_france = "10/dirURL/11";
    public static volatile String vietnam_pei_thailand = "11/dirURL/11";
    public static volatile String vietnam_lei_canada = "12/dirURL/11";
    public static volatile String vietnam_jiri_brazil = "13/dirURL/11";
    public static volatile String vietnam_andrew_malaysia = "14/dirURL/11";
    public static volatile String vietnam_amelia_vietnam = "15/dirURL/11";
    public static volatile String egypt_bin_egypt = "0/dirURL/12";
    public static volatile String egypt_gene_germany = "1/dirURL/12";
    public static volatile String egypt_chaomin_spanish = "2/dirURL/12";
    public static volatile String egypt_pei_america = "3/dirURL/12";
    public static volatile String egypt_lei_china = "4/dirURL/12";
    public static volatile String egypt_jiri_india = "5/dirURL/12";
    public static volatile String egypt_andrew_japan = "6/dirURL/12";
    public static volatile String egypt_amelia_korea = "7/dirURL/12";
    public static volatile String egypt_bin_england = "8/dirURL/12";
    public static volatile String egypt_gene_france = "9/dirURL/12";
    public static volatile String egypt_chaomin_thailand = "10/dirURL/12";
    public static volatile String egypt_pei_canada = "11/dirURL/12";
    public static volatile String egypt_lei_brazil = "12/dirURL/12";
    public static volatile String egypt_jiri_malaysia = "13/dirURL/12";
    public static volatile String egypt_andrew_vietnam = "14/dirURL/12";
    public static volatile String egypt_amelia_egypt = "15/dirURL/12";
    public static volatile String germany_bin_germany = "0/dirURL/13";
    public static volatile String germany_gene_spanish = "1/dirURL/13";
    public static volatile String germany_chaomin_america = "2/dirURL/13";
    public static volatile String germany_pei_china = "3/dirURL/13";
    public static volatile String germany_lei_india = "4/dirURL/13";
    public static volatile String germany_jiri_japan = "5/dirURL/13";
    public static volatile String germany_andrew_korea = "6/dirURL/13";
    public static volatile String germany_amelia_england = "7/dirURL/13";
    public static volatile String germany_bin_france = "8/dirURL/13";
    public static volatile String germany_gene_thailand = "9/dirURL/13";
    public static volatile String germany_chaomin_canada = "10/dirURL/13";
    public static volatile String germany_pei_brazil = "11/dirURL/13";
    public static volatile String germany_lei_malaysia = "12/dirURL/13";
    public static volatile String germany_jiri_vietnam = "13/dirURL/13";
    public static volatile String germany_andrew_egypt = "14/dirURL/13";
    public static volatile String germany_amelia_germany = "15/dirURL/13";
    public static volatile String spanish_bin_spanish = "0/dirURL/14";
    public static volatile String spanish_gene_america = "1/dirURL/14";
    public static volatile String spanish_chaomin_china = "2/dirURL/14";
    public static volatile String spanish_pei_india = "3/dirURL/14";
    public static volatile String spanish_lei_japan = "4/dirURL/14";
    public static volatile String spanish_jiri_korea = "5/dirURL/14";
    public static volatile String spanish_andrew_england = "6/dirURL/14";
    public static volatile String spanish_amelia_france = "7/dirURL/14";
    public static volatile String spanish_bin_thailand = "8/dirURL/14";
    public static volatile String spanish_gene_canada = "9/dirURL/14";
    public static volatile String spanish_chaomin_brazil = "10/dirURL/14";
    public static volatile String spanish_pei_malaysia = "11/dirURL/14";
    public static volatile String spanish_lei_vietnam = "12/dirURL/14";
    public static volatile String spanish_jiri_egypt = "13/dirURL/14";
    public static volatile String spanish_andrew_germany = "14/dirURL/14";
    public static volatile String spanish_amelia_spanish = "15/dirURL/14";
    public static volatile String russia_bin_america = "0/dirURL/15";
    public static volatile String russia_gene_china = "1/dirURL/15";
    public static volatile String russia_chaomin_india = "2/dirURL/15";
    public static volatile String russia_pei_japan = "3/dirURL/15";
    public static volatile String russia_lei_korea = "4/dirURL/15";
    public static volatile String russia_jiri_england = "5/dirURL/15";
    public static volatile String russia_andrew_france = "6/dirURL/15";
    public static volatile String russia_amelia_thailand = "7/dirURL/15";
    public static volatile String russia_bin_canada = "8/dirURL/15";
    public static volatile String russia_gene_brazil = "9/dirURL/15";
    public static volatile String russia_chaomin_malaysia = "10/dirURL/15";
    public static volatile String russia_pei_vietnam = "11/dirURL/15";
    public static volatile String russia_lei_egypt = "12/dirURL/15";
    public static volatile String russia_jiri_germany = "13/dirURL/15";
    public static volatile String russia_andrew_spanish = "14/dirURL/15";
    public static volatile String russia_amelia_america = "15/dirURL/15";
    public static volatile String america_bin_china = "0/dirURL/16";
    public static volatile String america_gene_india = "1/dirURL/16";
    public static volatile String america_chaomin_japan = "2/dirURL/16";
    public static volatile String america_pei_korea = "3/dirURL/16";
    public static volatile String america_lei_england = "4/dirURL/16";
    public static volatile String america_jiri_france = "5/dirURL/16";
    public static volatile String america_andrew_thailand = "6/dirURL/16";
    public static volatile String america_amelia_canada = "7/dirURL/16";
    public static volatile String america_bin_brazil = "8/dirURL/16";
    public static volatile String america_gene_malaysia = "9/dirURL/16";
    public static volatile String america_chaomin_vietnam = "10/dirURL/16";
    public static volatile String america_pei_egypt = "11/dirURL/16";
    public static volatile String america_lei_germany = "12/dirURL/16";
    public static volatile String america_jiri_spanish = "13/dirURL/16";
    public static volatile String america_andrew_america = "14/dirURL/16";
    public static volatile String america_amelia_china = "15/dirURL/16";
  }

  private String dirURL(int i, int id) {
    String path = Integer.toString(id) + "/dirURL/" + Integer.toString(i);
    return path;
  }

  private void putTask(int fileNumber, int id) {
    // long startTime = System_nanoTime();
    for (int k = 1; k <= fileNumber;) {
      InnerConfiguration.brazil_amelia_china = dirURL(k++, id);
      InnerConfiguration.brazil_bin_india = dirURL(k++, id);
      InnerConfiguration.brazil_gene_japan = dirURL(k++, id);
      InnerConfiguration.vietnam_bin_vietnam = dirURL(k++, id);
      InnerConfiguration.vietnam_gene_egypt = dirURL(k++, id);
      InnerConfiguration.vietnam_chaomin_germany = dirURL(k++, id);
      InnerConfiguration.vietnam_pei_spanish = dirURL(k++, id);
      InnerConfiguration.vietnam_lei_america = dirURL(k++, id);
      InnerConfiguration.vietnam_pei_thailand = dirURL(k++, id);
      InnerConfiguration.vietnam_lei_canada = dirURL(k++, id);
      InnerConfiguration.vietnam_jiri_brazil = dirURL(k++, id);
      InnerConfiguration.vietnam_andrew_malaysia = dirURL(k++, id);
      InnerConfiguration.vietnam_amelia_vietnam = dirURL(k++, id);
      InnerConfiguration.egypt_bin_egypt = dirURL(k++, id);
      InnerConfiguration.egypt_gene_germany = dirURL(k++, id);
      InnerConfiguration.egypt_chaomin_spanish = dirURL(k++, id);
      InnerConfiguration.egypt_pei_america = dirURL(k++, id);
      InnerConfiguration.egypt_lei_china = dirURL(k++, id);
      InnerConfiguration.egypt_jiri_india = dirURL(k++, id);
      InnerConfiguration.china_bin_china = dirURL(k++, id);
      InnerConfiguration.china_gene_india = dirURL(k++, id);
      InnerConfiguration.china_chaomin_japan = dirURL(k++, id);
      InnerConfiguration.russia_andrew_france = dirURL(k++, id);
      InnerConfiguration.russia_amelia_thailand = dirURL(k++, id);
      InnerConfiguration.russia_bin_canada = dirURL(k++, id);
      InnerConfiguration.russia_gene_brazil = dirURL(k++, id);
      InnerConfiguration.russia_chaomin_malaysia = dirURL(k++, id);
      InnerConfiguration.russia_pei_vietnam = dirURL(k++, id);
      InnerConfiguration.russia_lei_egypt = dirURL(k++, id);
      InnerConfiguration.russia_jiri_germany = dirURL(k++, id);
      InnerConfiguration.russia_andrew_spanish = dirURL(k++, id);
      InnerConfiguration.russia_amelia_america = dirURL(k++, id);
      InnerConfiguration.america_bin_china = dirURL(k++, id);
      InnerConfiguration.america_gene_india = dirURL(k++, id);
      InnerConfiguration.america_chaomin_japan = dirURL(k++, id);
      InnerConfiguration.america_pei_korea = dirURL(k++, id);
      InnerConfiguration.america_lei_england = dirURL(k++, id);
      InnerConfiguration.america_jiri_france = dirURL(k++, id);
      InnerConfiguration.china_gene_malaysia = dirURL(k++, id);
      InnerConfiguration.china_chaomin_vietnam = dirURL(k++, id);
      InnerConfiguration.china_pei_egypt = dirURL(k++, id);
      InnerConfiguration.china_lei_germany = dirURL(k++, id);
      InnerConfiguration.china_jiri_spanish = dirURL(k++, id);
      InnerConfiguration.china_andrew_america = dirURL(k++, id);
      InnerConfiguration.china_amelia_china = dirURL(k++, id);
      InnerConfiguration.india_bin_india = dirURL(k++, id);
      InnerConfiguration.india_gene_japan = dirURL(k++, id);
      InnerConfiguration.india_chaomin_korea = dirURL(k++, id);
      InnerConfiguration.india_pei_england = dirURL(k++, id);
      InnerConfiguration.korea_lei_china = dirURL(k++, id);
      InnerConfiguration.korea_jiri_india = dirURL(k++, id);
      InnerConfiguration.korea_andrew_japan = dirURL(k++, id);
      InnerConfiguration.korea_amelia_korea = dirURL(k++, id);
      InnerConfiguration.england_bin_england = dirURL(k++, id);
      InnerConfiguration.england_gene_france = dirURL(k++, id);
      InnerConfiguration.england_chaomin_thailand = dirURL(k++, id);
      InnerConfiguration.england_pei_canada = dirURL(k++, id);
      InnerConfiguration.england_lei_brazil = dirURL(k++, id);
      InnerConfiguration.england_jiri_malaysia = dirURL(k++, id);
      InnerConfiguration.england_andrew_vietnam = dirURL(k++, id);
      InnerConfiguration.england_amelia_egypt = dirURL(k++, id);
      InnerConfiguration.india_andrew_china = dirURL(k++, id);
      InnerConfiguration.india_amelia_india = dirURL(k++, id);
      InnerConfiguration.japan_bin_japan = dirURL(k++, id);
      InnerConfiguration.japan_gene_korea = dirURL(k++, id);
      InnerConfiguration.japan_chaomin_england = dirURL(k++, id);
      InnerConfiguration.japan_pei_france = dirURL(k++, id);
      InnerConfiguration.japan_lei_thailand = dirURL(k++, id);
      InnerConfiguration.japan_jiri_canada = dirURL(k++, id);
      InnerConfiguration.japan_andrew_brazil = dirURL(k++, id);
      InnerConfiguration.japan_amelia_malaysia = dirURL(k++, id);
      InnerConfiguration.japan_bin_vietnam = dirURL(k++, id);
      InnerConfiguration.japan_gene_egypt = dirURL(k++, id);
      InnerConfiguration.japan_chaomin_germany = dirURL(k++, id);
      InnerConfiguration.japan_pei_spanish = dirURL(k++, id);
      InnerConfiguration.japan_lei_america = dirURL(k++, id);
      InnerConfiguration.japan_jiri_china = dirURL(k++, id);
      InnerConfiguration.japan_andrew_india = dirURL(k++, id);
      InnerConfiguration.japan_amelia_japan = dirURL(k++, id);
      InnerConfiguration.korea_bin_korea = dirURL(k++, id);
      InnerConfiguration.korea_gene_england = dirURL(k++, id);
      InnerConfiguration.korea_chaomin_france = dirURL(k++, id);
      InnerConfiguration.korea_pei_thailand = dirURL(k++, id);
      InnerConfiguration.korea_lei_canada = dirURL(k++, id);
      InnerConfiguration.korea_jiri_brazil = dirURL(k++, id);
      InnerConfiguration.england_bin_germany = dirURL(k++, id);
      InnerConfiguration.england_gene_spanish = dirURL(k++, id);
      InnerConfiguration.england_chaomin_america = dirURL(k++, id);
      InnerConfiguration.england_pei_china = dirURL(k++, id);
      InnerConfiguration.england_lei_india = dirURL(k++, id);
      InnerConfiguration.england_jiri_japan = dirURL(k++, id);
      InnerConfiguration.england_andrew_korea = dirURL(k++, id);
      InnerConfiguration.england_amelia_england = dirURL(k++, id);
      InnerConfiguration.france_bin_france = dirURL(k++, id);
      InnerConfiguration.france_gene_thailand = dirURL(k++, id);
      InnerConfiguration.france_chaomin_canada = dirURL(k++, id);
      InnerConfiguration.france_pei_brazil = dirURL(k++, id);
      InnerConfiguration.france_lei_malaysia = dirURL(k++, id);
      InnerConfiguration.france_jiri_vietnam = dirURL(k++, id);
      InnerConfiguration.france_andrew_egypt = dirURL(k++, id);
      InnerConfiguration.france_amelia_germany = dirURL(k++, id);
      InnerConfiguration.spanish_jiri_egypt = dirURL(k++, id);
      InnerConfiguration.spanish_andrew_germany = dirURL(k++, id);
      InnerConfiguration.spanish_amelia_spanish = dirURL(k++, id);
      InnerConfiguration.russia_bin_america = dirURL(k++, id);
      InnerConfiguration.russia_gene_china = dirURL(k++, id);
      InnerConfiguration.russia_chaomin_india = dirURL(k++, id);
      InnerConfiguration.russia_pei_japan = dirURL(k++, id);
      InnerConfiguration.russia_lei_korea = dirURL(k++, id);
      InnerConfiguration.russia_jiri_england = dirURL(k++, id);
      InnerConfiguration.america_jiri_spanish = dirURL(k++, id);
      InnerConfiguration.america_andrew_america = dirURL(k++, id);
      InnerConfiguration.america_amelia_china = dirURL(k++, id);
      InnerConfiguration.brazil_chaomin_korea = dirURL(k++, id);
      InnerConfiguration.brazil_pei_england = dirURL(k++, id);
      InnerConfiguration.brazil_lei_france = dirURL(k++, id);
      InnerConfiguration.brazil_jiri_thailand = dirURL(k++, id);
      InnerConfiguration.brazil_andrew_canada = dirURL(k++, id);
      InnerConfiguration.brazil_amelia_brazil = dirURL(k++, id);
      InnerConfiguration.malaysia_bin_malaysia = dirURL(k++, id);
      InnerConfiguration.malaysia_gene_vietnam = dirURL(k++, id);
      InnerConfiguration.malaysia_chaomin_egypt = dirURL(k++, id);
      InnerConfiguration.malaysia_pei_germany = dirURL(k++, id);
      InnerConfiguration.malaysia_lei_spanish = dirURL(k++, id);
      InnerConfiguration.malaysia_jiri_america = dirURL(k++, id);
      InnerConfiguration.malaysia_andrew_china = dirURL(k++, id);
      InnerConfiguration.france_bin_spanish = dirURL(k++, id);
      InnerConfiguration.france_gene_america = dirURL(k++, id);
      InnerConfiguration.france_chaomin_china = dirURL(k++, id);
      InnerConfiguration.france_pei_india = dirURL(k++, id);
      InnerConfiguration.france_lei_japan = dirURL(k++, id);
      InnerConfiguration.france_jiri_korea = dirURL(k++, id);
      InnerConfiguration.france_andrew_england = dirURL(k++, id);
      InnerConfiguration.france_amelia_france = dirURL(k++, id);
      InnerConfiguration.thailand_bin_thailand = dirURL(k++, id);
      InnerConfiguration.thailand_gene_canada = dirURL(k++, id);
      InnerConfiguration.thailand_chaomin_brazil = dirURL(k++, id);
      InnerConfiguration.thailand_pei_malaysia = dirURL(k++, id);
      InnerConfiguration.thailand_lei_vietnam = dirURL(k++, id);
      InnerConfiguration.thailand_jiri_egypt = dirURL(k++, id);
      InnerConfiguration.thailand_andrew_germany = dirURL(k++, id);
      InnerConfiguration.thailand_amelia_spanish = dirURL(k++, id);
      InnerConfiguration.thailand_bin_america = dirURL(k++, id);
      InnerConfiguration.thailand_gene_china = dirURL(k++, id);
      InnerConfiguration.thailand_chaomin_india = dirURL(k++, id);
      InnerConfiguration.thailand_pei_japan = dirURL(k++, id);
      InnerConfiguration.america_andrew_thailand = dirURL(k++, id);
      InnerConfiguration.america_amelia_canada = dirURL(k++, id);
      InnerConfiguration.america_bin_brazil = dirURL(k++, id);
      InnerConfiguration.america_gene_malaysia = dirURL(k++, id);
      InnerConfiguration.america_chaomin_vietnam = dirURL(k++, id);
      InnerConfiguration.america_pei_egypt = dirURL(k++, id);
      InnerConfiguration.america_lei_germany = dirURL(k++, id);
      InnerConfiguration.china_pei_korea = dirURL(k++, id);
      InnerConfiguration.china_lei_england = dirURL(k++, id);
      InnerConfiguration.china_jiri_france = dirURL(k++, id);
      InnerConfiguration.china_andrew_thailand = dirURL(k++, id);
      InnerConfiguration.china_amelia_canada = dirURL(k++, id);
      InnerConfiguration.china_bin_brazil = dirURL(k++, id);
      InnerConfiguration.india_lei_france = dirURL(k++, id);
      InnerConfiguration.india_jiri_thailand = dirURL(k++, id);
      InnerConfiguration.germany_bin_france = dirURL(k++, id);
      InnerConfiguration.germany_gene_thailand = dirURL(k++, id);
      InnerConfiguration.germany_chaomin_canada = dirURL(k++, id);
      InnerConfiguration.germany_pei_brazil = dirURL(k++, id);
      InnerConfiguration.germany_lei_malaysia = dirURL(k++, id);
      InnerConfiguration.germany_jiri_vietnam = dirURL(k++, id);
      InnerConfiguration.germany_andrew_egypt = dirURL(k++, id);
      InnerConfiguration.germany_amelia_germany = dirURL(k++, id);
      InnerConfiguration.spanish_bin_spanish = dirURL(k++, id);
      InnerConfiguration.spanish_gene_america = dirURL(k++, id);
      InnerConfiguration.spanish_chaomin_china = dirURL(k++, id);
      InnerConfiguration.spanish_pei_india = dirURL(k++, id);
      InnerConfiguration.spanish_lei_japan = dirURL(k++, id);
      InnerConfiguration.spanish_jiri_korea = dirURL(k++, id);
      InnerConfiguration.spanish_andrew_england = dirURL(k++, id);
      InnerConfiguration.spanish_amelia_france = dirURL(k++, id);
      InnerConfiguration.india_andrew_canada = dirURL(k++, id);
      InnerConfiguration.india_amelia_brazil = dirURL(k++, id);
      InnerConfiguration.india_bin_malaysia = dirURL(k++, id);
      InnerConfiguration.india_gene_vietnam = dirURL(k++, id);
      InnerConfiguration.india_chaomin_egypt = dirURL(k++, id);
      InnerConfiguration.india_pei_germany = dirURL(k++, id);
      InnerConfiguration.india_lei_spanish = dirURL(k++, id);
      InnerConfiguration.india_jiri_america = dirURL(k++, id);
      InnerConfiguration.korea_andrew_malaysia = dirURL(k++, id);
      InnerConfiguration.korea_amelia_vietnam = dirURL(k++, id);
      InnerConfiguration.korea_bin_egypt = dirURL(k++, id);
      InnerConfiguration.korea_gene_germany = dirURL(k++, id);
      InnerConfiguration.korea_chaomin_spanish = dirURL(k++, id);
      InnerConfiguration.korea_pei_america = dirURL(k++, id);
      InnerConfiguration.thailand_lei_korea = dirURL(k++, id);
      InnerConfiguration.thailand_jiri_england = dirURL(k++, id);
      InnerConfiguration.thailand_andrew_france = dirURL(k++, id);
      InnerConfiguration.thailand_amelia_thailand = dirURL(k++, id);
      InnerConfiguration.canada_bin_canada = dirURL(k++, id);
      InnerConfiguration.canada_gene_brazil = dirURL(k++, id);
      InnerConfiguration.canada_chaomin_malaysia = dirURL(k++, id);
      InnerConfiguration.canada_pei_vietnam = dirURL(k++, id);
      InnerConfiguration.canada_lei_egypt = dirURL(k++, id);
      InnerConfiguration.germany_lei_india = dirURL(k++, id);
      InnerConfiguration.germany_jiri_japan = dirURL(k++, id);
      InnerConfiguration.germany_andrew_korea = dirURL(k++, id);
      InnerConfiguration.germany_amelia_england = dirURL(k++, id);
      InnerConfiguration.spanish_bin_thailand = dirURL(k++, id);
      InnerConfiguration.spanish_gene_canada = dirURL(k++, id);
      InnerConfiguration.spanish_chaomin_brazil = dirURL(k++, id);
      InnerConfiguration.spanish_pei_malaysia = dirURL(k++, id);
      InnerConfiguration.spanish_lei_vietnam = dirURL(k++, id);
      InnerConfiguration.malaysia_amelia_india = dirURL(k++, id);
      InnerConfiguration.malaysia_bin_japan = dirURL(k++, id);
      InnerConfiguration.malaysia_gene_korea = dirURL(k++, id);
      InnerConfiguration.malaysia_chaomin_england = dirURL(k++, id);
      InnerConfiguration.malaysia_pei_france = dirURL(k++, id);
      InnerConfiguration.malaysia_lei_thailand = dirURL(k++, id);
      InnerConfiguration.canada_jiri_germany = dirURL(k++, id);
      InnerConfiguration.canada_andrew_spanish = dirURL(k++, id);
      InnerConfiguration.canada_amelia_america = dirURL(k++, id);
      InnerConfiguration.canada_bin_china = dirURL(k++, id);
      InnerConfiguration.canada_gene_india = dirURL(k++, id);
      InnerConfiguration.vietnam_jiri_china = dirURL(k++, id);
      InnerConfiguration.vietnam_andrew_india = dirURL(k++, id);
      InnerConfiguration.vietnam_amelia_japan = dirURL(k++, id);
      InnerConfiguration.vietnam_bin_korea = dirURL(k++, id);
      InnerConfiguration.vietnam_gene_england = dirURL(k++, id);
      InnerConfiguration.vietnam_chaomin_france = dirURL(k++, id);
      InnerConfiguration.canada_chaomin_japan = dirURL(k++, id);
      InnerConfiguration.canada_pei_korea = dirURL(k++, id);
      InnerConfiguration.canada_lei_england = dirURL(k++, id);
      InnerConfiguration.canada_jiri_france = dirURL(k++, id);
      InnerConfiguration.canada_andrew_thailand = dirURL(k++, id);
      InnerConfiguration.canada_amelia_canada = dirURL(k++, id);
      InnerConfiguration.brazil_bin_brazil = dirURL(k++, id);
      InnerConfiguration.brazil_gene_malaysia = dirURL(k++, id);
      InnerConfiguration.brazil_chaomin_vietnam = dirURL(k++, id);
      InnerConfiguration.brazil_pei_egypt = dirURL(k++, id);
      InnerConfiguration.brazil_lei_germany = dirURL(k++, id);
      InnerConfiguration.brazil_jiri_spanish = dirURL(k++, id);
      InnerConfiguration.brazil_andrew_america = dirURL(k++, id);
      InnerConfiguration.egypt_andrew_japan = dirURL(k++, id);
      InnerConfiguration.egypt_amelia_korea = dirURL(k++, id);
      InnerConfiguration.egypt_bin_england = dirURL(k++, id);
      InnerConfiguration.egypt_gene_france = dirURL(k++, id);
      InnerConfiguration.egypt_chaomin_thailand = dirURL(k++, id);
      InnerConfiguration.egypt_pei_canada = dirURL(k++, id);
      InnerConfiguration.egypt_lei_brazil = dirURL(k++, id);
      InnerConfiguration.egypt_jiri_malaysia = dirURL(k++, id);
      InnerConfiguration.egypt_andrew_vietnam = dirURL(k++, id);
      InnerConfiguration.egypt_amelia_egypt = dirURL(k++, id);
      InnerConfiguration.germany_bin_germany = dirURL(k++, id);
      InnerConfiguration.germany_gene_spanish = dirURL(k++, id);
      InnerConfiguration.germany_chaomin_america = dirURL(k++, id);
      InnerConfiguration.germany_pei_china = dirURL(k++, id);
      InnerConfiguration.malaysia_jiri_canada = dirURL(k++, id);
      InnerConfiguration.malaysia_andrew_brazil = dirURL(k++, id);
      InnerConfiguration.malaysia_amelia_malaysia = dirURL(k++, id);
    }
  }

  private void getTask(int optNumber, int id) {
    String ss;
    for (int k = 1; k <= optNumber; k += 1) {
      ss = InnerConfiguration.india_chaomin_egypt;
      ss = InnerConfiguration.india_pei_germany;
      ss = InnerConfiguration.india_lei_spanish;
      ss = InnerConfiguration.india_jiri_america;
      ss = InnerConfiguration.korea_pei_thailand;
      ss = InnerConfiguration.korea_lei_canada;
      ss = InnerConfiguration.england_andrew_vietnam;
      ss = InnerConfiguration.england_amelia_egypt;
      ss = InnerConfiguration.england_bin_germany;
      ss = InnerConfiguration.england_gene_spanish;
      ss = InnerConfiguration.england_chaomin_america;
      ss = InnerConfiguration.england_pei_china;
      ss = InnerConfiguration.england_lei_india;
      ss = InnerConfiguration.thailand_jiri_egypt;
      ss = InnerConfiguration.thailand_andrew_germany;
      ss = InnerConfiguration.thailand_amelia_spanish;
      ss = InnerConfiguration.thailand_bin_america;
      ss = InnerConfiguration.thailand_gene_china;
      ss = InnerConfiguration.thailand_chaomin_india;
      ss = InnerConfiguration.thailand_pei_japan;
      ss = InnerConfiguration.thailand_lei_korea;
      ss = InnerConfiguration.thailand_jiri_england;
      ss = InnerConfiguration.thailand_andrew_france;
      ss = InnerConfiguration.thailand_amelia_thailand;
      ss = InnerConfiguration.canada_bin_canada;
      ss = InnerConfiguration.canada_gene_brazil;
      ss = InnerConfiguration.canada_chaomin_malaysia;
      ss = InnerConfiguration.canada_pei_vietnam;
      ss = InnerConfiguration.canada_lei_egypt;
      ss = InnerConfiguration.korea_jiri_brazil;
      ss = InnerConfiguration.korea_andrew_malaysia;
      ss = InnerConfiguration.korea_amelia_vietnam;
      ss = InnerConfiguration.korea_bin_egypt;
      ss = InnerConfiguration.korea_gene_germany;
      ss = InnerConfiguration.korea_chaomin_spanish;
      ss = InnerConfiguration.korea_pei_america;
      ss = InnerConfiguration.england_jiri_japan;
      ss = InnerConfiguration.england_andrew_korea;
      ss = InnerConfiguration.england_amelia_england;
      ss = InnerConfiguration.france_bin_france;
      ss = InnerConfiguration.france_gene_thailand;
      ss = InnerConfiguration.france_chaomin_canada;
      ss = InnerConfiguration.france_pei_brazil;
      ss = InnerConfiguration.france_lei_malaysia;
      ss = InnerConfiguration.germany_chaomin_america;
      ss = InnerConfiguration.germany_pei_china;
      ss = InnerConfiguration.germany_lei_india;
      ss = InnerConfiguration.germany_jiri_japan;
      ss = InnerConfiguration.germany_andrew_korea;
      ss = InnerConfiguration.germany_amelia_england;
      ss = InnerConfiguration.germany_bin_france;
      ss = InnerConfiguration.germany_gene_thailand;
      ss = InnerConfiguration.germany_chaomin_canada;
      ss = InnerConfiguration.germany_pei_brazil;
      ss = InnerConfiguration.brazil_pei_england;
      ss = InnerConfiguration.brazil_lei_france;
      ss = InnerConfiguration.brazil_jiri_thailand;
      ss = InnerConfiguration.brazil_andrew_canada;
      ss = InnerConfiguration.brazil_amelia_brazil;
      ss = InnerConfiguration.malaysia_bin_malaysia;
      ss = InnerConfiguration.malaysia_gene_vietnam;
      ss = InnerConfiguration.malaysia_chaomin_egypt;
      ss = InnerConfiguration.malaysia_pei_germany;
      ss = InnerConfiguration.malaysia_lei_spanish;
      ss = InnerConfiguration.malaysia_jiri_america;
      ss = InnerConfiguration.malaysia_andrew_china;
      ss = InnerConfiguration.russia_jiri_england;
      ss = InnerConfiguration.russia_andrew_france;
      ss = InnerConfiguration.russia_amelia_thailand;
      ss = InnerConfiguration.russia_bin_canada;
      ss = InnerConfiguration.russia_gene_brazil;
      ss = InnerConfiguration.america_bin_brazil;
      ss = InnerConfiguration.america_gene_malaysia;
      ss = InnerConfiguration.america_chaomin_vietnam;
      ss = InnerConfiguration.america_pei_egypt;
      ss = InnerConfiguration.america_lei_germany;
      ss = InnerConfiguration.america_jiri_spanish;
      ss = InnerConfiguration.america_andrew_america;
      ss = InnerConfiguration.america_amelia_china;
      ss = InnerConfiguration.germany_lei_malaysia;
      ss = InnerConfiguration.germany_jiri_vietnam;
      ss = InnerConfiguration.germany_andrew_egypt;
      ss = InnerConfiguration.germany_amelia_germany;
      ss = InnerConfiguration.spanish_bin_spanish;
      ss = InnerConfiguration.spanish_gene_america;
      ss = InnerConfiguration.spanish_chaomin_china;
      ss = InnerConfiguration.spanish_pei_india;
      ss = InnerConfiguration.spanish_lei_japan;
      ss = InnerConfiguration.egypt_bin_england;
      ss = InnerConfiguration.egypt_gene_france;
      ss = InnerConfiguration.egypt_chaomin_thailand;
      ss = InnerConfiguration.egypt_pei_canada;
      ss = InnerConfiguration.egypt_lei_brazil;
      ss = InnerConfiguration.egypt_jiri_malaysia;
      ss = InnerConfiguration.egypt_andrew_vietnam;
      ss = InnerConfiguration.egypt_amelia_egypt;
      ss = InnerConfiguration.germany_bin_germany;
      ss = InnerConfiguration.germany_gene_spanish;
      ss = InnerConfiguration.spanish_lei_vietnam;
      ss = InnerConfiguration.spanish_jiri_egypt;
      ss = InnerConfiguration.spanish_andrew_germany;
      ss = InnerConfiguration.spanish_amelia_spanish;
      ss = InnerConfiguration.russia_bin_america;
      ss = InnerConfiguration.russia_gene_china;
      ss = InnerConfiguration.russia_chaomin_india;
      ss = InnerConfiguration.russia_pei_japan;
      ss = InnerConfiguration.russia_lei_korea;
      ss = InnerConfiguration.brazil_andrew_america;
      ss = InnerConfiguration.brazil_amelia_china;
      ss = InnerConfiguration.brazil_bin_india;
      ss = InnerConfiguration.spanish_chaomin_brazil;
      ss = InnerConfiguration.spanish_pei_malaysia;
      ss = InnerConfiguration.france_jiri_vietnam;
      ss = InnerConfiguration.france_andrew_egypt;
      ss = InnerConfiguration.france_amelia_germany;
      ss = InnerConfiguration.france_bin_spanish;
      ss = InnerConfiguration.china_pei_korea;
      ss = InnerConfiguration.china_lei_england;
      ss = InnerConfiguration.india_bin_malaysia;
      ss = InnerConfiguration.india_gene_vietnam;
      ss = InnerConfiguration.japan_gene_egypt;
      ss = InnerConfiguration.japan_chaomin_germany;
      ss = InnerConfiguration.japan_pei_spanish;
      ss = InnerConfiguration.japan_lei_america;
      ss = InnerConfiguration.japan_jiri_china;
      ss = InnerConfiguration.china_jiri_france;
      ss = InnerConfiguration.china_andrew_thailand;
      ss = InnerConfiguration.china_amelia_canada;
      ss = InnerConfiguration.china_bin_brazil;
      ss = InnerConfiguration.china_gene_malaysia;
      ss = InnerConfiguration.france_gene_america;
      ss = InnerConfiguration.france_chaomin_china;
      ss = InnerConfiguration.france_pei_india;
      ss = InnerConfiguration.france_lei_japan;
      ss = InnerConfiguration.france_jiri_korea;
      ss = InnerConfiguration.france_andrew_england;
      ss = InnerConfiguration.france_amelia_france;
      ss = InnerConfiguration.thailand_bin_thailand;
      ss = InnerConfiguration.thailand_gene_canada;
      ss = InnerConfiguration.thailand_chaomin_brazil;
      ss = InnerConfiguration.thailand_pei_malaysia;
      ss = InnerConfiguration.thailand_lei_vietnam;
      ss = InnerConfiguration.korea_lei_china;
      ss = InnerConfiguration.korea_jiri_india;
      ss = InnerConfiguration.korea_andrew_japan;
      ss = InnerConfiguration.korea_amelia_korea;
      ss = InnerConfiguration.england_bin_england;
      ss = InnerConfiguration.england_gene_france;
      ss = InnerConfiguration.england_chaomin_thailand;
      ss = InnerConfiguration.england_pei_canada;
      ss = InnerConfiguration.england_lei_brazil;
      ss = InnerConfiguration.england_jiri_malaysia;
      ss = InnerConfiguration.canada_jiri_germany;
      ss = InnerConfiguration.canada_andrew_spanish;
      ss = InnerConfiguration.canada_amelia_america;
      ss = InnerConfiguration.canada_bin_china;
      ss = InnerConfiguration.canada_gene_india;
      ss = InnerConfiguration.canada_chaomin_japan;
      ss = InnerConfiguration.russia_chaomin_malaysia;
      ss = InnerConfiguration.russia_pei_vietnam;
      ss = InnerConfiguration.russia_lei_egypt;
      ss = InnerConfiguration.russia_jiri_germany;
      ss = InnerConfiguration.russia_andrew_spanish;
      ss = InnerConfiguration.russia_amelia_america;
      ss = InnerConfiguration.america_bin_china;
      ss = InnerConfiguration.america_gene_india;
      ss = InnerConfiguration.america_chaomin_japan;
      ss = InnerConfiguration.america_pei_korea;
      ss = InnerConfiguration.america_lei_england;
      ss = InnerConfiguration.america_jiri_france;
      ss = InnerConfiguration.america_andrew_thailand;
      ss = InnerConfiguration.america_amelia_canada;
      ss = InnerConfiguration.canada_pei_korea;
      ss = InnerConfiguration.canada_lei_england;
      ss = InnerConfiguration.canada_jiri_france;
      ss = InnerConfiguration.canada_andrew_thailand;
      ss = InnerConfiguration.canada_amelia_canada;
      ss = InnerConfiguration.brazil_bin_brazil;
      ss = InnerConfiguration.brazil_gene_malaysia;
      ss = InnerConfiguration.brazil_chaomin_vietnam;
      ss = InnerConfiguration.brazil_pei_egypt;
      ss = InnerConfiguration.brazil_lei_germany;
      ss = InnerConfiguration.brazil_jiri_spanish;
      ss = InnerConfiguration.malaysia_amelia_india;
      ss = InnerConfiguration.malaysia_bin_japan;
      ss = InnerConfiguration.malaysia_gene_korea;
      ss = InnerConfiguration.malaysia_chaomin_england;
      ss = InnerConfiguration.malaysia_pei_france;
      ss = InnerConfiguration.malaysia_lei_thailand;
      ss = InnerConfiguration.malaysia_jiri_canada;
      ss = InnerConfiguration.malaysia_andrew_brazil;
      ss = InnerConfiguration.malaysia_amelia_malaysia;
      ss = InnerConfiguration.vietnam_bin_vietnam;
      ss = InnerConfiguration.vietnam_gene_egypt;
      ss = InnerConfiguration.vietnam_chaomin_germany;
      ss = InnerConfiguration.vietnam_pei_spanish;
      ss = InnerConfiguration.vietnam_lei_america;
      ss = InnerConfiguration.vietnam_jiri_china;
      ss = InnerConfiguration.vietnam_andrew_india;
      ss = InnerConfiguration.vietnam_amelia_japan;
      ss = InnerConfiguration.vietnam_bin_korea;
      ss = InnerConfiguration.spanish_jiri_korea;
      ss = InnerConfiguration.spanish_andrew_england;
      ss = InnerConfiguration.spanish_amelia_france;
      ss = InnerConfiguration.spanish_bin_thailand;
      ss = InnerConfiguration.spanish_gene_canada;
      ss = InnerConfiguration.china_lei_germany;
      ss = InnerConfiguration.china_jiri_spanish;
      ss = InnerConfiguration.china_andrew_america;
      ss = InnerConfiguration.china_amelia_china;
      ss = InnerConfiguration.india_bin_india;
      ss = InnerConfiguration.india_gene_japan;
      ss = InnerConfiguration.india_chaomin_korea;
      ss = InnerConfiguration.india_pei_england;
      ss = InnerConfiguration.india_lei_france;
      ss = InnerConfiguration.india_jiri_thailand;
      ss = InnerConfiguration.india_andrew_canada;
      ss = InnerConfiguration.india_amelia_brazil;
      ss = InnerConfiguration.japan_andrew_india;
      ss = InnerConfiguration.japan_amelia_japan;
      ss = InnerConfiguration.korea_bin_korea;
      ss = InnerConfiguration.korea_gene_england;
      ss = InnerConfiguration.korea_chaomin_france;
      ss = InnerConfiguration.egypt_jiri_india;
      ss = InnerConfiguration.egypt_andrew_japan;
      ss = InnerConfiguration.egypt_amelia_korea;
      ss = InnerConfiguration.vietnam_gene_england;
      ss = InnerConfiguration.vietnam_chaomin_france;
      ss = InnerConfiguration.vietnam_pei_thailand;
      ss = InnerConfiguration.vietnam_lei_canada;
      ss = InnerConfiguration.vietnam_jiri_brazil;
      ss = InnerConfiguration.vietnam_andrew_malaysia;
      ss = InnerConfiguration.vietnam_amelia_vietnam;
      ss = InnerConfiguration.egypt_bin_egypt;
      ss = InnerConfiguration.egypt_gene_germany;
      ss = InnerConfiguration.egypt_chaomin_spanish;
      ss = InnerConfiguration.egypt_pei_america;
      ss = InnerConfiguration.egypt_lei_china;
      ss = InnerConfiguration.india_andrew_china;
      ss = InnerConfiguration.india_amelia_india;
      ss = InnerConfiguration.japan_bin_japan;
      ss = InnerConfiguration.japan_gene_korea;
      ss = InnerConfiguration.japan_chaomin_england;
      ss = InnerConfiguration.japan_pei_france;
      ss = InnerConfiguration.japan_lei_thailand;
      ss = InnerConfiguration.japan_jiri_canada;
      ss = InnerConfiguration.japan_andrew_brazil;
      ss = InnerConfiguration.japan_amelia_malaysia;
      ss = InnerConfiguration.japan_bin_vietnam;
      ss = InnerConfiguration.china_bin_china;
      ss = InnerConfiguration.china_gene_india;
      ss = InnerConfiguration.china_chaomin_japan;
      ss = InnerConfiguration.china_chaomin_vietnam;
      ss = InnerConfiguration.china_pei_egypt;
      ss = InnerConfiguration.brazil_gene_japan;
      ss = InnerConfiguration.brazil_chaomin_korea;
    }
  }



  /**
   * Creates a new Alluxio file system, generating empty files in the root directory with random
   * name_
   */
  public class PutThread implements Callable<Long> {
    private final int mOptNumber;
    private final int mId;
    private CyclicBarrier mBarrier;

    /**
     * Creates a new Alluxio file system, generating empty files in the root directory with random
     * name_
     *
     * @param optNumber the object to remove from index
     * @param id the object to remove from index
     */
    public PutThread(int optNumber, int id, CyclicBarrier barrier) {
      mOptNumber = optNumber;
      mId = id;
      mBarrier = barrier;
    }

    @Override
    public Long call() throws Exception {
      mBarrier.await();
      long startTime = System.currentTimeMillis();
      putTask(mOptNumber, mId);
      return System.currentTimeMillis() - startTime;
    }
  }

  public class GetThread implements Callable<Long> {
    private final int mOptNumber;
    private final int mId;
    private CyclicBarrier mBarrier;

    /**
     * Creates a new Alluxio file system, generating empty files in the root directory with random
     * name_
     *
     * @param optNumber the object to remove from index
     * @param id the object to remove from index
     */
    public GetThread(int optNumber, int id, CyclicBarrier barrier) {
      mOptNumber =  optNumber / 256 * OptPGRatios;
      mId = id;
      mBarrier = barrier;
    }

    @Override
    public Long call() throws Exception {
      mBarrier.await();
      long startTime = System.currentTimeMillis();
      getTask(mOptNumber, mId);
      return System.currentTimeMillis() - startTime;
    }
  }

  public static void mixedLoad(int optNumber, int threadNumber, int pgRatio)
      throws ExecutionException, InterruptedException {
    TypedConfiguration mapBenchmark = new TypedConfiguration();
    List<Future<Long>> getFutures = new ArrayList<>();
    List<Future<Long>> putFutures = new ArrayList<>();
    List<Future<Long>> prepareFutures = new ArrayList<>();
    ExecutorService pool = Executors.newFixedThreadPool(threadNumber * (1 + pgRatio));

    System.out.printf("--- mixedLoad, %d-thread, p:g = 1:%d -----%n ", threadNumber, pgRatio);

    // prepare data
    CyclicBarrier barrier = new CyclicBarrier(1);
    for (int i = 0; i < threadNumber * pgRatio; i++) {
      TypedConfiguration.PutThread
          putThread = mapBenchmark.new PutThread(optNumber / threadNumber, i, barrier);
      prepareFutures.add(pool.submit(putThread));
    }

    for (Future<Long> future : prepareFutures) {
      future.get();
    }
    pool.shutdown();

    // start
    pool = Executors.newFixedThreadPool(threadNumber * (1 + pgRatio));
    barrier = new CyclicBarrier(threadNumber * (1 + pgRatio));

    for (int i = 0; i < threadNumber; i++) {
      TypedConfiguration.PutThread
          putThread = mapBenchmark.new PutThread(optNumber / threadNumber, i, barrier);
      putFutures.add(pool.submit(putThread));
    }

    for (int i = 0; i < threadNumber * pgRatio; i++) {
      TypedConfiguration.GetThread
          getThread = mapBenchmark.new GetThread(optNumber / threadNumber, i, barrier);
      getFutures.add(pool.submit(getThread));
    }

    Long timePut = 0l;
    for (Future<Long> future : putFutures) {
      Long tt = future.get();
      timePut += tt;
    }
    System.out.printf("put throghput %d%n", optNumber / timePut);

    Long timeGet = 0l;
    for (Future<Long> future : getFutures) {
      Long tt = future.get();
      timeGet += tt;
    }
    System.out.printf("get throghput %d%n", optNumber * pgRatio / timeGet * OptPGRatios);

    pool.shutdown();
  }

  public static void separatedLoad(int optNumber, int threadNumber)
      throws ExecutionException, InterruptedException {
    TypedConfiguration mapBenchmark = new TypedConfiguration();
    List<Future<Long>> getFutures = new ArrayList<>();
    List<Future<Long>> putFutures = new ArrayList<>();
    ExecutorService pool = Executors.newFixedThreadPool(2 * threadNumber);

    System.out.printf("--- separatedLoad, %d-thread-----%n ", threadNumber);

    CyclicBarrier barrier = new CyclicBarrier(threadNumber);
    for (int i = 0; i < threadNumber; i++) {
      TypedConfiguration.PutThread
          putThread = mapBenchmark.new PutThread(optNumber / threadNumber, i, barrier);
      putFutures.add(pool.submit(putThread));
    }

    Long timePut = 0l;
    for (Future<Long> future : putFutures) {
      Long tt = future.get();
      timePut += tt;
    }
    System.out.printf("put throghput %d%n", optNumber / timePut);

    barrier = new CyclicBarrier(threadNumber);
    for (int i = 0; i < threadNumber; i++) {
      TypedConfiguration.GetThread
          getThread = mapBenchmark.new GetThread(optNumber / threadNumber, i, barrier);
      getFutures.add(pool.submit(getThread));
    }

    Long timeGet = 0l;
    for (Future<Long> future : getFutures) {
      Long tt = future.get();
      timeGet += tt;
    }
    System.out.printf("get throghput %d%n", optNumber / timeGet * OptPGRatios);

    pool.shutdown();
  }


  private static final int OptNumber = 80000000;
  private static final int OptPGRatios = 256;
  @Test
  public void benchMark() throws ExecutionException, InterruptedException {
    int optNumber = OptNumber;
    int[] threadNumbers = {1, 4, 8, 16};
    int[] pgRatios = {2, 5};

    for (int threadNumber : threadNumbers) {
      separatedLoad(optNumber, threadNumber);
      for (int pgRatio : pgRatios) {
        mixedLoad(optNumber, threadNumber, pgRatio);
      }
    }

  }

}
