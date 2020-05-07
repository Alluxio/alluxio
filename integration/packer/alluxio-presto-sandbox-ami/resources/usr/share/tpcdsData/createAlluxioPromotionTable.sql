create database if not exists alluxio;
use alluxio;  
drop table if exists promotion;
create external table if not exists promotion(
      p_promo_sk bigInt
,     p_promo_id string
,     p_start_date_sk bigInt
,     p_end_date_sk bigInt
,     p_item_sk bigInt
,     p_cost double
,     p_response_target bigInt
,     p_promo_name string
,     p_channel_dmail string
,     p_channel_email string
,     p_channel_catalog string
,     p_channel_tv string
,     p_channel_radio string
,     p_channel_press string
,     p_channel_event string
,     p_channel_demo string
,     p_channel_details string
,     p_purpose string
,     p_discount_active string
)
stored as parquet
location 'alluxio://localhost:19998/promotion'
tblproperties (
  'parquet.compression'='SNAPPY')
;

