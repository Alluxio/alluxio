create database if not exists alluxio;
use alluxio;
drop table if exists store_sales;
create external table if not exists store_sales(
      ss_sold_date_sk bigint
,     ss_sold_time_sk bigint
,     ss_item_sk bigint
,     ss_customer_sk bigint
,     ss_cdemo_sk bigint
,     ss_hdemo_sk bigint
,     ss_addr_sk bigint
,     ss_store_sk bigint
,     ss_promo_sk bigint
,     ss_ticket_number bigint
,     ss_quantity int
,     ss_wholesale_cost double
,     ss_list_price double
,     ss_sales_price double
,     ss_ext_discount_amt double
,     ss_ext_sales_price double
,     ss_ext_wholesale_cost double
,     ss_ext_list_price double
,     ss_ext_tax double
,     ss_coupon_amt double
,     ss_net_paid double
,     ss_net_paid_inc_tax double
,     ss_net_profit double
)
stored as parquet
location 'alluxio:///scale1/store_sales'
tblproperties (
  'parquet.compression'='SNAPPY')
;

msck repair table store_sales;
select * from store_sales limit 10;

drop table if exists store_returns;
create external table if not exists store_returns(
      sr_returned_date_sk bigint
,     sr_return_time_sk bigint
,     sr_item_sk bigint
,     sr_customer_sk bigint
,     sr_cdemo_sk bigint
,     sr_hdemo_sk bigint
,     sr_addr_sk bigint
,     sr_store_sk bigint
,     sr_reason_sk bigint
,     sr_ticket_number bigint
,     sr_return_quantity int
,     sr_return_amt double
,     sr_return_tax double
,     sr_return_amt_inc_tax double
,     sr_fee double
,     sr_return_ship_cost double
,     sr_refunded_cash double
,     sr_reversed_charge double
,     sr_store_credit double
,     sr_net_loss double
)
stored as parquet
location 'alluxio:///scale1/store_returns'
tblproperties (
  'parquet.compression'='SNAPPY')
;

msck repair table store_returns;
select * from store_returns limit 10;

drop table if exists catalog_sales;
create external table if not exists catalog_sales(
      cs_sold_date_sk bigint
,     cs_sold_time_sk bigint
,     cs_ship_date_sk bigint
,     cs_bill_customer_sk bigint
,     cs_bill_cdemo_sk bigint
,     cs_bill_hdemo_sk bigint
,     cs_bill_addr_sk bigint
,     cs_ship_customer_sk bigint
,     cs_ship_cdemo_sk bigint
,     cs_ship_hdemo_sk bigint
,     cs_ship_addr_sk bigint
,     cs_call_center_sk bigint
,     cs_catalog_page_sk bigint
,     cs_ship_mode_sk bigint
,     cs_warehouse_sk bigint
,     cs_item_sk bigint
,     cs_promo_sk bigint
,     cs_order_number bigint
,     cs_quantity int
,     cs_wholesale_cost double
,     cs_list_price double
,     cs_sales_price double
,     cs_ext_discount_amt double
,     cs_ext_sales_price double
,     cs_ext_wholesale_cost double
,     cs_ext_list_price double
,     cs_ext_tax double
,     cs_coupon_amt double
,     cs_ext_ship_cost double
,     cs_net_paid double
,     cs_net_paid_inc_tax double
,     cs_net_paid_inc_ship double
,     cs_net_paid_inc_ship_tax double
,     cs_net_profit double
)
stored as parquet
location 'alluxio:///scale1/catalog_sales'
tblproperties (
  'parquet.compression'='SNAPPY')
;

msck repair table catalog_sales;
select * from catalog_sales limit 10;

drop table if exists catalog_returns;
create external table if not exists catalog_returns(
      cr_returned_date_sk bigint
,     cr_returned_time_sk bigint
,     cr_item_sk bigint
,     cr_refunded_customer_sk bigint
,     cr_refunded_cdemo_sk bigint
,     cr_refunded_hdemo_sk bigint
,     cr_refunded_addr_sk bigint
,     cr_returning_customer_sk bigint
,     cr_returning_cdemo_sk bigint
,     cr_returning_hdemo_sk bigint
,     cr_returning_addr_sk bigint
,     cr_call_center_sk bigint
,     cr_catalog_page_sk bigint
,     cr_ship_mode_sk bigint
,     cr_warehouse_sk bigint
,     cr_reason_sk bigint
,     cr_order_number bigint
,     cr_return_quantity int
,     cr_return_amount double
,     cr_return_tax double
,     cr_return_amt_inc_tax double
,     cr_fee double
,     cr_return_ship_cost double
,     cr_refunded_cash double
,     cr_reversed_charge double
,     cr_store_credit double
,     cr_net_loss double
)
stored as parquet
location 'alluxio:///scale1/catalog_returns'
tblproperties (
  'parquet.compression'='SNAPPY')
;

msck repair table catalog_returns;
select * from catalog_returns limit 10;

drop table if exists web_sales;
create external table if not exists web_sales(
      ws_sold_date_sk bigInt
,     ws_sold_time_sk bigInt
,     ws_ship_date_sk bigInt
,     ws_item_sk bigInt
,     ws_bill_customer_sk bigInt
,     ws_bill_cdemo_sk bigInt
,     ws_bill_hdemo_sk bigInt
,     ws_bill_addr_sk bigInt
,     ws_ship_customer_sk bigInt
,     ws_ship_cdemo_sk bigInt
,     ws_ship_hdemo_sk bigInt
,     ws_ship_addr_sk bigInt
,     ws_web_page_sk bigInt
,     ws_web_site_sk bigInt
,     ws_ship_mode_sk bigInt
,     ws_warehouse_sk bigInt
,     ws_promo_sk bigInt
,     ws_order_number bigInt
,     ws_quantity int
,     ws_wholesale_cost double
,     ws_list_price double
,     ws_sales_price double
,     ws_ext_discount_amt double
,     ws_ext_sales_price double
,     ws_ext_wholesale_cost double
,     ws_ext_list_price double
,     ws_ext_tax double
,     ws_coupon_amt double
,     ws_ext_ship_cost double
,     ws_net_paid double
,     ws_net_paid_inc_tax double
,     ws_net_paid_inc_ship double
,     ws_net_paid_inc_ship_tax double
,     ws_net_profit double
)
stored as parquet
location 'alluxio:///scale1/web_sales'
tblproperties (
  'parquet.compression'='SNAPPY')
;

msck repair table web_sales;
select * from web_sales limit 10;

drop table if exists web_returns;
create external table if not exists web_returns(
      wr_returned_date_sk bigInt
,     wr_returned_time_sk bigInt
,     wr_item_sk bigInt
,     wr_refunded_customer_sk bigInt
,     wr_refunded_cdemo_sk bigInt
,     wr_refunded_hdemo_sk bigInt
,     wr_refunded_addr_sk bigInt
,     wr_returning_customer_sk bigInt
,     wr_returning_cdemo_sk bigInt
,     wr_returning_hdemo_sk bigInt
,     wr_returning_addr_sk bigInt
,     wr_web_page_sk bigInt
,     wr_reason_sk bigInt
,     wr_order_number bigInt
,     wr_return_quantity int
,     wr_return_amt double
,     wr_return_tax double
,     wr_return_amt_inc_tax double
,     wr_fee double
,     wr_return_ship_cost double
,     wr_refunded_cash double
,     wr_reversed_charge double
,     wr_account_credit double
,     wr_net_loss double
)
stored as parquet
location 'alluxio:///scale1/web_returns'
tblproperties (
  'parquet.compression'='SNAPPY')
;

msck repair table web_returns;
select * from web_returns limit 10;


drop table if exists inventory;
create external table if not exists inventory(
      inv_date_sk bigInt
,     inv_item_sk bigInt
,     inv_warehouse_sk bigInt
,     inv_quantity_on_hand int
)
stored as parquet
location 'alluxio:///scale1/inventory'
tblproperties (
  'parquet.compression'='SNAPPY')
;

msck repair table inventory;
select * from inventory limit 10;

drop table if exists store;
create external table if not exists store(
      s_store_sk bigInt
,     s_store_id string
,     s_rec_start_date string
,     s_rec_end_date string
,     s_closed_date_sk bigInt
,     s_store_name string
,     s_number_employees int
,     s_floor_space int
,     s_hours string
,     S_manager string
,     S_market_id bigInt
,     S_geography_class string
,     S_market_desc string
,     s_market_manager string
,     s_division_id bigInt
,     s_division_name string
,     s_company_id bigInt
,     s_company_name string
,     s_street_number string
,     s_street_name string
,     s_street_type string
,     s_suite_number string
,     s_city string
,     s_county string
,     s_state string
,     s_zip string
,     s_country string
,     s_gmt_offset double
,     s_tax_percentage double
)
stored as parquet
location 'alluxio:///scale1/store'
tblproperties (
  'parquet.compression'='SNAPPY')
;

select * from store limit 10;

drop table if exists call_center;
create external table if not exists call_center(
      cc_call_center_sk bigInt
,     cc_call_center_id string
,     cc_rec_start_date string
,     cc_rec_end_date string
,     cc_closed_date_sk bigInt
,     cc_open_date_sk bigInt
,     cc_name string
,     cc_class string
,     cc_employees int
,     cc_sq_ft int
,     cc_hours string
,     cc_manager string
,     cc_mkt_id bigInt
,     cc_mkt_class string
,     cc_mkt_desc string
,     cc_market_manager string
,     cc_division bigInt
,     cc_division_name string
,     cc_company bigInt
,     cc_company_name string
,     cc_street_number string
,     cc_street_name string
,     cc_street_type string
,     cc_suite_number string
,     cc_city string
,     cc_county string
,     cc_state string
,     cc_zip string
,     cc_country string
,     cc_gmt_offset double
,     cc_tax_percentage double
)
stored as parquet
location 'alluxio:///scale1/call_center'
tblproperties (
  'parquet.compression'='SNAPPY')
;

select * from call_center limit 10;

drop table if exists catalog_page;
create external table if not exists catalog_page(
      cp_catalog_page_sk bigint
,     cp_catalog_page_id string
,     cp_start_date_sk bigint
,     cp_end_date_sk bigint
,     cp_department string
,     cp_catalog_number bigint
,     cp_catalog_page_number bigint
,     cp_description string
,     cp_type string
)
stored as parquet
location 'alluxio:///scale1/catalog_page'
tblproperties (
  'parquet.compression'='SNAPPY')
;

select * from  catalog_page limit 10;

drop table if exists web_site;
create external table if not exists web_site(
      web_site_sk bigInt
,     web_site_id string
,     web_rec_start_date string
,     web_rec_end_date string
,     web_name string
,     web_open_date_sk bigInt
,     web_close_date_sk bigInt
,     web_class string
,     web_manager string
,     web_mkt_id bigInt
,     web_mkt_class string
,     web_mkt_desc string
,     web_market_manager string
,     web_company_id bigInt
,     web_company_name string
,     web_street_number string
,     web_street_name string
,     web_street_type string
,     web_suite_number string
,     web_city string
,     web_county string
,     web_state string
,     web_zip string
,     web_country string
,     web_gmt_offset double
,     web_tax_percentage double
)
stored as parquet
location 'alluxio:///scale1/web_site'
tblproperties (
  'parquet.compression'='SNAPPY')
;

select * from  web_site limit 10;

drop table if exists web_page;
create external table if not exists web_page(
      wp_web_page_sk bigInt
,     wp_web_page_id string
,     wp_rec_start_date string
,     wp_rec_end_date string
,     wp_creation_date_sk bigInt
,     wp_access_date_sk bigInt
,     wp_autogen_flag string
,     wp_customer_sk bigInt
,     wp_url string
,     wp_type string
,     wp_char_count bigInt
,     wp_link_count int
,     wp_image_count int
,     wp_max_ad_count int
)
stored as parquet
location 'alluxio:///scale1/web_page'
tblproperties (
  'parquet.compression'='SNAPPY')
;

select * from web_page limit 10;

drop table if exists warehouse;
create external table if not exists warehouse(
      w_warehouse_sk bigInt
,     w_warehouse_id string
,     w_warehouse_name string
,     w_warehouse_sq_ft int
,     w_street_number string
,     w_street_name string
,     w_street_type string
,     w_suite_number string
,     w_city string
,     w_county string
,     w_state string
,     w_zip string
,     w_country string
,     w_gmt_offset double
)
stored as parquet
location 'alluxio:///scale1/warehouse'
tblproperties (
  'parquet.compression'='SNAPPY')
;

select * from warehouse limit 10;

drop table if exists customer;
create external table if not exists customer(
      c_customer_sk bigInt
,     c_customer_id string
,     c_current_cdemo_sk bigInt
,     c_current_hdemo_sk bigInt
,     c_current_addr_sk bigInt
,     c_first_shipto_date_sk bigInt
,     c_first_sales_date_sk bigInt
,     c_salutation string
,     c_first_name string
,     c_last_name string
,     c_preferred_cust_flag string
,     c_birth_day int
,     c_birth_month int
,     c_birth_year int
,     c_birth_country string
,     c_login string
,     c_email_address string
,     c_last_review_date_sk bigInt
)
stored as parquet
location 'alluxio:///scale1/customer'
tblproperties (
  'parquet.compression'='SNAPPY')
;

select * from customer limit 10;

drop table if exists customer_address;
create external table if not exists customer_address(
      ca_address_sk bigInt
,     ca_address_id string
,     ca_street_number string
,     ca_street_name string
,     ca_street_type string
,     ca_suite_number string
,     ca_city string
,     ca_county string
,     ca_state string
,     ca_zip string
,     ca_country string
,     ca_gmt_offset double
,     ca_location_type string
)
stored as parquet
location 'alluxio:///scale1/customer_address'
tblproperties (
  'parquet.compression'='SNAPPY')
;

select * from customer_address limit 10;

drop table if exists customer_demographics;
create external table if not exists customer_demographics(
      cd_demo_sk bigInt
,     cd_gender string
,     cd_marital_status string
,     cd_education_status string
,     cd_purchase_estimate bigInt
,     cd_credit_rating string
,     cd_dep_count int
,     cd_dep_employed_count int
,     cd_dep_college_count int
)
stored as parquet
location 'alluxio:///scale1/customer_demographics'
tblproperties (
  'parquet.compression'='SNAPPY')
;

select * from customer_demographics limit 10;

drop table if exists date_dim;
create external table if not exists date_dim(
      d_date_sk bigInt
,     d_date_id string
,     d_date string
,     d_month_seq bigInt
,     d_week_seq bigInt
,     d_quarter_seq bigInt
,     d_year bigInt
,     d_dow bigInt
,     d_moy bigInt
,     d_dom bigInt
,     d_qoy bigInt
,     d_fy_year bigInt
,     d_fy_quarter_seq bigInt
,     d_fy_week_seq bigInt
,     d_day_name string
,     d_quarter_name string
,     d_holiday string
,     d_weekend string
,     d_following_holiday string
,     d_first_dom bigInt
,     d_last_dom bigInt
,     d_same_day_ly bigInt
,     d_same_day_lq bigInt
,     d_current_day string
,     d_current_week string
,     d_current_month string
,     d_current_quarter string
,     d_current_year string
)
stored as parquet
location 'alluxio:///scale1/date_dim'
tblproperties (
  'parquet.compression'='SNAPPY')
;

select * from date_dim limit 10;

drop table if exists household_demographics;
create external table if not exists household_demographics(
      hd_demo_sk bigInt
,     hd_income_band_sk bigInt
,     hd_buy_potential string
,     hd_dep_count int
,     hd_vehicle_count int
)
stored as parquet
location 'alluxio:///scale1/household_demographics'
tblproperties (
  'parquet.compression'='SNAPPY')
;

select * from household_demographics limit 10;

drop table if exists item;
create external table if not exists item(
      i_item_sk bigInt
,     i_item_id string
,     i_rec_start_date string
,     i_rec_end_date string
,     i_item_desc string
,     i_current_price double
,     i_wholesale_cost double
,     i_brand_id bigInt
,     i_brand string
,     i_class_id bigInt
,     i_class string
,     i_category_id bigInt
,     i_category string
,     i_manufact_id bigInt
,     i_manufact string
,     i_size string
,     i_formulation string
,     i_color string
,     i_units string
,     i_container string
,     i_manager_id bigInt
,     i_product_name string
)
stored as parquet
location 'alluxio:///scale1/item'
tblproperties (
  'parquet.compression'='SNAPPY')
;

select * from item limit 10;

drop table if exists income_band;
create external table if not exists income_band(
      ib_income_band_sk bigInt
,     ib_lower_bound bigInt
,     ib_upper_bound bigInt
)
stored as parquet
location 'alluxio:///scale1/income_band'
tblproperties (
  'parquet.compression'='SNAPPY')
;

select * from income_band limit 10;

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
location 'alluxio:///scale1/promotion'
tblproperties (
  'parquet.compression'='SNAPPY')
;

select * from promotion limit 10;

drop table if exists reason;
create external table if not exists reason(
      r_reason_sk bigInt
,     r_reason_id string
,     r_reason_desc string
)
stored as parquet
location 'alluxio:///scale1/reason'
tblproperties (
  'parquet.compression'='SNAPPY')
;

select * from reason limit 10;

drop table if exists ship_mode;
create external table if not exists ship_mode(
      sm_ship_mode_sk bigInt
,     sm_ship_mode_id string
,     sm_type string
,     sm_code string
,     sm_carrier string
,     sm_contract string
)
stored as parquet
location 'alluxio:///scale1/ship_mode'
tblproperties (
  'parquet.compression'='SNAPPY')
;

select * from  ship_mode limit 10;

drop table if exists time_dim;
create external table if not exists time_dim(
      t_time_sk bigInt
,     t_time_id string
,     t_time bigInt
,     t_hour bigInt
,     t_minute bigInt
,     t_second bigInt
,     t_am_pm string
,     t_shift string
,     t_sub_shift string
,     t_meal_time string
)
stored as parquet
location 'alluxio:///scale1/time_dim'
tblproperties (
  'parquet.compression'='SNAPPY')
;

select * from time_dim limit 10;
