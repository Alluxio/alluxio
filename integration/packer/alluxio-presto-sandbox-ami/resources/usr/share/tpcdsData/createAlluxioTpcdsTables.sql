create database if not exists alluxio;
use alluxio;  
drop table if exists store_sales;
create external table if not exists store_sales(
      ss_sold_time_sk bigint
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
partitioned by (ss_sold_date_sk bigint)
stored as parquet
location 'alluxio:///s3/store_sales'
tblproperties (
  'parquet.compression'='SNAPPY')
;

msck repair table store_sales;

drop table if exists store_returns;
create external table if not exists store_returns(
      sr_return_time_sk bigint
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
partitioned by (sr_returned_date_sk bigint)
stored as parquet
location 'alluxio:///s3/store_returns'
tblproperties (
  'parquet.compression'='SNAPPY')
;

msck repair table store_returns;

drop table if exists web_sales;
create external table if not exists web_sales(
      ws_sold_time_sk bigInt
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
partitioned by (ws_sold_date_sk bigInt)
stored as parquet
location 'alluxio:///s3/web_sales'
tblproperties (
  'parquet.compression'='SNAPPY')
;

msck repair table web_sales;

drop table if exists web_returns;
create external table if not exists web_returns(
      wr_returned_time_sk bigInt
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
partitioned by (wr_returned_date_sk bigInt)
stored as parquet
location 'alluxio:///s3/web_returns'
tblproperties (
  'parquet.compression'='SNAPPY')
;

msck repair table web_returns;

