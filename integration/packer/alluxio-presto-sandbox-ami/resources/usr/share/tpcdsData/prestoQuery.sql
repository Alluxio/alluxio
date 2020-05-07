--
-- The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
-- (the "License"). You may not use this work except in compliance with the License, which is
-- available at www.apache.org/licenses/LICENSE-2.0
--
-- This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
-- either express or implied, as more fully set forth in the License.
--
-- See the NOTICE file distributed with this work for information regarding copyright ownership.
--

use alluxio;

with ssr as
(select  ss_store_sk as store_sk,
     sum(ss_ext_sales_price) as sales,
     sum(coalesce(sr_return_amt, 0)) as returns,
     sum(ss_net_profit - coalesce(sr_net_loss, 0)) as profit
 from store_sales left outer join store_returns on
     (ss_item_sk = sr_item_sk and ss_ticket_number = sr_ticket_number)
     , promotion
 where ss_promo_sk = p_promo_sk
 group by ss_store_sk),
wsr as
(select  ws_web_site_sk as website_sk,
     sum(ws_ext_sales_price) as sales,
     sum(coalesce(wr_return_amt, 0)) as returns,
     sum(ws_net_profit - coalesce(wr_net_loss, 0)) as profit
 from web_sales left outer join web_returns on
     (ws_item_sk = wr_item_sk and ws_order_number = wr_order_number)
     , promotion
 where ws_promo_sk = p_promo_sk
 group by ws_web_site_sk)
 select channel, sk, sum(sales) as sales, sum(returns) as returns, sum(profit) as profit
 from (select 'store channel' as channel, store_sk as sk, sales, returns, profit from ssr
     union all
     select 'web channel' as channel, website_sk as sk, sales, returns, profit from  wsr) x
 group by rollup (channel, sk) order by channel, sk limit 100;

