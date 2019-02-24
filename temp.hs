#!/bin/bash
date1=`date -d "yesterday" +%Y%m%d`
date63=`date -d "63 days ago" +%Y%m%d`
echo $date1 $date63
hive -e"
use tg;
drop table bujiu;
create table if not exists bujiu as
select * from bujiu.report_cc_base_stat
where
(regexp_replace(dt, '-', '')
between $date63 and $date1)
order by (regexp_replace(dt, '-', ''))
;"
