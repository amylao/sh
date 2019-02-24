# 记录应用商店 20180901 至 20190130 之间所有的 cc 和所有新增的 smid

hive -e"
set hive.cli.print.header = true;
select distinct a.category_2 as cat_2, 
b.smid as smid,
b.create_time as create_time,
b.client as client,
b.package_name as package_name,
b.cc as cc,
b.cv as cv,
b.ip as ip,
b.osversion as osversion,
b.info_extra as info_extra,
b.ymd as ymd
from tg.map_channel as a
left join dws.smid_download as b
on a.cc = b.cc
where b.ymd between '20180901' and '20190130'
and a.category_1 = '应用商店'
and b.stat = '新增'
" sed -e 's/\t/,/g' > ~/inke/temp/cat2_new_smid.txt
