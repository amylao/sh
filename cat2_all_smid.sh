# 记录应用商店 20180901 至 20190130 之间所有的 cc 和 smid

hive -e"
set hive.cli.print.header = true;
select a.category_2 as cat_2, b.*
from tg.map_channel as a
left join dws.smid_download as b
on a.cc = b.cc
where a.category_1 = '应用商店'
and b.ymd between '20180901' and '20190130'
" sed -e 's/\t/,/g' > ~/inke/temp/cat2_all_smid.txt
