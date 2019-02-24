# 记录应用商店 20180901 至 20190130 之间所有新增 smid 和大厅曝光的 smid，采用 join 即可

hive -e"
set hive.cli.print.header = true;
select new.cat_2 as cat_2, new.smid as rec_smid,
new.ymd as rec_ymd, d.*
from tg.cat2_new_smid  as new
join hds.view_newapplog_rec_card_show as d
on new.smid = d.smid
and new.ymd = d.ymd
" sed -e 's/\t/,/g' > ~/inke/temp/new_smid_rec.txt
