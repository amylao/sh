# 记录 20180901 至 20190130 之间所有新增 smid 和直播观看的 smid，采用 join 即可

hive -e"
set hive.cli.print.header = true;
select b.cc as cc, b.ymd as ymd, 
b.smid as new_smid, f.*
from tg.map_channel as a
left join dws.smid_download as b
on a.cc = b.cc
join dws.ev_act_view as f
on b.smid = f.smid
and b.ymd = f.ymd
and b.ymd between '20180901' and '20190130'
where a.category_1 = '应用商店'
and b.stat = '新增'
" sed -e 's/\t/,/g' > ~/inke/temp/new_smid_act.txt
