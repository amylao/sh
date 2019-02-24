# 选择20180901至20190130期间，每天的新增smid，注册页曝光率，大厅曝光率，大厅推荐直播间观看率，人均直播观看时间
# a表为新增数盟ID
# b表为首启注册页曝光
# c表为大厅曝光
# d表为大厅推荐直播间观看
# e表为直播观看
# mc表为map_channel
hive -e"
set hive.cli.print.header =true;
set mapred.reduce.tasks = 5000;
set mapred.map.tasks = 20000;
set mapreduce.reduce.memory.mb = 16384;
set hive.exec.parallel = true;
set hive.exec.max.dynamic.partitions.pernode = 500;
set hive.map.aggr = true;
set hive.groupby.skewindata = true;
select /*+ MAPJOIN(a)*/ 
a.ymd, a.cat_2,
count(a.smid) as new_smid, 
count(distinct b.smid)/count(a.smid) as reg_exp_rate,
count(distinct c.smid)/count(a.smid) as rec_exp_rate,
count(distinct d.smid)/count(a.smid) as view_rate,
count(distinct e.smid)/count(a.smid) as view2_rate
from tg.cat2_new_smid as a   
left join (select smid, ymd from hds.view_newapplog_login_open where ymd between '20180901' and '20190130') as b 
on a.smid = b.smid and a.ymd = b.ymd 
left join (select smid, ymd from hds.view_newapplog_rec_card_show where ymd between '20180901' and '20190130') as c
on a.smid = c.smid and a.ymd = c.ymd
left join (select  smid, ymd from hds.view_newapplog_live_view where ymd between '20180901' and '20190130') as d
on a.smid = d.smid and a.ymd = d.ymd
left join (select  smid, ymd from dws.ev_act_view where ymd between '20180901' and '20190130') as e
on a.smid = e.smid and a.ymd = e.ymd
group by a.cat_2, a.ymd
"| sed -e 's/\t/,/g' > ~/inke/temp/user_view_rate.txt
