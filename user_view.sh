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
use tg;
select /*+ MAPJOIN(cat2_new_smid,smid)*/ 
a.ymd as ymd, 
a.cat_2 as cat2,
a.smid as new_smid, 
b.smid as reg_smid,
c.smid as rec_simd,
d.smid as view_smid,
e.smid as act_view_smid
from (select t1.ymd, t1.cat_2, t1.smid 
from (select ymd, cat_2, smid, row_number() 
over(partition by ymd, cat_2,smid) rn1 
from cat2_new_smid)
as t1 where t1.rn1 =1) as a   
left join (select t2.smid, t2.ymd 
from (select smid, ymd, row_number()
over(partition by smid, ymd) rn2 
from hds.view_newapplog_login_open 
where ymd between '20180901' and '20190130') as t2
where t2.rn2 =1) as b 
on a.smid = b.smid and a.ymd = b.ymd
and a.ymd between '20180901' and '20190130' 
left join (select t3.smid, t3.ymd 
from (select smid, ymd, row_number()
over(partition by smid, ymd) rn3
from hds.view_newapplog_rec_card_show 
where ymd between '20180901' and '20190130') as t3
where t3.rn3 = 1) as c
on a.smid = c.smid and a.ymd = c.ymd
and a.ymd between '20180901' and '20190130'
left join (select t4.smid, t4.ymd 
from (select smid, ymd, row_number() 
over(partition by smid, ymd) rn4
from hds.view_newapplog_live_view 
where ymd between '20180901' and '20190130') as t4
where t4.rn4 = 1) as d
on a.smid = d.smid and a.ymd = d.ymd
and a.ymd between '20180901' and '20190130'
left join (select t5.smid, t5.ymd 
from (select smid, ymd, row_number()
over(partition by smid, ymd) rn5
from dws.ev_act_view 
where ymd between '20180901' and '20190130') as t5
where t5.rn5 = 1) as e
on a.smid = e.smid and a.ymd = e.ymd
and a.ymd between '20180901' and '20190130'
"| sed -e 's/\t/,/g' > ~/inke/temp/user_view.txt
