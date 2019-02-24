# 需求
# 制作一张包含渠道和'映客','种子','不就'三种产品的大表
# 显示出每个渠道、每种产品的 CTR、CVR、激活成本、次日留存率
# 步骤
# 先 select 三个产品的后向数据表与day_cc_data内联，保留
# 统一命名为 cc, day1_num, dt, 采用union的方式连接, 命名为 b
# 保留 cc, exposure_num, click_num, activation_num, day1_num, money, dt 
# 与 map_channel_info 内连 字段增加 category_1, category_2, category_3
# 连表顺序
# map_channel_info join (day_cc_data join 分表)

00 09 * * * bash /home/liuyiming/xshell/average_level/average_level.sh > /home/liuyiming/log/average_level/log_`date -d today +\%Y\%m\%d`.log 2>&1
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
drop table average_level;
create table if not exists average_level as
select  
a.category_1 as category_1,
a.category_2 as category_2,
a.category_3 as category_3,
a.agent_name as agent_name,
a.app_id as app_id,
b.cc as cc,
b.exposure_num as exposure_num,
b.click_num as click_num,
b.activation_num as activation_num,
b.reg_num as reg_num,
b.day1_num as day1_num, 
b.day7_num as day7_num,
b.money as money, 
b.dt as dt
from tg.map_channel_info as a
right join
(select t2.cc cc, t1.exposure_num exposure_num, t1.click_num click_num,
t2.active_smid activation_num, t2.active_reg_smid reg_num, t2.day1 as day1_num, t2.day7 as day7_num, t1.money money, t2.dt dt 
from tg.day_cc_data t1 right join tg.bujiu t2 
on t1.dt = t2.dt 
and t1.cc = t2.cc
union all
select t3.cc cc, t1.exposure_num exposure_num, t1.click_num click_num, 
t3.download_smid activation_num, t3.reg_smid reg_num, t3.download_smid_day1 as day1_num, t3.download_smid_day7 as day7_num,
t1.money money, t3.dt dt
from tg.day_cc_data t1 right join bi.cc_download_smid_reg_view_pay_rday t3 
on t1.dt = t3.dt 
and t1.cc = t3.cc
union all
select t4.cc cc, t1.exposure_num exposure_num, t1.click_num click_num, 
t4.new_ndid activation_num, t4.reg_ndid reg_num, t4.stay_ndid_1 as day1_num, t4.stay_ndid_7 as day7_num, 
t1.money money, t4.dt dt 
from tg.day_cc_data t1 right join 
(select t5.cc cc, t5.new_ndid new_ndid, t5.reg_ndid as reg_ndid, t5.dt as dt, t6.stay_ndid_1 as stay_ndid_1, t6.stay_ndid_7 as stay_ndid_7 
from tg.gaia_new_reg_view_share as t5 
left join tg.gaia_stay as t6
on t5.cc = t6.cc 
and t5.ymd = t6.ymd) as t4 
on t1.dt = t4.dt 
and t1.cc = t4.cc) as b 
on a.cc = b.cc"

