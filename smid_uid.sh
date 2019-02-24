# 选择所有期间 smid 和 uid 的全连接
hive -e"
set hive.cli.print.header =true;
set mapred.reduce.tasks = 10000;
set mapred.map.tasks = 20000;
set mapreduce.reduce.memory.mb = 16384;
set hive.exec.parallel = true;
set hive.exec.max.dynamic.partitions.pernode = 500;
set hive.map.aggr = true;
set hive.groupby.skewindata = true;
 
select a.*, b.* from 
(select * from dws.smid_download 
where stat = '新增') a 
left outer join dws.u_user_new b 
on a.smid = b.smid 
"| sed -e 's/\t/,/g' > ~/inke/smid_uid/smid_uid2.txt
