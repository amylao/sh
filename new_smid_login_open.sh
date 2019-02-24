# 记录 20180901 至 20190130 之间所有新增 smid 和注册页曝光的 smid，采用 join 即可

hive -e"
set hive.cli.print.header = true;
select new.cat_2 as cat_2, new.smid as log_smid, 
new.ymd as log_ymd, c.*
from tg.cat2_new_smid as new
join hds.view_newapplog_login_open as c
on new.smid = c.smid
and new.ymd = c.ymd
" sed -e 's/\t/,/g' > ~/inke/temp/new_smid_login.txt
