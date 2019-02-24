hive -e"
set hive.cli.print.header = true;
select a.*, b.category_1, b.category_2, b.category_3, b.agent_name, c.exposure_num,
c.click_num, c.download_num, c.money, c.discount
from bi.cc_download_smid_reg_view_pay_rday as a 
right join tg.map_channel as b 
on a.cc = b.cc 
left join tg.day_cc_data as c 
on a.dt = c.dt 
and a.cc = c.cc 
where a.dt between '2018-01-01' and '2018-12-31'"| sed -e 's/\t/,/g' > ~/inke/temp/data_smid_money_2018.csv
