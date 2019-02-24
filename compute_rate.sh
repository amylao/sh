# 2018-12-15 至 1019-01-15 区间，所有渠道新增激活、 3 日留存和 7 日留存数保存
hive -e "set hive.cli.print.header = true; select b.category_1,b.category_2, a.download_smid, a.download_smid_day3, a.download_smid_day7 from bi.cc_download_smid_reg_view_pay_rday as a right join tg.map_channel as b on a.cc = b.cc and a.dt between '2018-12-15' and '2019-01-15' " sed -e 's/\t/,/g' > ~/inke/temp/12150115.csv 
