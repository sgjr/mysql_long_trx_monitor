# mysql_long_trx_monitor
此脚本主要用来监控MySQL主库的长事务，
通过读取mysql.cfg配置文件，获取MySQL连接地址，以及告警阈值
日志按天记录至当前文件夹下边的log目录中，日志格式为YYYY-mm-dd.log
通过pushgateway进行打点
## 准备工作
需要先部署好以下工具
- prometheus
- grafana
- pushgateway

安装好python依赖，修改脚本相关参数即可。

## 监控效果


## 报警效果

