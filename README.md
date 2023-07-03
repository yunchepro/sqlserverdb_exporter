SQL Server Exporter for Prometheus

# 采集指标

* Instance Information
* Performance Counter
* Mirror State
* Database State
* Backup
* SQL
* Wait Events
* Active Session
* Config


# 监控账号

```
use master;
create login monitor with password=[replace with a complex password];
grant VIEW ANY DEFINITION to lazybug;
grant VIEW SERVER STATE to lazybug;
```

# 常见问题

## TLS Handshake failed
linux和windows系统下，连接数据库（ SQL Server 2012，SQL Server 2014 ) 失败
```
TLS Handshake failed: tls: server selected unsupported protocol version 301
```

https://github.com/denisenkom/go-mssqldb/issues/726

解决方法1：

安装SQL Server 支持TLS 1.2的补丁。

1、升级到SQL Server 2012 SP2

2、安装补丁KB3205054

https://support.microsoft.com/zh-cn/topic/kb3135244-tls-1-2-%E5%AF%B9-microsoft-sql-server-e4472ef8-90a9-13c1-e4d8-44aad198cdbe

解决方法2:

go1.8版本，设置如下环境变量（linux系统下测试有效）

```
export GODEBUG=tls10default=1
```

解决方法3:

SQL Server连接串加入选项：encrypt=disable