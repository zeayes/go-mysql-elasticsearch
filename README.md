帖子数据同步到ES
====================================

该项目fork自 [go-mysql-elasticsearch](https://github.com/siddontang/go-mysql-elasticsearch)

#### 项目原理

该项目起的进程会伪装成一个 `MySQL` 的 `Slave`，设置 `MySQL` 的 `Binlog` 的 `offset` 后， `MySQL` 会把 `Binlog` 发送到该服务，
该服务解析 `Binlog` 后，会按照一定的规则处理数据，然后把处理后的数据同步到 `Elasticsearch` 中。


#### 部署机器

| 服务器名称 | 进程管理工具 | 代码                   | 进程名称       | 说明           |
| ----       | ----         | ----                   | ----           | ----           |
| t-qb-elk3  | supervisord  | go-mysql-elasticsearch | articles_at_00 | 同步旧的帖子库 |
| t-qb-elk3  | supervisord  | go-mysql-elasticsearch | articles_at_01 | 同步新的帖子库 |
