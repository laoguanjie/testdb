#### 背景
- tikv在insert并发写入的时候可能存在着一致性的问题，例如insert的数据已经响应commited了但仍旧不可见，或者不同节点同一个表的字段不一致等，所以需要相应的工具来验证一致性问题。

#### 设计
- 表初始化，记录schema结构及当前时间timestamp到schemaRecords。
- 对同一个表并发insert及ddl，其语句随机。
- ddl成功时，记录新schema结构及当前时间timestamp到schemaRecords。
- 每次insert记录开始时间startTimestamp和endTimestamp
- insert成功时，通过id查询db 数据是否存在。
	- 存在，说明数据写入成功。
	- 不存在，说明db存在数据不一致问题。
- insert失败时，从schemaRecords中，以小于startTimestamp的最近timestamp开始，以小于等于endTimestamp的timestamp结束，取出当中的多个schema，以失败的insert语句作异常验证。
	- 如果存在至少一个schema的结构会导致insert语句抛出指定的异常，则认为数据的确没有写入db。
	- 否则认为db存在数据不一致问题。

#### 进度
- 开发
	- 基础搭建。1d DONE
	- 随机insert。0.5d DONE
	- 随机ddl。1d DOING
	- insert校验。1d DOING
- 测试 1d TODO
