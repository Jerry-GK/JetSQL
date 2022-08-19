log for recovery and transaction
1. 磁盘元信息没有被日志记录，是否会导致问题，能否克服（统一buffer pool和disk pool）
2. 退出后重新进入，初始化时出错(是1导致！)
3. 日志新建页时页号不一致（难解决）
4. catalog的读写（尤其是读）直接操作数据结构，跳过buffer pool? （可能导致问题，锁错乱）
（已经能解决不含新建页的事务原子性，支持rollback）
5. 不是完全的ARIES，未记录脏页表。redo、undo策略有很大优化空间。

hash index
1. 只能支持512页以内的bucket page（directory page最多一页的架构导致）
2. 建索引速度远不如B+树

conccurrency
(暂未实现业务逻辑上的并发控制)

能否在运行过程中更改DBMS设置
