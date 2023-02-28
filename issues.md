log for recovery and transaction
1. 日志在redo new、undo delete时，新建页时页号不一致（难解决, 最大的日志问题）
2. 不是完全的ARIES，未记录脏页表。redo、undo策略有很大优化空间。

hash index
1. 只能支持512页以内的bucket page（directory page最多一页的架构导致）
2. 建索引速度远不如B+树（比较次数更多

性能：
反序列化太多，主要集中在索引的比较key的过程中，难以解决。

多线程的物理并发存在未知问题。

conccurrency
(暂未实现业务逻辑上的并发控制)

能否在运行过程中更改DBMS设置
