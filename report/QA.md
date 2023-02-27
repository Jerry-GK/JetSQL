- 你负责了哪些部分？
  数据的序列化与反序列化、buffer pool的设计、记录表页结构的设计、执行器的设计。

- 数据库文件的磁盘格式是怎样的？
- buffer pool有什么作用？
- buffer pool对页的pin和unpin是什么含义和作用？
- 堆表管理表页是什么意思？
- 执行器如何执行SQL语句？执行计划是什么？
- 如何实现LRU替换策略？


- 日志的格式是怎样的？
- 怎样用日志实现崩溃恢复，undo、redo的流程是怎样的？
- 怎样用日志维护事务的原子性？
- 怎样实现物理层面的多线程并发？
  给page进行latch和unlatch操作。

- 做过哪些优化？
  1. 最小堆代替链表管理表页。
  2. LRU算法的优化。
  3. B+树的keysize实现动态大小
  4. 自己实现的高效内存池 
   
- 项目遇到过哪些困难，怎样克服？
  1. 页泄露，原因是pin和unpin/delete不一一匹配
  2. 性能瓶颈：链式表+LRU在连续插入时性能极差
  3. 性能瓶颈：修改STL为自己写的数据结构管理memheap
  4. 性能瓶颈：row的正反序列化占据了绝大部分时间
  5. 粒度问题：以页为粒度进行并发控制和日志恢复，性能和开销不好。但不同页没有统一结构，细粒度下不好统一管理。
  6. disk manager对于disk meta和bitmap页单独维护page cache管理，与buffer pool独立，日志无法管理。（日志的添加和利用都是跟bpMgr对接）
   - 思路1: logMgr与diskMgr直接对接（不可行，log需要记录没写入disk的信息）
   - 思路2: log只跟diskMgr的元信息管理部分对接，也就是page cache处 # 需要对log分类，undo/redo时需要用diskMgr的接口（耦合性增大）
   - 思路3：bpMgr可以直接管理磁盘的meta和bitmap page，将磁盘管理信息纳入bp管理，diskMgr只负责页的读写。对外bpMgr仍只提供逻辑页访问接口。
  7. 迭代器的生命周期问题
  8. 多线程：共享buffer pool、diskMgr、logMgr，带来很多奇奇怪怪的并发问题。