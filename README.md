# JetSQL
## 简介

JetSQL是一个支持基本SQL语法的关系型数据库管理系统，支持同时维护多个数据库实例，支持数据表的插入、删除、更新，支持建立唯一性索引并利用索引加速查询，支持简单的查询计划生成。JetSQL的原型是浙江大学数据库系统课程的项目MiniSQL，在课程结束后，在MiniSQL基础上又增加了一些功能，使之可以在SAFE模式下支持日志，可利用日志进行事务回滚，维持事务原子性，另外日志也可以用于意外退出后的数据恢复，维护数据库一致性。此外，MiniSQL仅支持B+树索引，JetSQL在MiniSQL的基础上增加了索引类型：哈希索引，可供用户选择。

框架参考CMU-15445 BusTub框架进行改写，在保留了缓冲池、索引、记录模块的一些核心设计理念的基础上，做了一些修改和扩展。
相比于BusTub，以下是改动/扩展较大的几个地方：

- 对Disk Manager模块进行改动，扩展了位图页、磁盘文件元数据页用于支持持久化数据页分配回收状态；
- 在Record Manager, Index Manager, Catalog Manager等模块中，通过对内存对象的序列化和反序列化来持久化数据；
- 对Record Manager层的一些数据结构（`Row`、`Field`、`Schema`、`Column`等）和实现进行重构；
- 对Catalog Manager层进行重构，支持持久化并为Executor层提供接口调用;
- 扩展了Parser层，Parser层支持输出语法树供Executor层调用；
- 另外，JetSQL还对索引模块进行了去模版化，使索引占据空间更灵活、模块封装性更好。

此外还涉及到很多零碎的改动，包括源代码中部分模块代码的调整，测试代码的修改，性能上的优化等，在此不做赘述。



## 文件结构说明

- ./src: 包含源代码
- ./report: 含总体设计报告与组员个人详细报告
- ./doc: 含存有数据库文件、数据库元信息的文件夹，测试用的sql文件请放在doc/sql/目录下。
    - ./doc/db: 数据库文件
    - ./doc/meta: 包含数据库元信息文件DatabaseMeta.txt, 记录存在的数据库名，用于初始化。
    - ./doc/log: 数据库的日志文件（如果是SAFE模式则会生成），日志文件可能较大
    - ./doc/sql: 要执行的sql脚本文件所在的目录

- ./test: 测试相关文件

- ./thirdparty: 第三方库，主要与google test有关。

    

## 编译&开发环境要求
- Apple clang version: 11.0+ (MacOS)，使用`gcc --version`和`g++ --version`查看

- gcc & g++ : 8.0+ (Linux)，使用`gcc --version`和`g++ --version`查看

- cmake: 3.20+ (Both)，使用`cmake --version`查看

- gdb: 7.0+ (Optional)，使用`gdb --version`查看

- flex & bison (暂时不需要安装，但如果需要对SQL编译器的语法进行修改，需要安装）

    

## 构建
### Windows
目前该代码暂不支持在Windows平台上的编译。但在Win10及以上的系统中，可以通过安装WSL（Windows的Linux子系统）来进行
开发和构建。WSL请选择Ubuntu子系统（推荐Ubuntu20及以上）。如果你使用Clion作为IDE，可以在Clion中配置WSL从而进行调试。

### MacOS & Linux & WSL
基本构建命令
```bash
mkdir build
cd build
cmake ..
make -j
```
若不涉及到`CMakeLists`相关文件的变动且没有新增或删除`.cpp`代码（通俗来说，就是只是对现有代码做了修改）
则无需重新执行`cmake..`命令，直接执行`make -j`编译即可，否则需要重新cmake。

默认以`release`模式进行编译，如果你需要使用`debug`模式进行编译，将./CMakeLists.txt中的

```cmake
SET(CMAKE_BUILD_TYPE Release)
```

改为

```cmake
SET(CMAKE_BUILD_TYPE Debug)
```

注意如果在M系列芯片的mac电脑上无法cmake，可以尝试将./CMakeLists.txt中的

```cmake
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC -Wall -march=native")
```

改为

```cmake
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC -Wall -mcpu=apple-m1") 
```

在构建完成后，在build目录下输入

```shell
./bin/main
```

看到显示 “ JetSQL > “ 字样，即说明已经进入程序。

接下来输入以分号结尾的命令并按下回车即可执行命令。




## 单元测试

在构建后，默认会在`build/test`目录下生成`minisql_test`的可执行文件，通过`./minisql_test`即可运行所有测试。

如果需要运行单个测试，例如，想要运行`lru_replacer_test.cpp`对应的测试文件，可以通过`make lru_replacer_test`
命令进行构建。注意单元测试最初是为MiniSQL设置的，在后续架构修改的过程中，部分测试可能不适用于JetSQL。



## 功能

- 基本SQL语句

    包括创建、删除、显示数据库实例，创建、删除、显示表，对表记录的插入、删除、更新。支持创建、删除、显示索引。筛选支持等值条件、范围条件、多条件，某些情况下支持利用索引加速查询。SAFE模式支持事务相关语法，能够保证事务原子性。不在begin语句之后的sql单独语句视为单个事务。变量类型包括整型、字符串型、浮点型，支持unique声明和主键声明。

    支持执行sql文件，sql文件必须放在./doc/sql目录下，执行时输入execfile "文件名"即可，需要包含可能存在的后缀。sql文件暂不支持注释，如果遇到某条指令执行失败，将会中止执行文件。

    具体功能可参考实验报告（位于./report目录下）

    SQL语法实例如下:

    ```sql
    create database db0;
    drop database db0;
    show databases;
    use db0;
    show tables;
    create table t1(a int, b char(20) unique, c float, primary key(a, c));
    create table t1(a int, b char(0) unique, c float, primary key(a, c));
    create table t1(a int, b char(-5) unique, c float, primary key(a, c));
    create table t1(a int, b char(3.69) unique, c float, primary key(a, c));
    create table t1(a int, b char(-0.69) unique, c float, primary key(a, c));
    create table student(
      sno char(8),
      sage int,
      sab float unique,
      primary key (sno, sab)
    );
    drop table t1;
    
    create index idx1 on t1(a, b);
    create index idx1 on t1(a, b);
    drop index idx1;
    show indexes;
    select * from t1;
    select id, name from t1;
    select * from t1 where id = 1;
    
    select * from t1 where id = 1 and name = "str";
    select * from t1 where id = 1 and name = "str" or age is null and bb not null;
    insert into t1 values(1, "aaa", null, 2.33);
    delete from t1;
    delete from t1 where id = 1 and amount = 2.33;
    update t1 set c = 3;
    update t1 set a = 1, b = "ccc" where b = 2.33;
    begin;
    commit;
    rollback;
    quit;
    execfile "test.sql";
    ```

- JetSQL系统设置

    对于系统模式、默认索引类型、替换器类型等属性，无法在程序运行时修改，可以在代码中修改相关属性。
    默认是FAST模式、B+树索引、LRU替换器。如果需要尝试支持日志、事务的SAFE模式，或者默认哈希索引、clock替换器，需要手动设置。

    相关文件是./src/include/common/setting.h

    ```c++
    // add DBMS mode
    //SAFE: using log for recovery and transaction rollback
    //FAST: does not support recovery and transaction, but faster performance without writing log
    enum DBMS_MODE {FAST, SAFE};
    
    //replacer type
    enum REPLACER_TYPE {LRU, CLOCK};
    
    //index type
    enum INDEX_TYPE {BPTREE, HASH};
    
    static bool LATER_INDEX_AVAILABLE = true;//if building an index on an non empty table is allowed
    static DBMS_MODE CUR_DBMS_MODE = FAST;
    static bool USING_LOG = (CUR_DBMS_MODE!=FAST);
    static REPLACER_TYPE CUR_REPLACER_TYPE = LRU;
    static INDEX_TYPE DEFAULT_INDEX_TYPE = BPTREE;
    ```

    FAST表示快速模式，SAFE表示安全模式，后者会利用日志防止意外崩溃导致的数据不一致性，并且支持事务，但速度较慢。

    默认索引类型有BPTREE和HASH两种，后者能容纳的索引键数量较少。

    替换策略可选择LRU或者CLOCK，建议LRU。

    后续可能增加在程序内修改相关设置的功能。

- 暂未包含的功能和改善空间

    - 日志系统并没有完全成型，没有对磁盘元信息页有很好的追踪，导致并不能完全处理崩溃问题，所以目前不建议使用SAFE模式。
    - 暂未实现逻辑层面的并发控制，未进行多线程测试。
    - 暂不支持外键，不支持join操作。不支持临时表、嵌套查询语法。
    - 未实现用户权限管理系统。
    - not null声明无效


