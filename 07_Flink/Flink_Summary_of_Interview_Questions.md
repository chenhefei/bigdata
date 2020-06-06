# 常见面试问题汇总

## 1．面试题一：应用架构

问题：公司怎么提交的实时任务，有多少 Job Manager？

解答： 

1)    我们使用yarn session模式提交任务。每次提交都会创建一个新的 Flink 集群，为每一个 job 提供一个 yarn-session，任务之间互相独立，互不影响，方便管理。任务执行完成之后创建的集群也会消失。线上命令脚本如下：

```
bin/yarn-session.sh -n 7 -s 8 -jm 3072 -tm 32768 -qu root.*.* -nm *-* -d
```

其中申请 7 个 taskManager，每个 8 核，每个 taskmanager 有 32768M 内存。 

2)    集群默认只有一个 Job Manager。但为了防止单点故障，我们配置了高可用。我们公司一般配置一个主 Job Manager，两个备用 Job Manager，然后结合 ZooKeeper 的使用，来达到高可用。

## 2．面试题二：压测和监控

问题：怎么做压力测试和监控？

解答：我们一般碰到的压力来自以下几个方面： 

1)    产生数据流的速度如果过快，而下游的算子消费不过来的话，会产生背压。背压的监控可以使用 Flink Web UI(localhost:8081) 来可视化监控，一旦报警就能知道。一般情况下背压问题的产生可能是由于 sink 这个 操作符没有优化好，做一下优化就可以了。比如如果是写入 ElasticSearch， 那么可以改成批量写入，可以调大 ElasticSearch 队列的大小等等策略。

2)    设置watermark的最大延迟时间这个参数，如果设置的过大，可能会造成内存的压力。可以设置最大延迟时间小一些，然后把迟到元素发送到侧输出流中去。晚一点更新结果。或者使用类似于 RocksDB 这样的状态后端， RocksDB 会开辟堆外存储空间，但 IO 速度会变慢，需要权衡。 

3)    还有就是滑动窗口的长度如果过长，而滑动距离很短的话，Flink 的性能会下降的很厉害。我们主要通过时间分片的方法，将每个元素只存入一个“重叠窗口”，这样就可以减少窗口处理中状态的写入。参见链接：

https://www.infoq.cn/article/sIhs_qY6HCpMQNblTI9M

4)    状态后端使用 RocksDB，还没有碰到被撑爆的问题。

## 3．面试题三：为什么用Flink

问题：为什么使用 Flink 替代 Spark？

解答：主要考虑的是flink的低延迟、高吞吐量和对流式数据应用场景更好的支持；另外，flink可以很好地处理乱序数据，而且可以保证exactly-once的状态一致性。

## 4．面试题四：checkpoint的存储

问题：Flink 的 checkpoint 存在哪里？

解答：可以是内存，文件系统，或者 RocksDB。

## 5．面试题五：exactly-once的保证

问题：如果下级存储不支持事务，Flink 怎么保证 exactly-once？

解答：端到端的exactly-once对sink要求比较高，具体实现主要有幂等写入和事务性写入两种方式。幂等写入的场景依赖于业务逻辑，更常见的是用事务性写入。而事务性写入又有预写日志（WAL）和两阶段提交（2PC）两种方式。

如果外部系统不支持事务，那么可以用预写日志的方式，把结果数据先当成状态保存，然后在收到 checkpoint 完成的通知时，一次性写入 sink 系统。

## 6．面试题六：状态机制

问题：说一下 Flink 状态机制？

解答：Flink内置的很多算子，包括源source，数据存储sink都是有状态的。在Flink中，状态始终与特定算子相关联。Flink会以checkpoint的形式对各个任务的状态进行快照，用于保证故障恢复时的状态一致性。Flink通过状态后端来管理状态和checkpoint的存储，状态后端可以有不同的配置选择。

## 7．面试题七：海量key去重

问题：怎么去重？考虑一个实时场景：双十一场景，滑动窗口长度为 1 小时，滑动距离为 10 秒钟，亿级用户，怎样计算 UV？ 

解答：使用类似于 scala 的 set 数据结构或者 redis 的 set 显然是不行的，因为可能有上亿个 Key，内存放不下。所以可以考虑使用布隆过滤器（Bloom Filter）来去重。

![img](https://img.lormer.cn/clip_image002.jpg)

## 8．面试题八：checkpoint与spark比较

问题：Flink 的 checkpoint 机制对比 spark 有什么不同和优势？

解答：spark streaming 的 checkpoint 仅仅是针对 driver 的故障恢复做了数据和元数据的 checkpoint。而 flink 的 checkpoint 机制 要复杂了很多，它采用的是轻量级的分布式快照，实现了每个算子的快照，及流动中的数据的快照。

## 9．面试题九：watermark机制

问题：请详细解释一下Flink的Watermark机制。

解答：Watermark本质是Flink中衡量EventTime进展的一个机制，主要用来处理乱序数据。

## 10．面试题十：exactly-once如何实现

问题：Flink中exactly-once语义是如何实现的，状态是如何存储的？

解答：Flink依靠checkpoint机制来实现exactly-once语义，如果要实现端到端的exactly-once，还需要外部source和sink满足一定的条件。状态的存储通过状态后端来管理，Flink中可以配置不同的状态后端。

## 11．面试题十一：CEP

问题：Flink CEP 编程中当状态没有到达的时候会将数据保存在哪里？

解答：在流式处理中，CEP 当然是要支持 EventTime 的，那么相对应的也要支持数据的迟到现象，也就是watermark的处理逻辑。CEP对未匹配成功的事件序列的处理，和迟到数据是类似的。在 Flink CEP的处理逻辑中，状态没有满足的和迟到的数据，都会存储在一个Map数据结构中，也就是说，如果我们限定判断事件序列的时长为5分钟，那么内存中就会存储5分钟的数据，这在我看来，也是对内存的极大损伤之一。

## 12．面试题十二：三种时间语义

问题：Flink 三种时间语义是什么，分别说出应用场景？

解答：

1. Event Time：这是实际应用最常见的时间语义。

2. Processing Time：没有事件时间的情况下，或者对实时性要求超高的情况下。 

3. Ingestion Time：存在多个Source Operator的情况下，每个Source Operator可以使用自己本地系统时钟指派 Ingestion Time。后续基于时间相关的各种操作，都会使用数据记录中的 Ingestion Time。

## 13．面试题十三：数据高峰的处理

问题：Flink程序在面对数据高峰期时如何处理？

解答：使用大容量的 Kafka 把数据先放到消息队列里面作为数据源，再使用 Flink 进行消费，不过这样会影响到一点实时性。



# 附录

## 面试题二中的文章链接

