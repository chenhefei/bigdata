---
title: Flink高阶编程
date: 2020-6-3 01:17:10
excerpt: Flink高阶编程 窗口 时间语义与Watermark ProcessFunctionAPI 状态编程和容错机制等
tags: 
  - Flink
  - 大数据框架
categories:
  - [Flink]
typora-root-url: C:\Users\Paradiser\Pictures\RootForTypora
---



# 第6章 Flink高阶编程

在上一个章节中，我们已经学习了Flink的基础编程API的使用，接下来，我们来学习Flink编程的高阶部分。所谓的高阶部分内容，其实就是Flink与其他计算框架不相同且占优势的地方，比如Window和Exactly-Once，接下来我们就对这些内容进行详细的学习。

## **6.1 Window**

### 6.1.1 窗口概述

流式计算是一种被设计用于处理无限数据集的数据处理引擎，而无限数据集是指一种不断增长的本质上无限的数据集，而Window窗口是一种切割无限数据为有限块进行处理的手段。

Window是无限数据流处理的核心，Window将一个无限的stream拆分成有限大小的”buckets”桶，我们可以在这些桶上做计算操作。

![](https://lormer.cn/20200602200806.png)

### 6.1.2 窗口类型

Window可以分成两类：

#### TimeWindow

按照时间生成Window，根据窗口实现原理可以分成三类：

##### 滚动窗口（Tumbling Window）

将数据依据固定的窗口长度对数据进行切片。滚动窗口分配器将每个元素分配到一个指     定窗口大小的窗口中，滚动窗口有一个固定的大小，并且不会出现重叠。

![](https://lormer.cn/20200602200828.png)

适用场景：适合做BI统计等（做每个时间段的聚合计算）

```scala
env.setParallelism(1)
val fileDS =
    env.socketTextStream("localhost", 9999)
val wordDS = fileDS.flatMap(_.split(" "))
val word2OneDS = 
    wordDS.map((_,1))
val wordKS = word2OneDS.keyBy(_._1)
val wordWS =
    wordKS.timeWindow(Time.seconds(3))
val sumDS = wordWS.sum(1)
sumDS.print("window>>>")
env.execute()
```

##### 滑动窗口（Sliding Window）

滑动窗口是固定窗口的更广义的一种形式，滑动窗口由固定的窗口长度和滑动间隔组成。滑动窗口分配器将元素分配到固定长度的窗口中，与滚动窗口类似，窗口的大小由窗口大小参数来配置，另一个窗口滑动参数控制滑动窗口开始的频率。因此，滑动窗口如果滑动参数小于窗口大小的话，窗口是可以重叠的，在这种情况下元素会被分配到多个窗口中。

![](https://lormer.cn/20200602200923.png)

适用场景：对最近一个时间段内的统计, 比如求最近1小时内每5分钟的水位变化

```scala
env.setParallelism(1)
val fileDS =
    env.socketTextStream("localhost", 9999)
val wordDS = fileDS.flatMap(_.split(" "))
val word2OneDS = 
    wordDS.map((_,1))
val wordKS = word2OneDS.keyBy(_._1)
val wordWS =
    wordKS.timeWindow(Time.seconds(3), Time.seconds(1))
val sumDS = wordWS.sum(1)
sumDS.print("window>>>")
env.execute()
```

##### 会话窗口（Session Window）

由一系列事件组合一个指定时间长度的timeout间隙组成，类似于web应用的session， 也就是一段时间没有接收到新数据就会生成新的窗口。

![](https://lormer.cn/20200602200958.png)

```scala
val env =
    StreamExecutionEnvironment
        .getExecutionEnvironment
env.setParallelism(1)
val fileDS =
    env.socketTextStream("localhost", 9999)
val wordDS = fileDS.flatMap(_.split(" "))
val word2OneDS = wordDS.map((_,1))
val wordKS = word2OneDS.keyBy(_._1)
val wordWS = wordKS.window(
    ProcessingTimeSessionWindows.withGap(
        Time.seconds(2)))
val sumDS = wordWS.sum(1)
sumDS.print("window>>>")
env.execute()
```

#### CountWindow

按照指定的数据条数生成一个Window，与时间无关。根据窗口实现原理可以分成两类:

##### 滚动窗口

默认的CountWindow是一个滚动窗口，只需要指定窗口大小即可，当元素数量达到窗 口大小时，就会触发窗口的执行。

```scala
val dataDS: DataStream[String] =
 env.socketTextStream("hadoop01", 9999)

val mapDS = dataDS.map(data=>{
    val datas = data.split(",")
    (datas(0),1)
})

val reduceDS = mapDS.keyBy(_._1)
    .countWindow(3).reduce(
    (t1, t2) => {
        (t1._1, t1._2 + t2._2)
    }
)

reduceDS.print()
```

##### 滑动窗口

滑动窗口和滚动窗口的函数名是完全一致的，只是在传参数时需要传入两个参数，一个     是window_size，一个是sliding_size。下面代码中的sliding_size设置为了2，也就是说，     每收到两个相同key的数据就计算一次，每一次计算的window范围是3个元素。

```scala
val dataDS = env.socketTextStream("hadoop02", 9999)

val mapDS = dataDS.map(data=>{
    val datas = data.split(",")
    (datas(0),datas(2).toInt)
})

val reduceDS = mapDS.keyBy(_._1)
    .countWindow(3,2).reduce(
    (t1, t2) => {
        (t1._1, t1._2 + t2._2)
    }
)

reduceDS.print()
```

### 6.1.3 窗口使用API

window function 定义了要对窗口中收集数据后所做的计算操作，主要可以分为两类：

#### 增量聚合函数（incremental aggregation functions）

每条数据到来就进行计算，保持一个简单的状态。典型的增量聚合函数有：

##### ReduceFunction

```scala
val dataDS: DataStream[String] =
 env.socketTextStream("hadoop02", 9999)

val mapDS = dataDS.map(data=>{
    val datas = data.split(",")
    (datas(0),datas(2).toInt)
})

val reduceDS: DataStream[(String, Int)] = mapDS.keyBy(_._1)
    .timeWindow(Time.seconds(3)).reduce(
        new ReduceFunction[(String, Int)] {
            override def reduce(t: (String, Int), t1: (String, Int)): (String, Int) = {
                (t._1, t._2 + t1._2)
            }
        }
    )
reduceDS.print()
```

##### AggregateFunction

```scala
val dataDS: DataStream[String] =
 env.socketTextStream("hadoop02", 9999)

val mapDS = dataDS.map(data=>{
    val datas = data.split(",")
    (datas(0),datas(2).toInt)
})

val aggregateDS: DataStream[(String, Int)] = mapDS.keyBy(_._1)
    .countWindow(3).aggregate(
        // TODO 此处聚合函数类似于Spark中的累加器
        new AggregateFunction[(String, Int), (Int, Int), (String, Int)] {
            override def createAccumulator(): (Int, Int) = {
                (0,0)
            }

            override def add(in: (String, Int), acc: (Int, Int)): (Int, Int) = {
                (in._2 + acc._1, acc._2 + 1)
            }

            override def getResult(acc: (Int, Int)): (String, Int) = {
                ("sensor", (acc._1 / acc._2))
            }

            override def merge(acc: (Int, Int), acc1: (Int, Int)): (Int, Int) = {
                (acc._1 + acc1._1, acc._2 + acc1._2)
            }
        }
    )

aggregateDS.print()
```

#### 全窗口函数（full window functions）

先把窗口所有数据收集起来，等到计算的时候会遍历所有数据。

##### ProcessWindowFunction

就是一个对整个窗口中数据处理的函数。

```scala
val dataDS: DataStream[String] =
 env.socketTextStream("hadoop02", 9999)

val mapDS = dataDS.map(data=>{
    val datas = data.split(",")
    (datas(0),datas(2).toInt)
})

val processDS: DataStream[String] = mapDS.keyBy(_._1)
    .countWindow(3)
    .process(new ProcessWindowFunction[(String, Int), String, String, GlobalWindow] {
        override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[String]): Unit = {
            //println(elements.mkString(","))
            out.collect(elements.mkString(","))
        }
    })
processDS.print()
```

##### 其它可选API

- `.trigger() `—— 触发器:定义 window 什么时候关闭，触发计算并输出结果

- `.evitor()` —— 移除器: 定义移除某些数据的逻辑

- `.allowedLateness() `—— 允许处理迟到的数据

- `.sideOutputLateData() `—— 将迟到的数据放入侧输出流

- `.getSideOutput()` —— 获取侧输出流

## 6.2 时间语义与Watermark

### 6.2.1 时间语义

在Flink的流式处理中，会涉及到时间的不同概念，如下图所示：

![](https://lormer.cn/20200601100627.png)

- Event Time：是事件创建的时间。
  - 它通常由事件中的时间戳描述，例如采集的日志数据中，每一条日志都会记录自己的生成时间，Flink通过时间戳分配器访问事件    时间戳。
- Ingestion Time：是数据进入Flink的时间。
- Processing Time：数据处理时间
  - 是每一个执行基于时间操作的算子的本地系统时间，与机器相关，默认的时间属性就是Processing Time。

Flink是一个事件驱动的计算框架，就意味着每来一条数据，才应该触发Flink中的计算，但是如果此时计算涉及到时间问题，就比较麻烦，比如一小时内每5分钟的水位变化。

在分布式环境中，真实数据发生时间和Flink中数据处理的时间是有延迟的，那么显然，你拿处理时间作为统计的窗口时间范围是不够准确的。

所以我们一般在业务处理时，需要使用事件产生的时间，而不是处理时间。

默认情况下，Flink框架中处理的时间语义为ProcessingTime，如果要使用EventTime，那么需要引入EventTime的时间属性，引入方式如下所示：

```scala
// ----- 课件 -----
import org.apache.flink.streaming.api.TimeCharacteristic

val env: StreamExecutionEnvironment = 
StreamExecutionEnvironment.getExecutionEnvironment
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
```

```scala
package com.atguigu.bigdata.flink.chapter06

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Flink06_Window_Time {
    
    def main(args: Array[String]): Unit = {
        
        // TODO 时间语义
        // 1. Flink是一个事件驱动的计算框架
        // 2. 默认采用的处理时间作为计算的时间标准（属性）
        //    在乱序数据的情况下，计算的结果会存在误差，所以不推荐使用
        // 3. 在乱序数据处理的场合，一般会采用事件时间作为计算的标准
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        // TODO 设置时间语义为EventTime
        // 如果设定时间语义为EventTime， 那么窗口计算的时间以数据产生的时间为基准
        // 需要指定数据的时间属性作为EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        
        val dataDS: DataStream[String] = env.readTextFile("input/sensor-data.log")
        
        val sensorDS: DataStream[WaterSensor] = dataDS.map(
            data => {
                val datas = data.split(",")
                WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
            }
            )
        // TODO 设定数据的EventTime
        // 从数据中抽取EventTime,作为计算的基准时间
        // 因为一行数据中可能有多个时间 我们需要显式指明使用哪个时间进行计算
        sensorDS.assignAscendingTimestamps(_.ts)
        
        env.execute()
    }
    case class WaterSensor(id: String, ts: Long, vc: Int)
}
```

需要传入计算基准时间

![](https://lormer.cn/20200601101322.png)

`org.apache.flink.streaming.api.TimeCharacteristic`

![](https://lormer.cn/20200601101606.png)

![](https://lormer.cn/20200601101640.png)

![](https://lormer.cn/20200601103252.png)

`assignAscendingTimestamps()` 有序数据时间戳

### 6.2.2 Watermark

流处理从事件产生，到流经source，再到operator，中间是有一个过程和时间的。虽然大部分情况下，流到operator的数据都是按照事件产生的时间顺序来的，但是也不排除由于网络、背压等原因，导致乱序的产生（out-of-order或者说late element）。

但是对于late element，我们又不能无限期的等下去，必须要有个机制来保证一个特定的时间后，必须触发window去进行计算了。这个特别的机制，就是watermark。

这里的Watermark什么意思呢？很简单，把数据流简单的理解为水流，那么当水流源源不断地流入咱们系统时，什么时候我们才知道要开始对数据计算了呢？总不能一直等吧。所以为了能够对数据计算的时间进行限定，我们的想法就是在水流上添加浮标或标记，当这个标记进入我们的数据窗口时，我们就认为可以开始计算了。这里在水流中增加的标记，我们就称之为Watermark（**水位标记/水位线/水印**）

![](https://lormer.cn/20200601104259.png)

在实际操作中，Watermark作为特殊标记数据由Flink根据当前数据的EventTime创建出来后自动加入到数据队列中。( **时间语义设置为EventTime后 触发窗口的计算不再以时间为触发 而是以Watermark为触发**)

当Watermark数据进入到窗口范围后，会判断时间窗口是否触发计算，所以Watermark数据中应该包含时间属性。

但是这个时间属性值设置为多少合适呢？首先我们需要明确，如果数据是按照顺序采集过来的，那么来一条计算一条的话，是不会出现问题的，也无需加入任何的标记，时间一到，窗口自动触发计算就可以了，

但实际情况恰恰是数据采集到Flink中时是乱序的，就意味着当触发窗口计算的时候，是有可能数据不全的，因为数据被打乱了，还没有采集到，

基于这样的原因，所以需要在数据采集队列中增加标记，表示指定时间的窗口数据全部到达，可以计算，无需等待了。

根据不同的数据处理场景watermark会有不同的生成方式：

1. 有序数据：`DataStream.assignAscendingTimestamps`

2. 乱序数据：`DataStream.assignTimestampsAndWatermarks`

   - 乱序数据中的watermark处理又分两大类：

   - `AssignerWithPeriodicWatermarks` 周期性的

   - `AssignerWithPunctuatedWatermarks` 间歇性的

```java
public abstract class BoundedOutOfOrdernessTimestampExtractor<T> implements AssignerWithPeriodicWatermarks<T> { ... }
```

```java
public interface AssignerWithPeriodicWatermarks<T> extends TimestampAssigner<T> {

	/**
	 * Returns the current watermark. This method is periodically called by the
	 * system to retrieve the current watermark. The method may return {@code null} to
	 * indicate that no new Watermark is available.
	 *
	 * <p>The returned watermark will be emitted only if it is non-null and its timestamp
	 * is larger than that of the previously emitted watermark (to preserve the contract of
	 * ascending watermarks). If the current watermark is still
	 * identical to the previous one, no progress in event time has happened since
	 * the previous call to this method. If a null value is returned, or the timestamp
	 * of the returned watermark is smaller than that of the last emitted one, then no
	 * new watermark will be generated.
	 *
	 * <p>The interval in which this method is called and Watermarks are generated
	 * depends on {@link ExecutionConfig#getAutoWatermarkInterval()}.
	 *
	 * @see org.apache.flink.streaming.api.watermark.Watermark
	 * @see ExecutionConfig#getAutoWatermarkInterval()
	 *
	 * @return {@code Null}, if no watermark should be emitted, or the next watermark to emit.
	 */
	@Nullable
	Watermark getCurrentWatermark();
}
```

```java
public interface AssignerWithPunctuatedWatermarks<T> extends TimestampAssigner<T> {

	/**
	 * Asks this implementation if it wants to emit a watermark. This method is called right after
	 * the {@link #extractTimestamp(Object, long)} method.
	 *
	 * <p>The returned watermark will be emitted only if it is non-null and its timestamp
	 * is larger than that of the previously emitted watermark (to preserve the contract of
	 * ascending watermarks). If a null value is returned, or the timestamp of the returned
	 * watermark is smaller than that of the last emitted one, then no new watermark will
	 * be generated.
	 *
	 * <p>For an example how to use this method, see the documentation of
	 * {@link AssignerWithPunctuatedWatermarks this class}.
	 *
	 * @return {@code Null}, if no watermark should be emitted, or the next watermark to emit.
	 */
	@Nullable
	Watermark checkAndGetNextWatermark(T lastElement, long extractedTimestamp);
}
```

```java
public interface TimestampAssigner<T> extends Function {

	/**
	 * Assigns a timestamp to an element, in milliseconds since the Epoch.
	 *
	 * <p>The method is passed the previously assigned timestamp of the element.
	 * That previous timestamp may have been assigned from a previous assigner,
	 * by ingestion time. If the element did not carry a timestamp before, this value is
	 * {@code Long.MIN_VALUE}.
	 *
	 * @param element The element that the timestamp will be assigned to.
	 * @param previousElementTimestamp The previous internal timestamp of the element,
	 *                                 or a negative value, if no timestamp has been assigned yet.
	 * @return The new timestamp.
	 */
	long extractTimestamp(T element, long previousElementTimestamp);
}
```



#### EventTime和ProcessingTime

```
--- 测试数据 ---
sensor_1,1549044122,1
sensor_1,1549044123,2
sensor_1,1549044124,3
sensor_1,1549044125,4
sensor_1,1549044126,5
sensor_1,1549044127,6
sensor_1,1549044128,7
sensor_1,1549044129,8
sensor_1,1549044130,9
```

```scala
package com.atguigu.bigdata.flink.chapter06

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

object Flink07_Window_Watermark {
    
    def main(args: Array[String]): Unit = {
        
        // TODO Watermark
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        // TODO 设置时间语义为EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        
        val dataDS: DataStream[String] = env.readTextFile("input/sensor-data.log")
        
        // watermark : 用于触发窗口计算的标记
        // 因为使用EventTime作为计算的时间标准，但是指定数据没有到达Flink
        // 那么窗口可能就无法计算，所以必须要增加标记，触发窗口的计算。
        //dataDS.assignTimestampsAndWatermarks()
        env.execute()
    }
    case class WaterSensor(id: String, ts: Long, vc: Int)
}
```

```scala
// ----- 课堂示例代码 -----
package com.atguigu.bigdata.flink.chapter06

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Flink08_Window_Watermark1 {
    
    def main(args: Array[String]): Unit = {
        
        // TODO Watermark
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        // TODO 设置时间语义为EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        
        val socketDS: DataStream[String] = env.socketTextStream("localhost", 9999)
        
        val sensorDS = socketDS.map(
            data => {
                val datas = data.split(",")
                WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
            }
            )
        // TODO 从数据中抽取EventTime
        // Flink的时间以毫秒为单位 如果拿到的数据的时间戳是10位 则是以秒为单位的 需要乘1000
        val timeDS: DataStream[WaterSensor] = sensorDS.assignAscendingTimestamps(_.ts * 1000L)
        
        val processDS: DataStream[String] = timeDS
                .keyBy(_.id)
                .timeWindow(Time.seconds(3))
                .process(
                    new ProcessWindowFunction[WaterSensor, String, String, TimeWindow] {
                        override def process(key: String, context: Context, elements: Iterable[WaterSensor], out: Collector[String]): Unit = {
                            out.collect("窗口被触发计算了. . .")
                        }
                    })
        // 这句话是老师上课时候为了看到每一条数据的抽取过程加上去的
        timeDS.print("time>>>>")
        processDS.print("et>>>>")
        
        env.execute()
    }
    case class WaterSensor(id: String, ts: Long, vc: Int)
}

// 执行的时候会看到 不再以实际的3秒为单位进行计算 
// 而是等待数据中的时间戳过了三秒钟才会进行一次计算

// 但是 这里最开始的时候会出现一个问题
// 例如第一条数据的时间戳是1549044122 则应该在1549044125时候触发计算
// 但是我们传进去1549044123这样的数据也会触发计算 这个问题在下下个代码块中解决.
```

对比代码 如果不指定EventTime 则会以实际中的3秒钟为窗口进行计算

```scala
package com.atguigu.bigdata.flink.chapter06

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Flink09_Window_ProcessTime {
    
    def main(args: Array[String]): Unit = {
        
        // TODO Watermark
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        
        val socketDS: DataStream[String] = env.socketTextStream("localhost", 9999)
        
        val sensorDS = socketDS.map(
            data => {
                val datas = data.split(",")
                WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
            }
            )
        val processDS: DataStream[String] = sensorDS
                .keyBy(_.id)
                .timeWindow(Time.seconds(3))
                .process(
                    new ProcessWindowFunction[WaterSensor, String, String, TimeWindow] {
                        override def process(key: String, context: Context, elements: Iterable[WaterSensor], out: Collector[String]): Unit = {
                            out.collect("窗口被触发计算了。。。")
                        }
                    }
                    )
        
        processDS.print("et>>>>")
        
        env.execute()
    }
    case class WaterSensor(id: String, ts: Long, vc: Int)
}
```

#### 窗口划分逻辑和源码

以下代码解释了Flink中时间窗口的划分逻辑 源码见下方

```scala
package com.atguigu.bigdata.flink.chapter06

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Flink10_Window_Watermark2 {
    
    def main(args: Array[String]): Unit = {
        
        // TODO Watermark
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        // TODO 设置时间语义为EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        
        val socketDS: DataStream[String] = env.socketTextStream("localhost", 9999)
        
        val sensorDS = socketDS.map(
            data => {
                val datas = data.split(",")
                WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
            }
            )
        // TODO 从数据中抽取EventTime
        // Flink的时间以毫秒为单位
        // 如果假设数据是有序的，那么watermark可以和eventtime关联在一起
        // 这里可以简单的理解为  watermark = eventtime
        val timeDS: DataStream[WaterSensor] = sensorDS.assignAscendingTimestamps(_.ts * 1000L)
        
        // 1. 窗口是如何划分的？
        //    时间取整：timestamp - (timestamp - offset + windowSize) % windowSize;
        //    这个式子的意思是 取这个时间戳的上一个整除窗口的数值 到下一个整除窗口的数值
        //    timestamp : 从数据中抽取的EventTime => 1549044122
        //    offset    : 默认偏移量0
        //    windowSize ：窗口的范围大小 => 5
        //    1549044125 - (1549044125 - 0 + 5) % 5
        //    1549044125 - 1549044130% 5
        //    1549044125 - 0 => 1549044125
        // TODO 窗口范围 : 左闭右开
        //    [1549044120，1549044125) => 1549044122, 1549044123, 1549044124
        //    [1549044125, 1549044130) => 1549044125, 1549044126
        // et: 1549044122 => wm:1549044122
        // et: 1549044123 => wm:1549044123
        // et: 1549044124 => wm:1549044124
        // et: 1549044125 => wm:1549044125
        // 2. 窗口是何时触发计算的？
        //    如果指定的时间标记进入到Flink时，大于等于窗口的结束时间，那么窗口就会被触发计算。
        //    这里的时间标记并不是EventTime,是Watermark
        val processDS: DataStream[String] = timeDS
                .keyBy(_.id)
                .timeWindow(Time.seconds(5))
                .process(
                    new ProcessWindowFunction[WaterSensor, String, String, TimeWindow] {
                        override def process(key: String, context: Context, elements: Iterable[WaterSensor], out: Collector[String]): Unit = {
                            out.collect("窗口被触发计算了。。。")
                        }
                    }
                    )
        
        timeDS.print("time>>>>")
        processDS.print("et>>>>")
        
        env.execute()
    }
    case class WaterSensor(id: String, ts: Long, vc: Int)
}
```

![](https://lormer.cn/20200601123028.png)

#### 读文件时的watermark

读文件的操作和通过别的数据源有所不同

具体如下

```scala
package com.atguigu.bigdata.flink.chapter06

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Flink12_Window_Watermark4 {
    
    def main(args: Array[String]): Unit = {
        
        // TODO Watermark
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        // TODO 设置时间语义为EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        
        // 如果读取文件，由于读取速度很快，所以很快读取到文件末尾
        // (上面这句话是老师在上课时候讲的 但是我认为不是因为读取速度快的问题 而是时间戳的问题 会造成最后的数据可能无法计算)
        // 所以文件的最后一条数据是有可能无法计算的。
        // 为了解决这个问题，读取文件时，如果到达末尾，Flink框架会自动在数据流中增加一个Long最大的Watermark
        // 为了提交所有未计算的窗口进行计算
        // 保证计算结果准确
        val socketDS: DataStream[String] =
        env.readTextFile("input/sensor-data.log")
        //env.socketTextStream("localhost", 9999)
        val sensorDS = socketDS.map(
            data => {
                val datas = data.split(",")
                WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
            }
            )
        
        // TODO 如果数据是有序的 那么可以将eventtime看作为watermark
        // eventtime = watermark(其实有1ms的偏差)
        val timeDS: DataStream[WaterSensor] =
        sensorDS.assignAscendingTimestamps(_.ts * 1000L)
        val processDS: DataStream[String] = timeDS
                .keyBy(_.id)
                .timeWindow(Time.seconds(5))
                .process(
                    new ProcessWindowFunction[WaterSensor, String, String, TimeWindow] {
                        override def process(key: String, context: Context, elements: Iterable[WaterSensor], out: Collector[String]): Unit = {
                            // 这里用context得到当前的watermark 来查看
                            out.collect(
                                "窗口[ " + context.window.getStart
                                        + ", "
                                        + context.window.getEnd
                                        + " ) 在 " + context.currentWatermark + " 触发数据["
                                        + elements.size + "]计算")
                        }
                    }
                    )
        
        timeDS.print("time>>>>")
        processDS.print("et>>>>")
        
        env.execute()
    }
    case class WaterSensor(id: String, ts: Long, vc: Int)
}
```

读别的数据源 得到的结果是如下的 : 

![](https://lormer.cn/20200601130032.png)

读文件  得到结果如下 :

![](https://lormer.cn/20200601130444.png)

#### 乱序数据 分配watermark

```scala
package com.atguigu.bigdata.flink.chapter06

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{OutputTag, _}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Flink13_Window_Watermark5 {
    
    def main(args: Array[String]): Unit = {
        
        // TODO Watermark - 乱序数据 & 迟到数据
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        // TODO 设置时间语义为EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        
        val socketDS: DataStream[String] =
        //env.readTextFile("input/sensor-data.log")
            env.socketTextStream("localhost", 9999)
        
        val sensorDS = socketDS.map(
            data => {
                val datas = data.split(",")
                WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
            }
            )
        
        // TODO 乱序数据需要动态生成watermark
        // BoundedOutOfOrdernessTimestampExtractor
        //  有2个参数列表
        //    第一个参数列表表示延迟时间，为了计算watermark
        //    第二个参数列表表示抽取事件时间。
        // watermark => 延迟时间
        // [20, 25)
        // 25 28 => watermark = eventtime - late time
        // [1549044120,  1549044125) => 1549044122
        // 28 - 3 => 25
        // 本来应该在25做计算 但是现在是
        val timeDS = sensorDS.assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor[WaterSensor](Time.seconds(3)) {
                // 从数据中抽取EventTime
                override def extractTimestamp(element: WaterSensor): Long = {
                    element.ts * 1000L
                }
            }
            )
        
        
        val processDS: DataStream[String] = timeDS
                .keyBy(_.id)
                .timeWindow(Time.seconds(5))
                .process(
                    new ProcessWindowFunction[WaterSensor, String, String, TimeWindow] {
                        override def process(key: String, context: Context, elements: Iterable[WaterSensor], out: Collector[String]): Unit = {
                            
                            out.collect(
                                "窗口[ " + context.window.getStart
                                        + ", "
                                        + context.window.getEnd
                                        + " ) 在 " + context.currentWatermark + " 触发数据["
                                        + elements.size + "]计算")
                        }
                    }
                    )
        
        timeDS.print("time>>>>")
        processDS.print("et>>>>")
        
        env.execute()
    }
    case class WaterSensor(id: String, ts: Long, vc: Int)
}

// 这里面设置了窗口大小为5
// 原本如果没有设置new BoundedOutOfOrdernessTimestampExtractor[WaterSensor](Time.seconds(3))的时候 20-25之间的数据会在25到来的时候进行计算 
// 现在设置了3秒延迟  那28这个时间来的时候才会做计算
// 在这个窗口计算完成之后 如果再来的属于这个窗口的数据已经不能再进行计算

// (班车在20-25之间等人上车 如果不设置延迟 25到点就发车
//   但是现在设置了延迟 3分钟 可以允许晚3分钟内上这趟车  28开车
//   但是如果28之后还想坐这趟车  对不起  车开走了)

// ------------------------------------
// 但是出现的问题就是 有可能真的有重要的数据存在延迟没有进入窗口
// 这时候需要设置一下允许迟到的数据
package com.atguigu.bigdata.flink.chapter06

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{OutputTag, _}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Flink13_Window_Watermark5 {
    
    def main(args: Array[String]): Unit = {
        
        // TODO Watermark - 乱序数据 & 迟到数据
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        // TODO 设置时间语义为EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        
        val socketDS: DataStream[String] =
        //env.readTextFile("input/sensor-data.log")
            env.socketTextStream("localhost", 9999)
        
        val sensorDS = socketDS.map(
            data => {
                val datas = data.split(",")
                WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
            }
            )
        
        // TODO 乱序数据需要动态生成watermark
        // BoundedOutOfOrdernessTimestampExtractor
        //  有2个参数列表
        //    第一个参数列表表示延迟时间，为了计算watermark
        //    第二个参数列表表示抽取事件时间。
        // watermark => 延迟时间
        // [20, 25)
        // 25 28 => watermark = eventtime - late time
        // [1549044120,  1549044125) => 1549044122
        // 28 - 3 => 25
        val timeDS = sensorDS.assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor[WaterSensor](Time.seconds(3)) {
                // 从数据中抽取EventTime
                override def extractTimestamp(element: WaterSensor): Long = {
                    element.ts * 1000L
                }
            }
            )
        // 这个是给侧输出流打标记
        val outputTag = new OutputTag[WaterSensor]("lateData")
        
        val processDS: DataStream[String] = timeDS
                .keyBy(_.id)
                .timeWindow(Time.seconds(5))
                .allowedLateness(Time.seconds(2)) // 允许迟到数据
                .sideOutputLateData(outputTag) // 迟到数据的侧输出流
                .process(
                    new ProcessWindowFunction[WaterSensor, String, String, TimeWindow] {
                        override def process(key: String, context: Context, elements: Iterable[WaterSensor], out: Collector[String]): Unit = {
                            
                            out.collect(
                                "窗口[ " + context.window.getStart
                                        + ", "
                                        + context.window.getEnd
                                        + " ) 在 " + context.currentWatermark + " 触发数据["
                                        + elements.size + "]计算")
                        }
                    }
                    )
        
        timeDS.print("time>>>>")
        processDS.print("et>>>>")
        // 从侧输出流中获取迟到数据
        processDS.getSideOutput(outputTag).print("late>>>>")
        
        env.execute()
    }
    case class WaterSensor(id: String, ts: Long, vc: Int)
}


// 这里面加了两句
// .allowedLateness(Time.seconds(2)) 允许数据迟到两秒
// 例如 21 22 25 (25本来应该触发计算 关闭窗口 因为上面设置了3秒延迟 推迟到28计算)
// 28(28来了之后 数据会进行第一次计算)
// 21(21来了 数据会进行第二次计算) 23(数据会进行第三次计算)
// 直到30来了(因为25 watermark延迟3 再allowedLateness允许迟到2 加起来是30) 
// 才会真正关闭该计算窗口
// 后面再来20-25之间的数  就不能进入该窗口了

// 如果上面两个措施之后 还有没到的数据
// 就把迟到的数据放到侧输出流中 数据不会丢失
// .sideOutputLateData(outputTag) // 迟到数据的侧输出流
```

#### watermark总结

```scala
package com.atguigu.bigdata.flink.chapter06

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{OutputTag, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Flink14_Window_Watermark6 {
    
    def main(args: Array[String]): Unit = {
        
        // TODO Watermark - 总结
        // 1. 如果想要对数据的窗口进行计算，一般采用事件时间
        // 2. 需要从数据中抽取事件时间EventTime，并且同时生成Watermark
        //    watermark = eventtime(有序)
        //    watermark = eventtime - late time(乱序)
        //    watermark = eventtime - late time - delay time(乱序)
        // 3. 根据watermark触发数据窗口的计算
        // 4. 使用allowedLateness方法增加延迟时间
        // 5. 使用sideOutputLateData方法保存迟到数据
        val env: StreamExecutionEnvironment =
        StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        // TODO 设置时间语义为EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        
        val socketDS: DataStream[String] =
        //env.readTextFile("input/sensor-data.log")
            env.socketTextStream("localhost", 9999)
        
        val sensorDS: DataStream[WaterSensor] = socketDS.map(
            data => {
                val datas: Array[String] = data.split(",")
                WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
            })
        
        val timeDS: DataStream[WaterSensor] = sensorDS.assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor[WaterSensor](Time.seconds(3)) {
                // 从数据中抽取EventTime
                override def extractTimestamp(element: WaterSensor): Long = {
                    element.ts * 1000L
                }
            })
        
        val outputTag = new OutputTag[WaterSensor]("lateData")
        
        val processDS: DataStream[String] = timeDS
                .keyBy(_.id)
                .timeWindow(Time.seconds(5))
                .allowedLateness(Time.seconds(2)) // 允许迟到数据
                .sideOutputLateData(outputTag) // 迟到数据的侧输出流
                .process(
                    new ProcessWindowFunction[WaterSensor, String, String, TimeWindow] {
                        override def process(key: String, context: Context, elements: Iterable[WaterSensor], out: Collector[String]): Unit = {
                            
                            out.collect(
                                "窗口[ " + context.window.getStart
                                        + ", "
                                        + context.window.getEnd
                                        + " ) 在 " + context.currentWatermark + " 触发数据["
                                        + elements.size + "]计算")
                        }
                    })
        
        timeDS.print("time>>>>")
        processDS.print("et>>>>")
        // 从侧输出流中获取迟到数据
        processDS.getSideOutput(outputTag).print("late>>>>")
        
        env.execute()
    }
    case class WaterSensor(id: String, ts: Long, vc: Int)
}
```

#### 并行度与Watermark

```scala
package com.atguigu.bigdata.flink.chapter06

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Flink16_Window_Watermark8 {
    
    def main(args: Array[String]): Unit = {
        
        // TODO Watermark
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        // TODO 设置时间语义为EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        
        val socketDS: DataStream[String] =
        //env.readTextFile("input/sensor-data.log")
            env.socketTextStream("localhost", 9999)
        
        val sensorDS = socketDS.map(
            data => {
                val datas = data.split(",")
                WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
            }
            )
        
        val timeDS = sensorDS.assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor[WaterSensor](Time.seconds(3)) {
                // 从数据中抽取EventTime
                override def extractTimestamp(element: WaterSensor): Long = {
                    element.ts * 1000L
                }
            }
            )
        
        val processDS: DataStream[String] = timeDS
                .keyBy(_.id)
                .timeWindow(Time.seconds(5))
                .process(
                    new ProcessWindowFunction[WaterSensor, String, String, TimeWindow] {
                        override def process(key: String, context: Context, elements: Iterable[WaterSensor], out: Collector[String]): Unit = {
                            
                            out.collect(
                                "窗口[ " + context.window.getStart
                                        + ", "
                                        + context.window.getEnd
                                        + " ) 在 " + context.currentWatermark + " 触发数据["
                                        + elements + "]计算")
                        }
                    }
                    )
        
        timeDS.print("time>>>>")
        processDS.print("et>>>>")
        
        env.execute()
    }
    case class WaterSensor(id: String, ts: Long, vc: Int)
}

// 一辆车不够坐 要开两辆车
// 但是两辆车要同时开车  需要同时满足条件 
// 28这个数据来的时候 第一个分区中水位线最大的是25了 第一个分区满足条件了
// 但是第二个分区中数据是23 水位线是20 我们要按照数值小的作为分区水位线
// 所以此时并不能触发

```

以下演示的是有序数据来的时候的情况

![](https://lormer.cn/20200601193647.png)

![](https://lormer.cn/20200601193718.png)

![](https://lormer.cn/20200601194729.png)

如果是乱序的数据

![](https://lormer.cn/20200601195225.png)

> 假如是25先来 第一个分区水位线目前是22 然后第二个分区来了22 水位线是19 则整个流中的水位线按最小的19来计算
>
> 现在第一个分区又来了一条数据21 水位线是18 但是第一个分区中水位线已经到了22 watermark只增不减





#### 周期性生成watermark

```scala
package com.atguigu.bigdata.flink.chapter06

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Flink17_Window_Watermark9 {
    
    def main(args: Array[String]): Unit = {
        
        // TODO Watermark
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        // TODO 设置时间语义为EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        // 设定watermark数据生成的周期 默认是200ms
        env.getConfig.setAutoWatermarkInterval(1000 * 10)
        
        val socketDS: DataStream[String] =
        //env.readTextFile("input/sensor-data.log")
            env.socketTextStream("localhost", 9999)
        
        val sensorDS: DataStream[WaterSensor] = socketDS.map(
            data => {
                val datas: Array[String] = data.split(",")
                WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
            }
            )
        
        // 周期性的生成watermark: BoundedOutOfOrdernessTimestampExtractor
        // Flink框架默认200ms生成watermark
        // 如果想要自定义生成watermark，需要自行创建对应的类
        // 1. AssignerWithPeriodicWatermarks
        // 传泛型 重写两个方法 
        val timeDS: DataStream[WaterSensor] = sensorDS.assignTimestampsAndWatermarks(
            // TODO 周期性生成watermark
            new AssignerWithPeriodicWatermarks[WaterSensor] {
                
                private var currentTs = 0L
                // 获得当前水位标记
                override def getCurrentWatermark: Watermark = {
                    // 周期性生成watermark数据
                    // 通过这句输出可以看到 即使没有数据 也会打印
                    // 是周期性进行调用 获取当前的Watermark
                    println("getCurrentWatermark...")
                    // watermark = eventtime - late time
                    // 水位线（水印）只增不减
                    new Watermark(currentTs - 3000)
                }
                // 抽取时间戳
                override def extractTimestamp(element: WaterSensor, previousElementTimestamp: Long): Long = {
                    currentTs = currentTs.max(element.ts * 1000L)
                    element.ts * 1000L
                }
            }
            // TODO 无序数据生成watermark的工具类
            //            new BoundedOutOfOrdernessTimestampExtractor[WaterSensor]( Time.seconds(3) ) {
            //                // 从数据中抽取EventTime
            //                override def extractTimestamp(element: WaterSensor): Long = {
            //                    element.ts * 1000L
            //                }
            //            }
            )
        
        val processDS: DataStream[String] = timeDS
                .keyBy(_.id)
                .timeWindow(Time.seconds(5))
                .process(
                    new ProcessWindowFunction[WaterSensor, String, String, TimeWindow] {
                        override def process(key: String, context: Context, elements: Iterable[WaterSensor], out: Collector[String]): Unit = {
                            
                            out.collect(
                                "窗口[ " + context.window.getStart
                                        + ", "
                                        + context.window.getEnd
                                        + " ) 在 " + context.currentWatermark + " 触发数据["
                                        + elements + "]计算")
                        }
                    }
                    )
        
        timeDS.print("time>>>>")
        processDS.print("et>>>>")
        
        env.execute()
    }
    case class WaterSensor(id: String, ts: Long, vc: Int)
}
```

![](https://lormer.cn/20200601220352.png)

#### 间歇性生成watermark

```scala
package com.atguigu.bigdata.flink.chapter06

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Flink17_Window_Watermark9 {
    
    def main(args: Array[String]): Unit = {
        
        // TODO Watermark
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        // TODO 设置时间语义为EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        // 设定watermark数据生成的周期
        env.getConfig.setAutoWatermarkInterval(1000 * 10)
        
        val socketDS: DataStream[String] =
        //env.readTextFile("input/sensor-data.log")
            env.socketTextStream("localhost", 9999)
        
        val sensorDS: DataStream[WaterSensor] = socketDS.map(
            data => {
                val datas: Array[String] = data.split(",")
                WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
            }
            )
        
        // 周期性的生成watermark: BoundedOutOfOrdernessTimestampExtractor
        // Flink框架默认200ms生成watermark
        // env.getConfig.setAutoWatermarkInterval(1000 * 10) 控制了生成watermark的间隔
        // 如果想要自定义生成watermark，需要自行创建对应的类
        // 1. AssignerWithPeriodicWatermarks
        // 2. AssignerWithPunctuatedWatermarks
        val timeDS: DataStream[WaterSensor] = sensorDS.assignTimestampsAndWatermarks(
            // TODO 间歇性生成watermark
            new AssignerWithPunctuatedWatermarks[WaterSensor] {
                override def checkAndGetNextWatermark(lastElement: WaterSensor, extractedTimestamp: Long): Watermark = {
                    // 为了验证间歇性
                    // 有数据的时候会触发获取Watermark
                    println("checkAndGetNextWatermark...")
                    // 把抽取出来的时间戳当成Watermark
                    new Watermark(extractedTimestamp)
                }
                
                // 抽取时间戳
                override def extractTimestamp(element: WaterSensor, previousElementTimestamp: Long): Long = {
                    element.ts * 1000L
                }
            })
        
        val processDS: DataStream[String] = timeDS
                .keyBy(_.id)
                .timeWindow(Time.seconds(5))
                .process(
                    new ProcessWindowFunction[WaterSensor, String, String, TimeWindow] {
                        override def process(key: String, context: Context, elements: Iterable[WaterSensor], out: Collector[String]): Unit = {
                            
                            out.collect(
                                "窗口[ " + context.window.getStart
                                        + ", "
                                        + context.window.getEnd
                                        + " ) 在 " + context.currentWatermark + " 触发数据["
                                        + elements + "]计算")
                        }
                    }
                    )
        
        timeDS.print("time>>>>")
        processDS.print("et>>>>")
        
        env.execute()
    }
    case class WaterSensor(id: String, ts: Long, vc: Int)
}
```

### 6.2.3 EventTime在window中的使用

#### 滚动窗口(TumblingEventTimeWindows)

```scala
val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
env.setParallelism(1)
val dataDS: DataStream[String] = env.readTextFile("input/data.txt")

// TODO 将数据进行转换
val mapDS = dataDS.map(data => {
    val datas = data.split(",")
    (datas(0), datas(1).toLong, datas(2).toInt)
})

val waterDS = mapDS.assignTimestampsAndWatermarks(
    new BoundedOutOfOrdernessTimestampExtractor[(String, Long, Int)](Time.seconds(0)) {
        override def extractTimestamp(element: (String, Long, Int)): Long = element._2 * 1000
    }
)

val resultDS = waterDS.keyBy(0)
    .window(TumblingEventTimeWindows.of(Time.seconds(5)))
    .reduce(
        (t1, t2) => {
            (t1._1, t1._2, math.max(t1._3, t2._3))
        }
    )
mapDS.print("water")
resultDS.print("result")
env.execute("sensor")
```

这个其实就是底层实现的代码放在表层使用了

`.timeWindow(Time.seconds(5))`

如果当前是EventTime的情况下使用的就是

`.window(TumblingEventTimeWindows.of(Time.seconds(5)))`

![](https://lormer.cn/20200602000629.png)

#### 滑动窗口(SlidingEventTimeWindows)

```scala
val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
env.setParallelism(1)

val dataDS: DataStream[String] = env.readTextFile("input/data.txt")

val mapDS = dataDS.map(data => {
    val datas = data.split(",")
    (datas(0), datas(1).toLong, datas(2).toInt)
})

val waterDS = mapDS.assignTimestampsAndWatermarks(
    new BoundedOutOfOrdernessTimestampExtractor[(String, Long, Int)](Time.seconds(0)) {
        override def extractTimestamp(element: (String, Long, Int)): Long = element._2 * 1000
    }
)

val resultDS = waterDS.keyBy(0)
    .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)))
    .reduce(
        (t1, t2) => {
            (t1._1, t1._2, math.max(t1._3, t2._3))
        }
    )
mapDS.print("water")
resultDS.print("result>>>>>")
env.execute("sensor")
```

`.window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)))`

其实就是

`.timeWindow(Time.seconds(5), Time.seconds(1)))`的底层实现代码

![](https://lormer.cn/20200602000504.png)

#### 会话窗口（EventTimeSessionWindows）

相邻两次数据的EventTime的时间差超过指定的时间间隔就会触发执行。如果加入Watermark，会在符合窗口触发的情况下进行延迟。到达延迟水位再进行窗口触发。

```scala
val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
env.setParallelism(1)

val dataDS: DataStream[String] = env.readTextFile("input/data1.txt")

val mapDS = dataDS.map(data => {
    val datas = data.split(",")
    (datas(0), datas(1).toLong, datas(2).toInt)
})

val waterDS = mapDS.assignTimestampsAndWatermarks(
    new BoundedOutOfOrdernessTimestampExtractor[(String, Long, Int)](Time.seconds(0)) {
        override def extractTimestamp(element: (String, Long, Int)): Long = element._2 * 1000
    }
)

val resultDS = waterDS.keyBy(0)
    .window(EventTimeSessionWindows.withGap(Time.seconds(2)))
    .reduce(
        (t1, t2) => {
            (t1._1, t1._2, math.max(t1._3, t2._3))
        }
    )
mapDS.print("water")
resultDS.print("result>>>>>")
env.execute("sensor")
```

![](https://lormer.cn/20200602000935.png)

`window()`函数中需要传一个WindowAssigner的实现类 实现类如下

![](https://lormer.cn/20200602000903.png)







## **6.3 ProcessFunction API**

我们之前学习的转换算子是无法访问事件的时间戳信息和水位线信息的。而这在一些应用场景下，极为重要。例如MapFunction这样的map转换算子就无法访问时间戳或者当前事件的事件时间。

基于此，DataStream API提供了一系列的Low-Level(低阶)转换算子(更偏向于底层)。可以访问时间戳、watermark以及注册定时事件。还可以输出特定的一些事件，例如超时事件等。Process Function用来构建事件驱动的应用以及实现自定义的业务逻辑(使用之前的window函数和转换算子无法实现)。例如，Flink SQL就是使用Process Function实现的。

Flink提供了8个Process Function：

- ProcessFunction
- KeyedProcessFunction
- CoProcessFunction
- ProcessJoinFunction
- BroadcastProcessFunction
- KeyedBroadcastProcessFunction
- ProcessWindowFunction
- ProcessAllWindowFunction

### 6.3.1 KeyedProcessFunction

KeyedProcessFunction用来操作KeyedStream。KeyedProcessFunction会处理流的每一个元素，输出为0个、1个或者多个元素。所有的Process Function都继承自RichFunction接口，所以都有`open()`、`close()`和`getRuntimeContext()`等方法。而`KeyedProcessFunction[KEY, IN, OUT]`还额外提供了两个方法:

- `processElement(v: IN, ctx: Context, out: Collector[OUT])`
  - 流中的每一个元素都会调用这个方法，调用结果将会放在Collector数据类型中输出。
  - Context可以访问元素的时间戳，元素的key，以及TimerService时间服务。
  - Context还可以将结果输出到别的流(side outputs)。

- `onTimer(timestamp: Long, ctx: OnTimerContext, out: Collector[OUT])`
  - 是一个回调函数。当之前注册的定时器触发时调用。
  - 参数timestamp 为定时器所设定的触发的时间戳。
  - Collector 为输出结果的集合。
  - OnTimerContext 和 processElement 的 Context 参数一样，提供了上下文的一些信息，例如定时器触发的时间信息(事件时间或者处理时间)。

```scala
// ----- 课件 -----
val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
env.setParallelism(1)

val dataDS: DataStream[String] = env.readTextFile("input/data1.txt")

val mapDS: DataStream[(String, Long, Int)] = dataDS.map(data => {
    val datas = data.split(",")
    (datas(0), datas(1).toLong, datas(2).toInt)
})

mapDS.keyBy(0)
        .process(
            new KeyedProcessFunction[Tuple,(String, Long, Int), String]{
                override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, (String, Long, Int), String]#OnTimerContext, out: Collector[String]): Unit = super.onTimer(timestamp, ctx, out)
                
override def processElement(value: (String, Long, Int), ctx: KeyedProcessFunction[Tuple, (String, Long, Int), String]#Context, out: Collector[String]): Unit = {
                    println(ctx.getCurrentKey)
                    out.collect(value.toString())
                }
            }
        ).print("keyprocess:")

```

#### ctx上下文环境可以获取多种参数

```scala
// ----- 课堂 -----
package com.atguigu.bigdata.flink.chapter06

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object Flink19_ProcessFunction_Keyed {
    
    def main(args: Array[String]): Unit = {
        
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        
        val socketDS: DataStream[String] =
            env.readTextFile("input/sensor-data.log")
        //env.socketTextStream("localhost", 9999)
        val sensorDS: DataStream[WaterSensor] = socketDS.map(
            data => {
                val datas: Array[String] = data.split(",")
                WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
            }
            )
        
        val processDS: DataStream[String] =
            sensorDS
                    .keyBy(_.id)
                    .process(
                        new KeyedProcessFunction[String, WaterSensor, String] {
                            // TODO 处理元素（数据）：来一条数据处理一条数据
                            // value : 采集到的数据
                            // ctx : 表示上下文环境
                            // out : 将处理结果输出到下游的数据流中
                            override def processElement(value: WaterSensor, ctx: KeyedProcessFunction[String, WaterSensor, String]#Context, out: Collector[String]): Unit = {
                                out.collect(value.toString)
                                
                                // 获取时间戳
                                //ctx.timestamp()
                                // 获取当前处理的key
                                //ctx.getCurrentKey
                                // 将数据输出到侧输出流 需要传一个标记
                                //ctx.output()
                                // 获取定时器服务
                                //ctx.timerService()
                                // 注册定时器（EventTime）传参Time
                                //ctx.timerService().registerEventTimeTimer()
                                // 注册定时器（ProcessingTime） 传参Time
                                //ctx.timerService().registerProcessingTimeTimer()
                                // 删除定时器（EventTime）
                                //ctx.timerService().deleteEventTimeTimer()
                                // 删除定时器（ProcessingTime）
                                //ctx.timerService().deleteProcessingTimeTimer()
                                // 当前水位标记
                                //ctx.timerService().currentWatermark()
                                // 当前处理时间
                                //ctx.timerService().currentProcessingTime()
                            }
                        }
                        )
        // 获取侧输出流
        //processDS.getSideOutput()
        processDS.print("process>>>")
        env.execute()
    }
    case class WaterSensor(id: String, ts: Long, vc: Int)
}
```

#### Timer

测试获取定时器

`ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime())`

并回调`onTimer`方法

```scala
package com.atguigu.bigdata.flink.chapter06

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object Flink20_ProcessFunction_Timer {

    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val socketDS: DataStream[String] =
        //env.readTextFile("input/sensor-data.log")
            env.socketTextStream("localhost", 9999)

        val sensorDS = socketDS.map(
            data => {
                val datas = data.split(",")
                WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
            }
        )

        val processDS: DataStream[String] =
            sensorDS.keyBy(_.id)
                    .process(
                        new KeyedProcessFunction[String, WaterSensor, String] {

                            override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, WaterSensor, String]#OnTimerContext, out: Collector[String]): Unit = {
                                println("定时器执行了")
                            }

                            override def processElement(value: WaterSensor, ctx: KeyedProcessFunction[String, WaterSensor, String]#Context, out: Collector[String]): Unit = {
                                // 增加定时器,在指定的时间执行某个操作
                                // 如果定时器的时间到达时，会由Flink框架自动触发执行。
                                // 自动调用onTimer方法
                                ctx.timerService().registerProcessingTimeTimer(
                                    // 把当前处理时间当成注册定时器的时间并调用方法
                                    ctx.timerService().currentProcessingTime()
                                )
                                out.collect(value.toString)
                            }
                        }
                    )
        // 获取侧输出流
        //processDS.getSideOutput()
        processDS.print("process>>>")
        env.execute()
    }

    case class WaterSensor(id: String, ts: Long, vc: Int)

}
```

### 6.3.2 TimerService 和 定时器（Timers）

Context和OnTimerContext所持有的TimerService对象拥有以下方法:

- `currentProcessingTime(): Long` 返回当前处理时间
- `currentWatermark(): Long` 返回当前watermark的时间戳
- `registerProcessingTimeTimer(timestamp: Long): Unit` 会注册当前key的processing time的定时器。当processing time到达定时时间时，触发timer。
- `registerEventTimeTimer(timestamp: Long): Unit` 会注册当前key的event time 定时器。当水位线大于等于定时器注册的时间时，触发定时器执行回调函数。
- `deleteProcessingTimeTimer(timestamp: Long): Unit` 删除之前注册处理时间定时器。如果没有这个时间戳的定时器，则不执行。
- `deleteEventTimeTimer(timestamp: Long): Unit`删除之前注册的事件时间定时器，如果没有此时间戳的定时器，则不执行。

当定时器timer触发时，会执行回调函数`onTimer()`。注意定时器timer只能在keyed streams上面使用。

```scala
package com.atguigu.bigdata.flink.chapter06

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object Flink21_ProcessFunction_Timer_EventTime {

    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val socketDS: DataStream[String] =
        //env.readTextFile("input/sensor-data.log")
            env.socketTextStream("localhost", 9999)

        val sensorDS = socketDS.map(
            data => {
                val datas = data.split(",")
                WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
            }
        )

        // 有序数据的场合，生成Watermark
        // watermark = eventtime - 1ms
        val timeDS = sensorDS.assignAscendingTimestamps(_.ts * 1000L)

        val processDS: DataStream[String] =
            timeDS.keyBy(_.id)
                    .process(
                        new KeyedProcessFunction[String, WaterSensor, String] {

                            override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, WaterSensor, String]#OnTimerContext, out: Collector[String]): Unit = {
                                println("定时器触发时间点："
                                        + timestamp
                                        + ", watermark:"
                                        + ctx.timerService().currentWatermark())
                            }

                            // TODO 只要在程序中使用了EventTime的时间语义
                            // 那么窗口计算和定时器的触发全部都依赖于watermark
                            override def processElement(value: WaterSensor, ctx: KeyedProcessFunction[String, WaterSensor, String]#Context, out: Collector[String]): Unit = {
                                ctx.timerService().registerEventTimeTimer(
                                    // 把当前sensor中的时间戳加上5s作为定时器时间
                                    value.ts * 1000L + 5000L
                                )
                                out.collect(value.toString)
                            }
                        }
                    )
        // 获取侧输出流
        //processDS.getSideOutput()
        processDS.print("process>>>")
        env.execute()
    }

    case class WaterSensor(id: String, ts: Long, vc: Int)

}

// 测试发现传22 没有触发 传27 也没有触发(按照逻辑应该是时间戳+5s触发 但是这里没有触发)
// 传28触发 发现此时定时器设置的时间是27000 此时的水位标记是 27999

// 下图为源码分析 图片有点大 短链接为https://imgchr.com/i/tNFref
```

![tNFref.png](https://lormer.cn/tNFref.png)



#### 小练习:如何实现watermark与EventTime相同

```scala
package com.atguigu.bigdata.flink.chapter06

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Flink23_Window_Calc {

    def main(args: Array[String]): Unit = {

        // TODO 窗口计算
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        // TODO 设置时间语义为EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        // 设定watermark数据生成的周期
        //env.getConfig.setAutoWatermarkInterval(1000 * 10)

        val socketDS: DataStream[String] =
        //env.readTextFile("input/sensor-data.log")
            env.socketTextStream("localhost", 9999)

        val sensorDS = socketDS.map(
            data => {
                val datas = data.split(",")
                WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
            }
        )

        // 对于有序的数据 如果用assignAscendingTimestamps就是使用框架给的watermark计算方式
        // 这时候watermark = EventTime - 1ms
        // 而我们如果想让数据的watermark跟EventTime相同 需要进行自定义
        // 需要使用assignTimestampsAndWatermarks 手动分配时间戳和水位标记
        //val timeDS = sensorDS.assignAscendingTimestamps(_.ts)
        val timeDS = sensorDS.assignTimestampsAndWatermarks(
            new AssignerWithPunctuatedWatermarks[WaterSensor] {
                // 这个方法为了获取Watermark 把时间戳传了进去 这样就完成了自定义操作
                override def checkAndGetNextWatermark(lastElement: WaterSensor, extractedTimestamp: Long): Watermark = {
                    new Watermark(extractedTimestamp)
                }

                // 这个方法为了抽取时间戳 指定每条数据中哪个字段被定为时间戳
                override def extractTimestamp(element: WaterSensor, previousElementTimestamp: Long): Long = {
                    element.ts
                }
            }
        )

        val processDS: DataStream[String] = timeDS
                .keyBy(_.id)
                .timeWindow(Time.seconds(5))
                .process(
                    new ProcessWindowFunction[WaterSensor, String, String, TimeWindow] {
                        override def process(key: String, context: Context, elements: Iterable[WaterSensor], out: Collector[String]): Unit = {

                            out.collect(
                                "窗口[ " + context.window.getStart
                                        + ", "
                                        + context.window.getEnd
                                        + " ) 在 " + context.currentWatermark + " 触发数据["
                                        + elements + "]计算")
                        }
                    }
                )

        timeDS.print("time>>>>")
        processDS.print("et>>>>")

        env.execute()
    }

    case class WaterSensor(id: String, ts: Long, vc: Int)

}

// 这时候 输入24999就能触发计算了 因为输入时间戳是24999 watermark也是24999 满足了>=end-1
```

#### 需求实现 : 连续5分钟水位上升

需求：监控水位传感器的水位值，如果水位值在五分钟之内(processing time)连续上升，则报警。

```scala
package com.atguigu.bigdata.flink.chapter06

import java.sql.Timestamp
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPunctuatedWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.util.Collector

object Flink24_Req_LevelUp {

    def main(args: Array[String]): Unit = {

        // TODO 水位值连续5s上涨，需要报警
        // 获取传感器的每一条数据，然后根据时间差进行处理和判断
        // 1.   22  10
        // 2.   23  11
        // 3.   23.2  10
        // 3.   24  12
        // 4.   25  14
        // 5.   26  15
        // 6.   27  16
        // 7.   28  17
        val env: StreamExecutionEnvironment =
        StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        // TODO 设置时间语义为EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val socketDS: DataStream[String] =
        //env.readTextFile("input/sensor-data.log")
            env.socketTextStream("localhost", 9999)

        val sensorDS = socketDS.map(
            data => {
                val datas = data.split(",")
                WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
            }
        )

        val timeDS = sensorDS.assignTimestampsAndWatermarks(
            new AssignerWithPunctuatedWatermarks[WaterSensor] {
                override def checkAndGetNextWatermark(lastElement: WaterSensor, extractedTimestamp: Long): Watermark = {
                    new Watermark(extractedTimestamp)
                }

                override def extractTimestamp(element: WaterSensor, previousElementTimestamp: Long): Long = {
                    element.ts * 1000L
                }
            }
        )

        val processDS = timeDS
                .keyBy(_.id)
                .process(new MyKeyedProcessFunction)

        timeDS.print("time>>>>")
        processDS.print("process>>>>")

        env.execute()
    }

    // 自定义数据处理函数
    class MyKeyedProcessFunction extends KeyedProcessFunction[String, WaterSensor, String] {
        private var currentHeight = 0L
        private var alarmTimer = 0L

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, WaterSensor, String]#OnTimerContext, out: Collector[String]): Unit = {
            out.collect("水位传感器" + ctx.getCurrentKey + "在 " + new Timestamp(timestamp) + "已经连续5s水位上涨。。。")
        }

        // 分区中每来一条数据就会调用processElement方法
        override def processElement(value: WaterSensor, ctx: KeyedProcessFunction[String, WaterSensor, String]#Context, out: Collector[String]): Unit = {

            // 判断当前水位值和之前记录的水位值的变化
            if (value.vc > currentHeight) {
                // 当水位值上升的时候，开始计算时间，如果到达5s，
                // 中间水位没有下降，那么定时器应该执行
                if (alarmTimer == 0) {
                    alarmTimer = value.ts * 1000 + 5000
                    ctx.timerService().registerEventTimeTimer(alarmTimer)
                }
            } else {
                // 水位下降的场合
                // 删除定时器处理
                ctx.timerService().deleteEventTimeTimer(alarmTimer)
                alarmTimer = 0L
            }

            // 保存当前水位值
            currentHeight = value.vc
        }
    }

    case class WaterSensor(id: String, ts: Long, vc: Int)

}
```



### 6.3.3 侧输出流（SideOutput）

大部分的DataStream API的算子的输出是单一输出，也就是某种数据类型的流。除了split算子，可以将一条流分成多条流，这些流的数据类型也都相同。process function的side outputs功能可以产生多条流，并且这些流的数据类型可以不一样。一个side output可以定义为OutputTag[X]对象，X是输出流的数据类型。process function可以通过Context对象发送一个事件到一个或者多个side outputs。

小练习：采集监控传感器水位值，将水位值高于5cm的值输出到side output。

```scala
// ----- 课件 ------
class MyHighLevelAlarm extends ProcessFunction[(String, Long, Int),(String, Long, Int)]{

    private lazy val highLevelVal = new OutputTag[Long]("highLevel");
    //private lazy val highLevelVal: OutputTag[Int] = new OutputTag[Int]("highLevel")

    override def processElement(value: (String, Long, Int), ctx: ProcessFunction[(String, Long, Int), (String, Long, Int)]#Context, out: Collector[(String, Long, Int)]): Unit = {
        if ( value._3 > 5 ) {
            ctx.output(highLevelVal, value._2)
        }
        out.collect(value)
    }
}

// -----------
val value: DataStream[(String, Long, Int)] = eventDS.keyBy(0).process(new MyHighLevelAlarm)
value.print("keyprocess:")
value.getSideOutput(new OutputTag[Long]("highLevel")).print("high")
```

#### 需求实现 : 水位高于5cm输出到侧输出流

```scala
package com.atguigu.bigdata.flink.chapter06

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPunctuatedWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.util.Collector

object Flink25_Req_LevelHigh {

    def main(args: Array[String]): Unit = {

        // TODO 水位高度大于5厘米输出到侧输出流中
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        // TODO 设置时间语义为EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val socketDS: DataStream[String] =
        //env.readTextFile("input/sensor-data.log")
            env.socketTextStream("localhost", 9999)

        val sensorDS = socketDS.map(
            data => {
                val datas = data.split(",")
                WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
            }
        )

        val timeDS = sensorDS.assignTimestampsAndWatermarks(
            new AssignerWithPunctuatedWatermarks[WaterSensor] {
                override def checkAndGetNextWatermark(lastElement: WaterSensor, extractedTimestamp: Long): Watermark = {
                    new Watermark(extractedTimestamp)
                }

                override def extractTimestamp(element: WaterSensor, previousElementTimestamp: Long): Long = {
                    element.ts * 1000L
                }
            }
        )

        val processDS = timeDS
                .keyBy(_.id)
                .process(new MyKeyedProcessFunction)

        //timeDS.print("time>>>>")
        processDS.print("process>>>>")
        // 从流中获取侧输出流数据
        val outputTag = new OutputTag[WaterSensor]("high")
        processDS.getSideOutput(outputTag).print("side>>>>")

        env.execute()
    }

    // 自定义数据处理函数
    class MyKeyedProcessFunction extends KeyedProcessFunction[String, WaterSensor, String] {

        // 分区中每来一条数据就会调用processElement方法
        override def processElement(value: WaterSensor, ctx: KeyedProcessFunction[String, WaterSensor, String]#Context, out: Collector[String]): Unit = {
            // 判断水位高度
            // 如果水位高度大于5cm,那么将数据输出到侧输出流中
            // out用于输出正常流
            if (value.vc >= 5) {
                // 定义侧输出流标记
                val outputTag = new OutputTag[WaterSensor]("high")
                ctx.output(outputTag, value)
            } else {
                out.collect(value + "水位值处于正常范围")
            }
        }
    }

    case class WaterSensor(id: String, ts: Long, vc: Int)

}
```



### 6.3.4 CoProcessFunction

对于两条输入流，DataStream API提供了CoProcessFunction这样的low-level操作。

CoProcessFunction提供了操作每一个输入流的方法: processElement1()和processElement2()。

类似于ProcessFunction，这两种方法都通过Context对象来调用。这个Context对象可以访问事件数据，定时器时间戳，TimerService，以及side outputs。

CoProcessFunction也提供了`onTimer()`回调函数。

```scala
// ----- 课件 -----
val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

val dataDS: DataStream[String] = env.readTextFile("input/data.txt")
val splitDS: SplitStream[WaterSensor] = dataDS.map(
    s => {
        val datas = s.split(",")
        WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
    }
).split(
    sensor => {
        if (sensor.vc >= 40) {
            Seq("alarm")
        } else if (sensor.vc >= 30) {
            Seq("warn")
        } else {
            Seq("normal")
        }
    }
)

val alarmDS: DataStream[WaterSensor] = splitDS.select("alarm")
val warnDS: DataStream[WaterSensor] = splitDS.select("warn")
val normalDS: DataStream[WaterSensor] = splitDS.select("normal")

val connectDS: ConnectedStreams[WaterSensor, WaterSensor] = alarmDS.connect(warnDS)

connectDS.process(new CoProcessFunction[WaterSensor, WaterSensor, WaterSensor] {
    override def processElement1(value: WaterSensor, ctx: CoProcessFunction[WaterSensor, WaterSensor, WaterSensor]#Context, out: Collector[WaterSensor]): Unit = {
        out.collect(value)
    }

    override def processElement2(value: WaterSensor, ctx: CoProcessFunction[WaterSensor, WaterSensor, WaterSensor]#Context, out: Collector[WaterSensor]): Unit = {
        out.collect(value)
    }
})

env.execute()
```

#### 小案例 : 分析改变并行度之后的coProcessFunction

```scala
package com.atguigu.bigdata.flink.chapter06

import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable

object Flink26_ProcessFunction_Connect {

    def main(args: Array[String]): Unit = {

        // TODO 案例实操 - 需求 - 订单交易匹配
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(3)

        val logDS1: DataStream[String] = env.readTextFile("input/OrderLog.csv")
        val orderDS = logDS1.map(
            log => {
                val datas = log.split(",")
                OrderEvent(
                    datas(0).toLong,
                    datas(1),
                    datas(2),
                    datas(3).toLong
                )
            }
        )
        val orderKS: KeyedStream[OrderEvent, String] = orderDS.keyBy(_.txId)

        val logDS2: DataStream[String] = env.readTextFile("input/ReceiptLog.csv")
        val txDS = logDS2.map(
            log => {
                val datas = log.split(",")
                TxEvent(
                    datas(0),
                    datas(1),
                    datas(2).toLong
                )
            }
        )
        // 如果并行度为1 则不需要keyBy之后也可以得到正确结果
        // 如果我们设置了并行度>1 这时候相同id的订单可能被分到不同的并行度中
        // 这时候 相同的订单id就没法对在一起了 就会造成对账成功的订单减少
        // 如果我们先进行一次keyBy 这样同样的key的就会进入一个分区中
        val txKS: KeyedStream[TxEvent, String] = txDS.keyBy(_.txId)

        // 将订单支付流和第三方交易数据流进行连接匹配，然后判断订单是否支付成功
        // connect操作一般情况下是在keyBy之后进行调用
        val orderTxCS: ConnectedStreams[OrderEvent, TxEvent] =
        orderKS.connect(txKS)

        // 处理连接的两个流
        val resultDS: DataStream[String] = orderTxCS.process(
            new CoProcessFunction[OrderEvent, TxEvent, String] {

                private val orderMap = mutable.Map[String, OrderEvent]()
                private val txMap = mutable.Map[String, TxEvent]()

                override def processElement1(order: OrderEvent,
                                             ctx: CoProcessFunction[OrderEvent, TxEvent, String]#Context,
                                             out: Collector[String]): Unit = {
                    // 假设Order数据来的。
                    val maybeEvent: Option[TxEvent] = txMap.get(order.txId)
                    if (maybeEvent.isEmpty) {
                        // 交易数据不存在的场合，临时将订单数据进行保存
                        orderMap.put(order.txId, order)
                    } else {
                        // 交易数据存在的场合， 订单支付成功
                        out.collect("订单" + order.orderId + "支付成功")
                        // 将临时保存的交易ID清除
                        txMap.remove(order.txId)
                    }
                }

                override def processElement2(tx: TxEvent,
                                             ctx: CoProcessFunction[OrderEvent, TxEvent, String]#Context,
                                             out: Collector[String]): Unit = {
                    // 假设交易数据来了。
                    val maybeEvent: Option[OrderEvent] = orderMap.get(tx.txId)
                    if (maybeEvent.isEmpty) {
                        // 订单数据不存在的场合，临时将交易数据进行保存
                        txMap.put(tx.txId, tx)
                    } else {
                        // 订单数据存在的场合， 订单支付成功
                        out.collect("订单" + maybeEvent.get.orderId + "支付成功")
                        // 将临时保存的交易ID清除
                        orderMap.remove(tx.txId)
                    }
                }
            }
        )
        resultDS.print("order<->tx >>>>>")


        env.execute()

    }

    case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)

    case class TxEvent(txId: String, payChannel: String, eventTime: Long)

}
```



## **6.4** **状态编程和容错机制**

### 6.4.1 概述

流式计算分为无状态和有状态两种情况。无状态的计算观察每个独立事件，并根据最后一个事件输出结果。例如，流处理应用程序从传感器接收水位数据，并在水位超过指定高度时发出警告。有状态的计算则会基于多个事件输出结果。以下是一些例子。例如，计算过去一小时的平均水位，就是有状态的计算。所有用于复杂事件处理的状态机。例如，若在一分钟内收到两个相差20cm以上的水位差读数，则发出警告，这是有状态的计算。流与流之间的所有关联操作，以及流与静态表或动态表之间的关联操作，都是有状态的计算。

 

### 6.4.2 有状态的算子

Flink内置的很多算子，数据源 source，数据存储sink都是有状态的，流中的数据都是buffer records，会保存一定的元素或者元数据。例如: ProcessWindowFunction会缓存输入流的数据，ProcessFunction会保存设置的定时器信息等等。

在Flink中，状态始终与特定算子相关联。总的来说，有两种类型的状态：

#### 算子状态（operator state）

算子状态的作用范围限定为算子任务。这意味着由同一并行任务所处理的所有数据都可以访问到相同的状态，状态对于同一任务而言是共享的。算子状态不能由相同或不同算子的另一个任务访问。

![](https://lormer.cn/20200602192103.png)

Flink为算子状态提供三种基本数据结构：

- 列表状态（List state）
  - 将状态表示为一组数据的列表。

- 联合列表状态（Union list state）
  - 也将状态表示为数据的列表。
  - 它与常规列表状态的区别在于，在发生故障时，或者从保存点（savepoint）启动应用程序时如何恢复。

- 广播状态（Broadcast state）
  - 如果一个算子有多项任务，而它的每项任务状态又都相同，那么这种特殊情况最适合应用广播状态。

```scala
package com.atguigu.bigdata.flink.chapter06

import org.apache.flink.streaming.api.scala._

object Flink27_State_Operator {

    def main(args: Array[String]): Unit = {

        // TODO 算子状态
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val datasDS = env.fromCollection(
            List((1, "a"), (2, "b"), (3, "c"))
        )

        // keyBy操作之后 可以进行有状态的算子的计算
        datasDS.keyBy(_._1)
                //.map()
                //.filterWithState()
                //.flatMapWithState()
                .mapWithState {
                    case (d, buff) => {
                        (d, buff)
                    }
                }

        env.execute()

    }

    case class WaterSensor(id: String, ts: Long, vc: Int)

}
```



#### 键控状态（keyed state）

键控状态是根据输入数据流中定义的键（key）来维护和访问的。

Flink为每个键值维护一个状态实例，并将具有相同键的所有数据，都分区到同一个算子任务中，这个任务会维护和处理这个key对应的状态。

当任务处理一条数据时，它会自动将状态的访问范围限定为当前数据的 key。因此，具有相同 key 的所有数据都会访问相同的状态。

Keyed State 很类似于一个分布式的 key-value map 数据结构，只能用于KeyedStream（keyBy算子处理之后）。

![](https://lormer.cn/20200602192130.png)

Flink 的 Keyed State 支持以下数据类型：

- ValueState[T] 保存单个的值，值的类型为T。
  - get操作: ValueState.value()
  - set操作: ValueState.update(value: T)

- ListState[T]保存一个列表，列表里的元素的数据类型为T。基本操作如下：
  - ListState.add(value: T)
  - ListState.addAll(values: java.util.List[T])
  - ListState.get()返回Iterable[T]
  - ListState.update(values: java.util.List[T])

- MapState[K, V]保存Key-Value对。
  - MapState.get(key: K)
  - MapState.put(key: K, value: V)
  - MapState.contains(key: K)
  - MapState.remove(key: K)

- ReducingState[T]

- AggregatingState[I, O]



> State.clear()是清空操作。

使用步骤

- 通过RuntimeContext注册StateDescriptor。

- StateDescriptor以状态state的名字和存储的数据类型为参数。

- 在open()方法中创建state变量。

> 注意复习之前的RichFunction相关知识。

##### 修改上面的连续5s上涨小练习为有状态

```scala
package com.atguigu.bigdata.flink.chapter06

import java.sql.Timestamp

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPunctuatedWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.util.Collector

object Flink28_State_KeyBy {

    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        // TODO 设置时间语义为EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val socketDS: DataStream[String] =
        //env.readTextFile("input/sensor-data.log")
            env.socketTextStream("localhost", 9999)

        val sensorDS = socketDS.map(
            data => {
                val datas = data.split(",")
                WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
            }
        )

        val timeDS = sensorDS.assignTimestampsAndWatermarks(
            new AssignerWithPunctuatedWatermarks[WaterSensor] {
                override def checkAndGetNextWatermark(lastElement: WaterSensor, extractedTimestamp: Long): Watermark = {
                    new Watermark(extractedTimestamp)
                }

                override def extractTimestamp(element: WaterSensor, previousElementTimestamp: Long): Long = {
                    element.ts * 1000L
                }
            }
        )

        val processDS = timeDS
                .keyBy(_.id)
                .process(new MyKeyedProcessFunction)

        timeDS.print("time>>>>")
        processDS.print("process>>>>")

        env.execute()
    }

    // 自定义数据处理函数
    class MyKeyedProcessFunction extends KeyedProcessFunction[String, WaterSensor, String] {
        // TODO 使用有状态的属性保存值
        // TODO 给状态对象进行初始化,需要使用环境对象
        // 状态对象的初始化必须在环境对象初始化之后才能使用
        // 如果这里没有加lazy val 而是用var 会报错 
        // The runtime context has not been initialized
        // 虽然延迟加载能够解决问题 但是实时性不是很好 因为等到用到的时候再加载会存在延迟时间
        // 下一块代码中给出了另一种解决方法  在open方法中加载 这样就解决了问题
        private lazy val currentHeightState: ValueState[Long] =
        getRuntimeContext.getState(
            new ValueStateDescriptor[Long]("currentHeightState", classOf[Long])
        )
        private lazy val alarmTimerState: ValueState[Long] =
            getRuntimeContext.getState(
                new ValueStateDescriptor[Long]("alarmTimerState", classOf[Long])
            )
        //private var currentHeight = 0L
        //private var alarmTimer = 0L

        override def onTimer(timestamp: Long,
                             ctx: KeyedProcessFunction[String, WaterSensor, String]#OnTimerContext,
                             out: Collector[String]): Unit = {
            out.collect("水位传感器" + ctx.getCurrentKey + "在 " + new Timestamp(timestamp) + "已经连续5s水位上涨。。。")
        }

        // 分区中每来一条数据就会调用processElement方法
        override def processElement(value: WaterSensor,
                                    ctx: KeyedProcessFunction[String, WaterSensor, String]#Context,
                                    out: Collector[String]): Unit = {

            // 判断当前水位值和之前记录的水位值的变化
            // TODO 使用状态属性值 ： state.value
            if (value.vc > currentHeightState.value()) {
                // 当水位值上升的时候，开始计算时间，如果到达5s，
                // 中间水位没有下降，那么定时器应该执行
                if (alarmTimerState.value() == 0) {
                    // TODO 更新状态属性值
                    alarmTimerState.update(value.ts * 1000 + 5000)
                  ctx.timerService().registerEventTimeTimer(alarmTimerState.value())
                }
            } else {
                // 水位下降的场合
                // 删除定时器处理
                ctx.timerService().deleteEventTimeTimer(alarmTimerState.value())
                alarmTimerState.update(0L)
            }

            // 保存当前水位值
            currentHeightState.update(value.vc)
        }
    }

    case class WaterSensor(id: String, ts: Long, vc: Int)

}
```

##### 键控状态使用open方法加载运行时上下文环境

```scala
package com.atguigu.bigdata.flink.chapter06

import java.sql.Timestamp

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPunctuatedWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.util.Collector

object Flink29_State_KeyBy1 {

    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        // TODO 设置时间语义为EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val socketDS: DataStream[String] =
        //env.readTextFile("input/sensor-data.log")
            env.socketTextStream("localhost", 9999)

        val sensorDS = socketDS.map(
            data => {
                val datas = data.split(",")
                WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
            }
        )

        val timeDS = sensorDS.assignTimestampsAndWatermarks(
            new AssignerWithPunctuatedWatermarks[WaterSensor] {
                override def checkAndGetNextWatermark(lastElement: WaterSensor, extractedTimestamp: Long): Watermark = {
                    new Watermark(extractedTimestamp)
                }

                override def extractTimestamp(element: WaterSensor, previousElementTimestamp: Long): Long = {
                    element.ts * 1000L
                }
            }
        )

        val processDS = timeDS
                .keyBy(_.id)
                .process(new MyKeyedProcessFunction)

        timeDS.print("time>>>>")
        processDS.print("process>>>>")

        env.execute()
    }

    // 自定义数据处理函数
    class MyKeyedProcessFunction extends KeyedProcessFunction[String, WaterSensor, String] {
        // TODO 使用有状态的属性保存值
        // TODO  给状态对象进行初始化,需要使用环境对象
        // 状态对象的初始化必须在环境对象初始化之后才能使用
        private var currentHeightState: ValueState[Long] = _
        private var alarmTimerState: ValueState[Long] = _

        override def open(parameters: Configuration): Unit = {
            // TODO 可以在当前的位置对状态数据进行初始化
            currentHeightState = getRuntimeContext.getState(
                new ValueStateDescriptor[Long]("currentHeightState", classOf[Long])
            )
            alarmTimerState = getRuntimeContext.getState(
                new ValueStateDescriptor[Long]("alarmTimerState", classOf[Long])
            )
        }

        override def onTimer(timestamp: Long,
                             ctx: KeyedProcessFunction[String, WaterSensor, String]#OnTimerContext,
                             out: Collector[String]): Unit = {
            out.collect("水位传感器" + ctx.getCurrentKey + "在 " + new Timestamp(timestamp) + "已经连续5s水位上涨。。。")
        }

        // 分区中每来一条数据就会调用processElement方法
        override def processElement(value: WaterSensor,
                                    ctx: KeyedProcessFunction[String, WaterSensor, String]#Context,
                                    out: Collector[String]): Unit = {

            // 判断当前水位值和之前记录的水位值的变化
            // TODO 使用状态属性值 ： state.value
            if (value.vc > currentHeightState.value()) {
                // 当水位值上升的时候，开始计算时间，如果到达5s，
                // 中间水位没有下降，那么定时器应该执行
                if (alarmTimerState.value() == 0) {
                    // TODO 更新状态属性值
                    alarmTimerState.update(value.ts * 1000 + 5000)
				  ctx.timerService().registerEventTimeTimer(alarmTimerState.value())
                }
            } else {
                // 水位下降的场合
                // 删除定时器处理
                ctx.timerService().deleteEventTimeTimer(alarmTimerState.value())
                alarmTimerState.update(0L)
            }

            // 保存当前水位值
            currentHeightState.update(value.vc)
        }
    }

    case class WaterSensor(id: String, ts: Long, vc: Int)

}

```

##### ListState/MapState测试演示

```scala
package com.atguigu.bigdata.flink.chapter06

import org.apache.flink.api.common.state._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPunctuatedWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.util.Collector

object Flink30_State_KeyBy2 {

    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        // TODO 设置时间语义为EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val socketDS: DataStream[String] =
        //env.readTextFile("input/sensor-data.log")
            env.socketTextStream("localhost", 9999)

        val sensorDS = socketDS.map(
            data => {
                val datas = data.split(",")
                WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
            }
        )

        val timeDS = sensorDS.assignTimestampsAndWatermarks(
            new AssignerWithPunctuatedWatermarks[WaterSensor] {
                override def checkAndGetNextWatermark(lastElement: WaterSensor, extractedTimestamp: Long): Watermark = {
                    new Watermark(extractedTimestamp)
                }

                override def extractTimestamp(element: WaterSensor, previousElementTimestamp: Long): Long = {
                    element.ts * 1000L
                }
            }
        )

        val processDS = timeDS
                .keyBy(_.id)
                .process(new MyKeyedProcessFunction)

        timeDS.print("time>>>>")
        processDS.print("process>>>>")

        env.execute()
    }

    // 自定义数据处理函数
    class MyKeyedProcessFunction extends KeyedProcessFunction[String, WaterSensor, String] {

        // TODO 状态属性必须在KeyBy才能使用

        // TODO ValueState
        private var testValueState: ValueState[Long] = _
        // TODO ListState
        private var testListState: ListState[Long] = _
        // TODO MapState
        private var testMapState: MapState[String, String] = _

        override def open(parameters: Configuration): Unit = {

            testValueState = getRuntimeContext.getState(
                new ValueStateDescriptor[Long]("testValueState", classOf[Long])
            )

            testListState = getRuntimeContext.getListState(
                new ListStateDescriptor[Long]("testListState", classOf[Long])
            )

            testMapState = getRuntimeContext.getMapState(
                new MapStateDescriptor[String, String]("testMapState", classOf[String], classOf[String])
            )
        }

        // 分区中每来一条数据就会调用processElement方法
        override def processElement(value: WaterSensor,
                                    ctx: KeyedProcessFunction[String, WaterSensor, String]#Context,
                                    out: Collector[String]): Unit = {

            // TODO ValueState
            //testValueState.value()
            //testValueState.update()
            testValueState.clear()

            // TODO ListState
            //testListState.addAll()
            //testListState.add()
            //testListState.update()
            //testListState.get()
            testListState.clear()

            // TODO MapState
            // testMapState.put()
            // testMapState.putAll()
            // testMapState.remove()
            // testMapState.get()
            testMapState.clear

        }
    }

    case class WaterSensor(id: String, ts: Long, vc: Int)

}
```



##### 课件小练习：如果连续两次水位差超过40cm，发生预警信息。

```scala
// ---- 课件  -----
class MyWaterLevelAlert extends KeyedProcessFunction[Tuple,(String, Long, Int), String]{
    
    // 需要保存当前时间水位
    private lazy val watermark: ValueState[Int] = getRuntimeContext.getState(new ValueStateDescriptor[Int]("watermark", Types.of[Int]))
    // 需要记录定时器时间
    private lazy val alarmTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("alarmtimer", Types.of[Long]))
    
    //TODO 每一个元素被加入时，执行此方法
    override def processElement(value: (String, Long, Int), ctx: KeyedProcessFunction[Tuple, (String, Long, Int), String]#Context, out: Collector[String]): Unit = {

        val preWatermark = watermark.value()
        val preAlarmTimer = alarmTimer.value()
        val currWatermark = value._3
        watermark.update(value._3)

        if ( preWatermark == 0 || currWatermark < preWatermark ) {
            ctx.timerService().deleteEventTimeTimer(preAlarmTimer)
            alarmTimer.clear()
        } else if ( currWatermark > preWatermark && preAlarmTimer == 0 ) {
            val newTs = ctx.timerService().currentProcessingTime() + 5000
            alarmTimer.update(newTs)
            ctx.timerService().registerEventTimeTimer(newTs)
        }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, (String, Long, Int), String]#OnTimerContext, out: Collector[String]): Unit= {
        out.collect(s"水位传感器${ctx.getCurrentKey},水位连续上升超5s, 现在水位为${watermark.value()}")
        alarmTimer.clear()
    }
}
```

#### 状态后端（state backend）

每传入一条数据，有状态的算子任务都会读取和更新状态。

由于有效的状态访问对于处理数据的低延迟至关重要，因此每个并行任务都会在本地维护其状态，以确保快速的状态访问。

状态的存储、访问以及维护，由一个可插入的组件决定，这个组件就叫做状态后端（state backend）

---

状态后端主要负责两件事：

- 本地的状态管理

- 将检查点（checkpoint）状态写入远程存储

> 知识回顾迁移 - spark
>
> checkpoint相当于把计算结果保存在了分布式存储中 相当于改变了数据源 会切断血缘关系
>
> 如果从checkpoint恢复 相当于从新文件中读取了
>
> cache因为保存在了内存中了 内存中的数据可能因为动态分配而丢失 所以不会切断血缘

---

**状态后端分类：**

**MemoryStateBackend**

概述

> 内存级的状态后端，会将键控状态作为内存中的对象进行管理，将它们存储在TaskManager的JVM堆上；
>
> 而将checkpoint存储在JobManager的内存中。

何时使用MemoryStateBackend？

> 建议使用MemoryStateBackend进行本地开发或调试，因为它的状态有限
>
> MemoryStateBackend最适合具有小状态大小的用例和有状态流处理应用程序，例如仅包含一次记录功能（Map，FlatMap或Filter）的作业或使用Kafkaconsumer。

**FsStateBackend**

概述

> 将checkpoint存到远程的持久化文件系统（FileSystem）上。而对于本地状态，跟MemoryStateBackend一样，也会存在TaskManager的JVM堆上。

何时使用FsStateBackend？

> FsStateBackend最适合处理大状态，长窗口或大键/值状态的Flink有状态流处理作业
>
> FsStateBackend最适合每个高可用性设置

**RocksDBStateBackend**

将所有状态序列化后，存入本地的RocksDB中存储。

注意：RocksDB的支持并不直接包含在flink中，需要引入依赖：

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-statebackend-rocksdb_2.11</artifactId>
    <version>1.10.0</version>
</dependency>
```

何时使用RocksDBStateBackend？

> RocksDBStateBackend最适合处理大状态，长窗口或大键/值状态的Flink有状态流处理作业
>
> RocksDBStateBackend最适合每个高可用性设置
>
> RocksDBStateBackend是目前唯一可用于支持**有状态流处理应用程序的增量检查点**的状态后端

选择一个状态后端(state backend)：

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-statebackend-rocksdb_2.11</artifactId>
    <version>1.10.0</version>
</dependency>
```

设置状态后端为RocksDBStateBackend：

```scala
// 设置检查点路径
val checkpointPath : String = "XXX路径"
// 选择Backend类型 
// 注意这里类型最好写为StateBackend 如果写具体类会当成抽象类的实现类 会报过时
val stateBackend:StateBackend = new RocksDBStateBackend(checkpointPath)
// 设置Backend
env.setStateBackend(stateBackend)
// 启用检查点 这里的1000是指多长时间生成一个checkpoint数据 后面是设定检查点模式
env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE)
```

![](https://lormer.cn/20200602225432.png)

![](https://lormer.cn/20200602225555.png)

![](https://lormer.cn/20200602225639.png)

StateBackEnd是一个接口 找其实现类 如下

![](https://lormer.cn/20200602225342.png)

注意这里AbstractStateBackend是一个抽象类 StateBackend是一个接口

如果在`env.setStateBackend(stateBackend)`时候 里面传的是一个抽象类的实现类 

(也就是如果在上一步`val stateBackend:StateBackend = new RocksDBStateBackend(checkpointPath)`中写成了`val stateBackend:RocksDBStateBackend = new RocksDBStateBackend(checkpointPath)` )

会报过时提示

![](https://lormer.cn/20200602225129.png)

![](https://lormer.cn/20200602230720.png)

演示代码

```scala
package com.atguigu.bigdata.flink.chapter06

import org.apache.flink.api.common.state._
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.{AssignerWithPunctuatedWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.util.Collector

object Flink31_State_RocksDB {

    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 设置状态后端，就是告诉Flink将状态数据保存在什么地方
        val checkpointPath : String = "hdfs://linux1:9000/flink/cp"
        val stateBackend:StateBackend = new RocksDBStateBackend(checkpointPath)
        env.setStateBackend(stateBackend)

        // 启动检查点
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE)

        env.execute()
    }
}
```

### 6.4.3 状态一致性

当在分布式系统中引入状态时，自然也引入了一致性问题。一致性实际上是"正确性级别"的另一种说法，也就是说在成功处理故障并恢复之后得到的结果，与没有发生任何故障时得到的结果相比，恢复的数据到底有多正确？

> 举例来说，假设要对最近一小时登录的用户计数。
>
> 在系统经历故障之后，计数结果是多少？
>
> 如果有偏差，是有漏掉的计数还是重复计数？

所以根据实际情况就可以对一致性设定不同的级别

#### 一致性级别

在流处理中，一致性可以分为3个级别：

- at-most-once
  - 最多处理一次
- 这其实是没有正确性保障的委婉说法——故障发生之后，计数结果可能丢失。同样的还有udp。
- at-least-once
  - 至少处理一次 一旦出现故障 可能再处理一次
- 这表示计数结果可能大于正确值，但绝不会小于正确值。也就是说，计数程序在发生故障后可能多算，但是绝不会少算。
- exactly-once
  - 这指的是系统保证在发生故障后得到的计数结果与正确值一致。

曾经，at-least-once非常流行。第一代流处理器(如Storm和Samza)刚问世时只保证at-least-once，原因有二。

- 保证 exactly-once 的系统实现起来更复杂。
- 这在基础架构层(决定什么代表正确，以及 exactly-once 的范围是什么)和实现层都很有挑战性。
- 流处理系统的早期用户愿意接受框架的局限性，并在应用层想办法弥补(例如使应用程序具有幂等性，或者用批量计算层再做一遍计算)。

最先保证exactly-once的系统(Storm Trident和Spark Streaming)在性能和表现力这两个方面付出了很大的代价。

- 为了保证exactly-once，这些系统无法单独地对每条记录运用应用逻辑，而是同时处理多条(一批)记录，保证对每一批的处理要么全部成功，要么全部失败。这就导致在得到结果前，必须等待一批记录处理结束。
- 因此，用户经常不得不使用两个流处理框架(一个用来保证exactly-once，另一个用来对每个元素做低延迟处理)，结果使基础设施更加复杂。
- 曾经，用户不得不在保证exactly-once与获得低延迟和效率之间权衡利弊。Flink避免了这种权衡。

Flink的一个重大价值在于，它既保证了exactly-once，也具有低延迟和高吞吐的处理能力。

> 从根本上说，Flink通过使自身满足所有需求来避免权衡，它是业界的一次意义重大的技术飞跃。
>
> 尽管这在外行看来很神奇，但是一旦了解，就会恍然大悟。

#### 端到端（end-to-end）状态一致性

目前我们看到的一致性保证都是由流处理器实现的，也就是说都是在 Flink 流处理器内部保证的；而在真实应用中，流处理应用除了流处理器以外还包含了数据源（例如 Kafka）和输出到持久化系统。

端到端的一致性保证，意味着结果的正确性贯穿了整个流处理应用的始终；每一个组件都保证了它自己的一致性，整个端到端的一致性级别取决于所有组件中一致性最弱的组件。

具体可以划分如下：

- source端 —— 需要外部源可重设数据的读取位置(文件 kafka等 socket就不能保证 ...)

- flink内部 —— 依赖checkpoint

- sink端 —— 需要保证从故障恢复时，数据不会重复写入外部系统

而对于sink端，又有两种具体的实现方式：幂等（Idempotent）写入和事务性（Transactional）写入。

**幂等写入**

所谓幂等操作，是说一个操作，可以重复执行很多次，但只导致一次结果更改，也就是说，后面再重复执行就不起作用了。(例如往hashmap中放同一个对象)

![](https://lormer.cn/20200602193256.png)

**事务写入**

需要构建事务来写入外部系统，构建的事务对应着 checkpoint，等到 checkpoint 真正完成的时候，才把所有对应的结果写入 sink 系统中。

对于事务性写入，具体又有两种实现方式：

- 预写日志（WAL）

- 两阶段提交（2PC）------ 分布式事务



![](https://lormer.cn/20200602193405.png)

DataStream API 提供了`GenericWriteAheadSink`模板类和`TwoPhaseCommitSinkFunction` 接口，可以方便地实现这两种方式的事务性写入。

![](https://lormer.cn/20200602233439.png)

![](https://lormer.cn/20200602233309.png)

![](https://lormer.cn/20200603004238.png)

不同 Source 和 Sink 的一致性保证可以用下表说明：

|   sink\source   |   不可重置   |                    可重置                    |
| :-------------: | :----------: | :------------------------------------------: |
|    任意(Any)    | At-most-once |                At-least-once                 |
|      幂等       | At-most-once | Exactly-once<br>(故障恢复时会出现暂时不一致) |
|  预写日志(WAL)  | At-most-once |                At-least-once                 |
| 两阶段提交(2PC) | At-most-once |                 Exactly-once                 |

> 不可重置  就是不能重新发数据
>
> 预写日志在同步的时候可能出现问题 可能同步失败 

### 6.4.4 检查点

> 跟Spark检查点不同 Flink中的检查点类似于之前的Watermark 是一条数据

Flink具体如何保证exactly-once呢? 

它使用一种被称为"检查点"（checkpoint）的特性，**在出现故障时将系统重置回正确状态**。

> 下面通过简单的类比来解释检查点的作用。
>
> 假设你和两位朋友正在数项链上有多少颗珠子，如下图所示。你捏住珠子，边数边拨，每拨过一颗珠子就给总数加一。你的朋友也这样数他们手中的珠子。当你分神忘记数到哪里时，怎么办呢? 如果项链上有很多珠子，你显然不想从头再数一遍，尤其是当三人的速度不一样却又试图合作的时候，更是如此(比如想记录前一分钟三人一共数了多少颗珠子，回想一下一分钟滚动窗口)。
>
> 于是，你想了一个更好的办法: 在项链上每隔一段就松松地系上一根有色皮筋，将珠子分隔开; 当珠子被拨动的时候，皮筋也可以被拨动; 然后，你安排一个助手，让他在你和朋友拨到皮筋时记录总数。用这种方法，当有人数错时，就不必从头开始数。相反，你向其他人发出错误警示，然后你们都从上一根皮筋处开始重数，助手则会告诉每个人重数时的起始数值，例如在粉色皮筋处的数值是多少。
>
> Flink检查点的作用就类似于皮筋标记。数珠子这个类比的关键点是: 对于指定的皮筋而言，珠子的相对位置是确定的; 这让皮筋成为重新计数的参考点。总状态(珠子的总数)在每颗珠子被拨动之后更新一次，助手则会保存与每根皮筋对应的检查点状态，如当遇到粉色皮筋时一共数了多少珠子，当遇到橙色皮筋时又是多少。当问题出现时，这种方法使得重新计数变得简单。

#### Flink的检查点算法

> 分布式状态存储
>
> Chandy-Lambort算法

Flink检查点的核心作用是**确保状态正确**，**即使遇到程序中断，也要正确**。

记住这一基本点之后，Flink为用户提供了用来定义状态的工具。

```scala
val dataDS: DataStream[String] = env.readTextFile("input/data.txt")

val mapDS: DataStream[(String, String, String)] = dataDS.map(data => {
    val datas = data.split(",")
    (datas(0), datas(1), datas(2))
})
val keyDS: KeyedStream[(String, String, String), Tuple] = mapDS.keyBy(0)

keyDS.mapWithState{
    case ( t, buffer ) => {
        (t, buffer)
    }
}
```

我们用一个图形来看检查点是如何运行的：

![](https://lormer.cn/20200602193906.png)

> 上图中的检查点机制也可以认为是两步提交操作 各个节点向JM提交数据 (预提交)
>
> 但是此时并不生效 如果中间有任何出现问题 该检查点数据回滚作废
>
> 如果整个流程都没有问题 则检查点数据可用 (正式提交)

Flink检查点算法的正式名称是异步分界线快照(asynchronous barrier snapshotting)。

该算法大致基于Chandy-Lamport分布式快照算法。

检查点是Flink最有价值的创新之一，因为它使Flink可以保证exactly-once，并且不需要牺牲性能。

#### Flink+Kafka如何实现端到端的Exactly-Once语义

我们知道，端到端的状态一致性的实现，需要每一个组件都实现，对于Flink + Kafka的数据管道系统（Kafka进、Kafka出）而言，各组件怎样保证exactly-once语义呢？

> 注意 精准一次性不是指这个数据只过处理一次 可能中间处理过多次(比如发生了多次崩溃 数据就会重复处理) 但是最后结果相当于只处理了一次 

- 内部 —— 利用checkpoint机制，把状态存盘，发生故障的时候可以恢复，保证内部的状态一致性

- source —— kafka consumer作为source，可以将偏移量保存下来，如果后续任务出现了故障，恢复的时候可以由连接器重置偏移量，重新消费数据，保证一致性

- sink —— kafka producer作为sink，采用两阶段提交 sink，需要实现一个 TwoPhaseCommitSinkFunction

内部的checkpoint机制我们已经有了了解，那source和sink具体又是怎样运行的呢？接下来我们逐步做一个分析。

我们知道Flink由JobManager协调各个TaskManager进行checkpoint存储，checkpoint保存在 StateBackend中，默认StateBackend是内存级的，也可以改为文件级的进行持久化保存。

![](https://lormer.cn/20200602193930.png)

当 checkpoint 启动时，JobManager 会将检查点分界线（barrier）注入数据流；barrier会在算子间传递下去。

![](https://lormer.cn/20200602193949.png)

每个算子会对当前的状态做个快照，保存到状态后端。对于source任务而言，就会把当前的offset作为状态保存起来。下次从checkpoint恢复时，source任务可以重新提交偏移量，从上次保存的位置开始重新消费数据。

![](https://lormer.cn/20200602194017.png)

每个内部的 transform 任务遇到 barrier 时，都会把状态存到 checkpoint 里。

sink 任务首先把数据写入外部 kafka，这些数据都属于预提交的事务（还不能被消费）；当遇到 barrier 时，把状态保存到状态后端，并开启新的预提交事务。

![](https://lormer.cn/20200602194040.png)

当所有算子任务的快照完成，也就是这次的 checkpoint 完成时，JobManager 会向所有任务发通知，确认这次 checkpoint 完成。

当sink任务收到确认通知，就会正式提交之前的事务，kafka 中未确认的数据就改为“已确认”，数据就真正可以被消费了。

![](https://lormer.cn/20200602194116.png)

所以我们看到，执行过程实际上是一个两段式提交，每个算子执行完成，会进行“预提交”，直到执行完sink操作，会发起“确认提交”，如果执行失败，预提交会放弃掉。

具体的两阶段提交步骤总结如下：

1. 第一条数据来了之后，开启一个 kafka 的事务（transaction），正常写入 kafka 分区日志但标记为未提交，这就是“预提交”
2. jobmanager 触发 checkpoint 操作，barrier 从 source 开始向下传递，遇到 barrier 的算子将状态存入状态后端，并通知 jobmanager
3. sink 连接器收到 barrier，保存当前状态，存入 checkpoint，通知 jobmanager，并开启下一阶段的事务，用于提交下个检查点的数据
4. jobmanager 收到所有任务的通知，发出确认信息，表示 checkpoint 完成
5. sink 任务收到 jobmanager 的确认信息，正式提交这段时间的数据
6. 外部kafka关闭事务，提交的数据可以正常消费了。

#### 引申知识 两阶段提交和三阶段提交

##### 一.分布式中的CAP怎么理解

###### **1.CAP**

- C（Consistency）一致性  
  - 每一次读取都会让你得到最新的写入结果
- A （Availability）可用性   
  - 每个节点(如果没有失败)，总能执行查询(读取和写入)操作
- P （Partition Tolerance）分区容忍性  
  - 即使节点之间的连接关闭，其他两个属性也会得到保证

CAP理论认为，任何联网的共享数据系统智能实现三个属性中的两个，但是可以通过明确处理分区，优化一致性和可用性，从而实现三者之间的某种权衡

###### **2.zookeeper提供的一致性服务**

很多文章和博客里提到，zookeeper是一种提供强一致性的服务，在分区容错性和可用性上做了一定折中，这和CAP理论是吻合的。但实际上zookeeper提供的只是单调一致性。

原因：

1. 假设有2n+1个server，在同步流程中，leader向follower同步数据，当同步完成的follower数量大于 n+1时同步流程结束，系统可接受client的连接请求。如果client连接的并非同步完成的follower，那么得到的并非最新数据，但可以保证单调性。
2. follower接收写请求后，转发给leader处理；leader完成两阶段提交的机制。向所有server发起提案，当提案获得超过半数（n+1）的server认同后，将对整个集群进行同步，超过半数（n+1）的server同步完成后，该写请求完成。如果client连接的并非同步完成follower，那么得到的并非最新数据，但可以保证单调性。

用分布式系统的CAP原则来分析Zookeeper：

1. C: Zookeeper保证了最终一致性,在十几秒可以Sync到各个节点.
2. A: Zookeeper保证了可用性,数据总是可用的,没有锁.并且有一大半的节点所拥有的数据是最新的,实时的. 如果想保证取得是数据一定是最新的,需要手工调用Sync()
3. P: 有两点需要分析
   - 节点多了会导致写数据延时非常大,因为需要多个节点同步.
   - 节点多了Leader选举非常耗时, 就会放大网络的问题. 可以通过引入 observer节点缓解这个问题.

##### 二.分布式一致性

###### **1.引言**

在分布式系统中，为了保证数据的高可用,通常我们会将数据保留多个副本(replica), 这些副本会放置在不同的物理机器上。为了对用户提供正确的curd等语意，我们需要保证这些放置在不同物理机器上的副本是一致的

为了解决这种分布式一致性问题，提出了很多典型的协议和算法，比较著名的是二阶段提交协议，三阶段提交协议和paxos算法。

 

###### **2.分布式事务**

在分布式系统中，各个节点之间在物理上相互独立，通过网络进行沟通和协调。

由于存在事务机制，可以保证每个独立节点上的数据操作可以满足ACID。

但是，相互独立的节点之间无法准确地知道其他节点的事务执行情况。

所以从理论上来讲，两台机器无法达到一致的状态。

如果想让分布式部署的多台机器中的数据保持一致性，那么就要保证在所有节点数据的写操作，要么全部都执行，要么全部都不执行。

但是，一台机器在执行本地事务的时候无法知道其他机器中的本地事务的执行结果，所以它也就不知道本次事务到底应该commit还是rollback。

所以，常规的解决办法就是引入一个"协调者"的组件来统一调度所有分布式节点的执行。

##### 三.2PC（Two-phaseCommit）

###### **1.二阶段提交**

二阶段提交的算法思路可以概括为: 参与者将操作成败通知协调者，再由协调者根据所有参与者的反馈情报决定各参与者是否要提交操作还是中止操作。

二阶段是指:  第一阶段 - 请求阶段(表决阶段)   第二阶段 - 提交阶段(执行阶段)

- 请求阶段(表决)：

  事务协调者通知每个参与者准备提交或取消事务，然后进入表决过程，参与者要么在本地执行事务，写本地的redo和undo日志，但不提交，到达一种"万事俱备，只欠东风"的状态。

  请求阶段，参与者将告知协调者自己的决策: 同意(事务参与者本地作业执行成功)或取消（本地作业执行故障）

- 提交阶段(执行):

  在该阶段，写调整将基于第一个阶段的投票结果进行决策: 提交或取消

  当且仅当所有的参与者同意提交事务，协调者才通知所有的参与者提交事务，否则协调者将通知所有的参与者取消事务

  参与者在接收到协调者发来的消息后将执行响应的操作

![](https://lormer.cn/20200603005319.png)

###### **2.两阶段提交的缺点**

1. 同步阻塞问题。

   执行过程中，所有参与节点都是事务阻塞型的。

   当参与者占有公共资源时，其他第三方节点访问公共资源不得不处于阻塞状态。

2. 单点故障。

   由于协调者的重要性，一旦协调者发生故障。参与者会一直阻塞下去。

   尤其在第二阶段，协调者发生故障，那么所有的参与者还都处于锁定事务资源的状态中，而无法继续完成事务操作。（如果是协调者挂掉，可以重新选举一个协调者，但是无法解决因为协调者宕机导致的参与者处于阻塞状态的问题）

3. 数据不一致。

   在二阶段提交的阶段二中，当协调者向参与者发送commit请求之后，发生了局部网络异常或者在发送commit请求过程中协调者发生了故障，这回导致只有一部分参与者接受到了commit请求。

   而在这部分参与者接到commit请求之后就会执行commit操作。

   但是其他部分未接到commit请求的机器则无法执行事务提交。

   于是整个分布式系统便出现了数据不一致性的现象。

 

###### **3.两阶段提交无法解决的问题**

当协调者出错，同时参与者也出错时，两阶段无法保证事务执行的完整性。

考虑协调者在发出commit消息之后宕机，而唯一接收到这条消息的参与者同时也宕机了。

那么即使协调者通过选举协议产生了新的协调者，这条事务的状态也是不确定的，没人知道事务是否被已经提交。

##### 四.3PC（Three-phaseCommit）

###### **1.三阶段提交**

三阶段提交协议在协调者和参与者中都引入超时机制，并且把两阶段提交协议的第一个阶段分成了两步: 询问，然后再锁资源，最后真正提交。

![](https://lormer.cn/20200603005539.png)

###### **2.三阶段的执行**

(1) **canCommit阶段**

3PC的canCommit阶段其实和2PC的准备阶段很像。协调者向参与者发送commit请求，参与者如果可以提交就返回yes响应，否则返回no响应

(2) **preCommit阶段**

协调者根据参与者canCommit阶段的响应来决定是否可以继续事务的preCommit操作。根据响应情况，有下面两种可能:

- 协调者从所有参与者得到的反馈都是yes:

  那么进行事务的预执行，协调者向所有参与者发送preCommit请求，并进入prepared阶段。参与泽和接收到preCommit请求后会执行事务操作，并将undo和redo信息记录到事务日志中。如果一个参与者成功地执行了事务操作，则返回ACK响应，同时开始等待最终指令

- 协调者从所有参与者得到的反馈有一个是No或是等待超时之后协调者都没收到响应:

  那么就要中断事务，协调者向所有的参与者发送abort请求。参与者在收到来自协调者的abort请求，或超时后仍未收到协调者请求，执行事务中断。

(3) **doCommit阶段**

协调者根据参与者preCommit阶段的响应来决定是否可以继续事务的doCommit操作。根据响应情况，有下面两种可能:

1. 协调者从参与者得到了ACK的反馈:

   协调者接收到参与者发送的ACK响应，那么它将从预提交状态进入到提交状态，并向所有参与者发送doCommit请求。参与者接收到doCommit请求后，执行正式的事务提交，并在完成事务提交之后释放所有事务资源，并向协调者发送haveCommitted的ACK响应。那么协调者收到这个ACK响应之后，完成任务。

2. 协调者从参与者没有得到ACK的反馈, 也可能是接收者发送的不是ACK响应，也可能是响应超时:

   执行事务中断。

##### 五.2PC vs 3PC

###### **1.2PC和3PC**

对于协调者(Coordinator)和参与者(Cohort)都设置了超时机制（在2PC中，只有协调者拥有超时机制，即如果在一定时间内没有收到cohort的消息则默认失败）。

在2PC的准备阶段和提交阶段之间，插入预提交阶段，使3PC拥有CanCommit、PreCommit、DoCommit三个阶段。

PreCommit是一个缓冲，保证了在最后提交阶段之前各参与节点的状态是一致的。

>三阶段提交是“非阻塞”协议。
>
>三阶段提交在两阶段提交的第一阶段与第二阶段之间插入了一个准备阶段，
>
>使得原先在两阶段提交中，参与者在投票之后，由于协调者发生崩溃或错误，而导致参与者处于无法知晓是否提交或者中止的“不确定状态”所产生的可能相当长的延时的问题得以解决。 
>
>举例来说，假设有一个决策小组由一个主持人负责与多位组员以电话联络方式协调是否通过一个提案，以两阶段提交来说，主持人收到一个提案请求，打电话跟每个组员询问是否通过并统计回复，然后将最后决定打电话通知各组员。
>
>要是主持人在跟第一位组员通完电话后失忆，而第一位组员在得知结果并执行后老人痴呆，那么即使重新选出主持人，也没人知道最后的提案决定是什么，也许是通过，也许是驳回，不管大家选择哪一种决定，都有可能与第一位组员已执行过的真实决定不一致，老板就会不开心认为决策小组沟通有问题而解雇。
>
>三阶段提交即是引入了另一个步骤，主持人打电话跟组员通知请准备通过提案，以避免没人知道真实决定而造成决定不一致的失业危机。
>
>为什么能够解决二阶段提交的问题呢？
>
>回到刚刚提到的状况，在主持人通知完第一位组员请准备通过后两人意外失忆，即使没人知道全体在第一阶段的决定为何，全体决策组员仍可以重新协调过程或直接否决，不会有不一致决定而失业。
>
>那么当主持人通知完全体组员请准备通过并得到大家的再次确定后进入第三阶段，
>
>当主持人通知第一位组员请通过提案后两人意外失忆，这时候其他组员再重新选出主持人后，
>
>仍可以知道目前至少是处于准备通过提案阶段，表示第一阶段大家都已经决定要通过了，此时便可以直接通过。

###### **2.三阶段提交协议的缺点**

如果进入PreCommit后，Coordinator发出的是abort请求，假设只有一个Cohort收到并进行了abort操作，
而其他对于系统状态未知的Cohort会根据3PC选择继续Commit，此时系统状态发生不一致性。

###### **3.替代**

目前还有一种重要的算法就是Paxos算法，Zookeeper采用的就是Paxos算法的改进。