---
title: Flink案例实操
date: 2020-6-3 01:36:32
excerpt: 案例实操
toc_min_depth: 3
tags: 
  - Flink
  - 大数据框架
categories:
  - [Flink]
---

## **6.5** **案例实操**

### 6.5.1 电商数据分析

电商平台中的用户行为频繁且较复杂，系统上线运行一段时间后，可以收集到大量的用户行为数据，进而利用大数据技术进行深入挖掘和分析，得到感兴趣的商业指标并增强对风险的控制。

电商用户行为数据多样，整体可以分为**用户行为习惯数据**和**业务行为数据**两大类。

用户的行为习惯数据包括了用户的登录方式、上线的时间点及时长、点击和浏览页面、页面停留时间以及页面跳转等等，我们可以从中进行流量统计和热门商品的统计，也可以深入挖掘用户的特征；这些数据往往可以从web服务器日志中直接读取到。

而业务行为数据就是用户在电商平台中针对每个业务（通常是某个具体商品）所作的操作，我们一般会在业务系统中相应的位置埋点，然后收集日志进行分析。

#### 实时热门商品统计

接下来我们将实现一个“实时热门商品”的需求，可以将“实时热门商品”翻译成程序员更好理解的需求：

每隔5分钟输出最近一小时内点击量最多的前N个商品 滑动窗口 TopN

( 滑动窗口 步长为5min 窗口为1h 计数wordcount 排序)

##### 1. 数据准备

这里依然采用UserBehavior.csv作为数据源，通过采集数据统计商品点击信息。

由于要采用窗口计算，所以需要设置时间语义

```scala
env.setStreamTimeCharacteristic(
    TimeCharacteristic.EventTime
)
```

##### 2. 读取日志数据转换样例类

```scala
val userBehaviorDS = env
    .readTextFile("input/UserBehavior.csv")
    .map(
        data => {
            val datas = data.split(",")
            UserBehavior(
                datas(0).toLong,
                datas(1).toLong,
                datas(2).toInt,
                datas(3),
                datas(4).toLong
            )
        }
    )
```

##### 3. 从转换后的数据中抽取时间戳和Watermark

```scala
val timeDS: DataStream[UserBehavior] =
    userBehaviorDS
       .assignAscendingTimestamps(_.timestamp * 1000L)

```


##### 4. 对数据进行过滤，保留商品点击数据

```scala
val pvDS: DataStream[UserBehavior] =
    timeDS.filter(_.behavior == "pv")
```


##### 5. 将数据根据商品ID进行分组

```scala
val itemKS: KeyedStream[UserBehavior, Long] =
    pvDS.keyBy(_.itemId)
```


##### 6. 设定数据窗口范围

```scala
// 第一个参数是窗口大小 第二个是滑动步长
val itemWS = itemKS.timeWindow(
    Time.hours(1),
    Time.minutes(5)
)
```


##### 7. 对窗口数据进行聚合并做转换

```scala
itemWS.aggregate(
    new AggregateFunction[UserBehavior, Long, Long] {
        override def createAccumulator(): Long = 0L

        override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

        override def getResult(accumulator: Long): Long = accumulator

        override def merge(a: Long, b: Long): Long = a + b
    },
    new WindowFunction[Long, HotItemClick, Long, TimeWindow] {
        override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[HotItemClick]): Unit = {
            out.collect( HotItemClick( key, input.iterator.next(), window.getEnd ) )
        }
    }
)
```


##### 8. 对窗口聚合转换后的数据进行分组

```scala
val aggKS = aggDS.keyBy(_.windowEndTime)
```


##### 9. 对分组后的数据进行排序和输出

```scala
aggKS.process(new KeyedProcessFunction[Long, HotItemClick, String]{
        // 数据集合
        private var itemList:ListState[HotItemClick] = _
        // 定时器
        private var alarmTimer:ValueState[Long] = _
    
    
        override def open(parameters: Configuration): Unit = {
            itemList = getRuntimeContext.getListState(
                new ListStateDescriptor[HotItemClick]("itemList", classOf[HotItemClick])
            )
            alarmTimer = getRuntimeContext.getState(
                new ValueStateDescriptor[Long]( "alarmTimer", classOf[Long] )
            )
        }
    
        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, HotItemClick, String]#OnTimerContext, out: Collector[String]): Unit = {
            val datas: lang.Iterable[HotItemClick] = itemList.get()
            val dataIter: util.Iterator[HotItemClick] = datas.iterator()
            val list = new ListBuffer[HotItemClick]
    
            while ( dataIter.hasNext ) {
                list.append(dataIter.next())
            }
    
            itemList.clear()
            alarmTimer.clear()
    
            val result: ListBuffer[HotItemClick] = list.sortBy(_.clickCount)(Ordering.Long.reverse).take(3)
    
            // 将结果输出到控制台
            val builder = new StringBuilder
            builder.append("当前时间：" + new Timestamp(timestamp) + "\n")
            for ( data <- result ) {
                builder.append("商品：" + data.itemId + ", 点击数量：" + data.clickCount + "\n")
            }
            builder.append("================")
    
            out.collect(builder.toString())
    
            Thread.sleep(1000)
        }
    
        override def processElement(value: HotItemClick, ctx: KeyedProcessFunction[Long, HotItemClick, String]#Context, out: Collector[String]): Unit = {
            itemList.add(value)
            if ( alarmTimer.value() == 0 ) {
                ctx.timerService().registerEventTimeTimer(value.windowEndTime)
                alarmTimer.update(value.windowEndTime)
            }
    
        }
    })
```

```scala
package com.atguigu.bigdata.flink.chapter06

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import scala.collection.mutable.ListBuffer

object Flink34_Req_HotItemRank_Analyses {

    def main(args: Array[String]): Unit = {

        // TODO 案例实操 - 需求 - 热门商品排行
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // TODO 1. 读取日志数据
        val dataDS: DataStream[String] =
            env.readTextFile("input/UserBehavior.csv")

        // TODO 2. 将数据转换为样例类便于使用
        val userBehaviorDS: DataStream[UserBehavior] = dataDS.map(
            data => {
                val datas = data.split(",")
                UserBehavior(
                    datas(0).toLong,
                    datas(1).toLong,
                    datas(2).toInt,
                    datas(3),
                    datas(4).toLong
                )
            }
        )

        // TODO 3. 过滤用户访问商品的数据
        // 先过滤再计算 可以减少计算的数据量
        val userBehaviorFilterDS: DataStream[UserBehavior] =
            userBehaviorDS.filter(_.behavior == "pv")

        // TODO 4. 抽取时间戳用于进行计算
        val userBehaviorTimeDS =
            userBehaviorFilterDS.assignAscendingTimestamps(_.timestamp * 1000L)

        // 全窗口函数
        //userBehaviorTimeDS.timeWindowAll(Time.hours(1)).process()

        // TODO 5. 统计WordCount => ( itemId, 1 ) =>( itemId, totalClick )
        //         根据商品ID对数据进行分组（分区）
        val userBehaviorKS: KeyedStream[UserBehavior, Long] =
        userBehaviorTimeDS.keyBy(_.itemId)

        // TODO 6. 增加时间窗口范围，统计数据
        val userBehaviorWS: WindowedStream[UserBehavior, Long, TimeWindow] =
            userBehaviorKS.timeWindow(
                Time.hours(1),
                Time.minutes(5)
            )

        // TODO 7. 对窗口的数据进行聚合
        // 注 : 为什么不用ProcessWindowFunction 是因为这个是把整个窗口中的数据拿过来计算 也就是每五分钟就会把所有的最近一个小时的数据全部计算一遍 这样造成多次重复计算 并且下面的process是在keyBy之后做的 这个会造成只能拿到同一个key的数据 而并不能实现我们对所有的key进行排序的需求
        // 而我们下面用的aggregate方法 是来一条数据处理一条 虽然这个方法最后返回的数据没有窗口的概念会丢失窗口信息 但是我们可以在数据上加上一个窗口的时间属性用来标记这个数据属于哪个窗口
//        userBehaviorWS.process(
//            new ProcessWindowFunction[UserBehavior,String, Long, TimeWindow] {
//                override def process(key: Long, context: Context, elements: Iterable[UserBehavior], out: Collector[String]): Unit = {
//
//                }
//            }
//        )
// sum的参数传？
// 排序? 时间?
// sum 和 reduce 无法拿到窗口信息 无法对窗口中数据进行下一步的排序操作
//val value: DataStream[UserBehavior] = userBehaviorWS.sum(1)
//userBehaviorWS.reduce()
// (1001, 20),(1002, 30)
// 使用process可以拿到窗口信息 但是是拿到整个窗口数据之后再做计算 这样会造成短时间计算量过大 且 不能做keyBy 因为keyBy之后 窗口变成了同一个key 的所有数据 而我们要做的是对所有的key进行排序 所以如果做keyBy之后使用process只能拿到一个key的窗口数据
// 如果要用process 只能用当前代码第52行部分直接全窗口操作然后进行process
//userBehaviorWS.process()
// aggregate : 窗口聚合方法
// aggregate聚合数据后缺失时间窗口信息
// aggregate方法可以传递两个参数
//    第一个参数表示为数据累加器函数
//    第二个参数表示为数据窗口处理函数，函数的输入应该为累加器函数的输出
        val hotItemClickDS: DataStream[HotItemClick] =
        userBehaviorWS.aggregate(
            // 聚合的输出结果是下一步窗口函数的输入
            new MyAggregateFunction,
            // 这个函数在下面定义 加上了数据所在窗口的EndTime
            new MyProcessWindowFunction
        )

        // TODO 8. 对聚合的数据进行分组
        val hotItemClickKS: KeyedStream[HotItemClick, Long] =
            hotItemClickDS.keyBy(_.windowEndTime)

        // TODO 9. 对聚合的数据进行排序
        // 看这一步  是对keyBy之后的数据进行处理 
        // 这里因为是用了窗口的时间进行聚合了 所以拿到的是整个窗口的数据
        hotItemClickKS.process(
            new MyHotItemClickProcessFunction
        ).print("topN>>>")

        env.execute()

    }

    // 自定义分组处理函数
    class MyHotItemClickProcessFunction extends KeyedProcessFunction[Long, HotItemClick, String] {
        private var hotItemList: ListState[HotItemClick] = _
        private var triggerTimer: ValueState[Long] = _

        // 使用open方法保存状态
        override def open(parameters: Configuration): Unit = {
            hotItemList = getRuntimeContext.getListState(
                new ListStateDescriptor[HotItemClick]("hotItemList", classOf[HotItemClick])
            )
            triggerTimer = getRuntimeContext.getState(
                new ValueStateDescriptor[Long]("triggerTimer", classOf[Long])
            )
        }

        // 定时器触发后，完成排序功能
        override def onTimer(timestamp: Long, 
                             ctx: KeyedProcessFunction[Long, HotItemClick, String]#OnTimerContext, 
                             out: Collector[String]): Unit = {
            val datas = hotItemList.get().iterator()
            val dataList = new ListBuffer[HotItemClick]()
            import scala.collection.JavaConversions._
            for (data <- datas) {
                dataList.append(data)
            }
            // 清除状态信息
            hotItemList.clear()
            triggerTimer.clear()

            // TODO 对数据进行排序处理
            val top3: ListBuffer[HotItemClick] = dataList.sortBy(_.clickCount)(Ordering.Long.reverse).take(3)

            out.collect(
                s"""
                   | 时间 ：${new java.sql.Timestamp(timestamp)}
                   | -----------------------------------------
                   | ${top3.mkString("\n ")}
                   | -----------------------------------------
                 """.stripMargin)
            Thread.sleep(1000)
        }

        // 每来一条数据，进行处理的函数
        override def processElement(value: HotItemClick, ctx: KeyedProcessFunction[Long, HotItemClick, String]#Context, out: Collector[String]): Unit = {

            // 将每一条数据临时保存起来
            hotItemList.add(value)
            // 当数据全部到达的情况，触发排序功能
            if (triggerTimer.value() == 0) {
                // 判断当前是否有触发定时器 如果为0个 就new一个
                // 如果已经有了  就不用再new了
		ctx.timerService().registerEventTimeTimer(value.windowEndTime)
                triggerTimer.update(value.windowEndTime)
            }

        }
    }

    // 自定义窗口函数
    class MyProcessWindowFunction extends ProcessWindowFunction[Long, HotItemClick, Long, TimeWindow] {
        override def process(key: Long, context: Context, elements: Iterable[Long], out: Collector[HotItemClick]): Unit = {
            out.collect(HotItemClick(key, elements.iterator.next(), context.window.getEnd))
        }
    }

    // 自定义聚合函数
    class MyAggregateFunction extends AggregateFunction[UserBehavior, Long, Long] {
        override def createAccumulator(): Long = 0L

        override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

        override def getResult(accumulator: Long): Long = accumulator

        override def merge(a: Long, b: Long): Long = a + b
    }

    //    class MyAggregateFunction extends AggregateFunction[UserBehavior, (Long, Long), HotItemClick] {
    //        override def createAccumulator(): (Long, Long) = (0L, 0L)
    //
    //        override def add(value: UserBehavior, accumulator: (Long, Long)): (Long, Long) = {
    //            (value.itemId, accumulator._2 + 1L)
    //        }
    //
    //        override def getResult(accumulator: (Long, Long)): HotItemClick = {
    //            HotItemClick( accumulator._1, accumulator._2, 0L )
    //        }
    //
    //        override def merge(a: (Long, Long), b: (Long, Long)): (Long, Long) = {
    //            (a._1, a._2 + b._2)
    //        }
    //    }
    // 热门商品的点击样例类
    case class HotItemClick(itemId: Long, clickCount: Long, windowEndTime: Long)

    // 用户行为样例类
    case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

}
```



#### 基于服务器log的热门页面浏览量统计

对于一个电商平台而言，用户登录的入口流量、不同页面的访问流量都是值得分析的重要数据，而这些数据，可以简单地从web服务器的日志中提取出来。

我们在这里先实现“热门页面浏览数”的统计，也就是读取服务器日志中的每一行log，统计在一段时间内用户访问每一个url的次数，然后排序输出显示。

具体做法为：每隔5秒，输出最近10分钟内访问量最多的前N个URL。可以看出，这个需求与之前“实时热门商品统计”非常类似，所以我们完全可以借鉴此前的代码。

数据日志为apache.log，样例类为ApacheLog

```scala
case class ApacheLog(
    ip:String,
    userId:String,
    eventTime:Long,
    method:String,
    url:String)
```

```scala
package com.atguigu.bigdata.flink.chapter06

import com.atguigu.bigdata.flink.common.SimpleAggregateFunction
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import scala.collection.mutable.ListBuffer

object Flink35_Req_HotResourceRank_Analyses {

    def main(args: Array[String]): Unit = {

        // TODO 案例实操 - 需求 - 热门资源排行
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // TODO 1. 读取服务器的日志数据
        val logDS: DataStream[String] = env.readTextFile("input/apache.log")

        // TODO 2. 将数据转换为样例类方便使用
        val apacheLogDS: DataStream[ApacheLog] = logDS.map(
            data => {
                val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
                /**
                 *   G 年代标志符
                 *   y 年
                 *   M 月
                 *   d 日
                 *   h 时 在上午或下午 (1~12)
                 *   H 时 在一天中 (0~23)
                 *   m 分
                 *   s 秒
                 *   S 毫秒
                 *   E 星期
                 *   D 一年中的第几天
                 *   F 一月中第几个星期几
                 *   w 一年中第几个星期
                 *   W 一月中第几个星期
                 *   a 上午 / 下午 标记符
                 *   k 时 在一天中 (1~24)
                 *   K 时 在上午或下午 (0~11)
                 *   z 时区
                 */
                val datas: Array[String] = data.split(" ")
                ApacheLog(
                    datas(0),
                    datas(1),
                    // 此处返回的是毫秒数 以上面规定好的格式进行解析
                    sdf.parse(datas(3)).getTime,
                    datas(5),
                    datas(6)
                )
            }
        )

        // TODO 3. 从数据中抽取时间戳
        val timeDS: DataStream[ApacheLog] = apacheLogDS.assignTimestampsAndWatermarks(
            // 参数传进去的是乱序数据的最大偏离时间 也就是最多晚来两分钟
            new BoundedOutOfOrdernessTimestampExtractor[ApacheLog](Time.minutes(2)) {
                override def extractTimestamp(element: ApacheLog): Long = {
                    // 这里不用乘1000 因为上面getTime拿到的就是毫秒数
                    element.eventTime
                }
            }
        )

        // TODO 4. 根据url进行分组
        val urlKS: KeyedStream[ApacheLog, String] = timeDS.keyBy(_.url)

        // TODO 5. 增加窗口范围用于计算
        val urlWS: WindowedStream[ApacheLog, String, TimeWindow] = urlKS.timeWindow(
            Time.minutes(10),
            Time.seconds(5)
        )

        // TODO 6. 对分组后的窗口数据进行聚合
        // 同第一个需求一样 需要传两个参数进去 第一个是进行处理 第二个是拿到窗口信息
        // 窗口聚合函数
        // 1. 聚合函数
        // 2. 窗口处理函数
        val resourceClickDS: DataStream[HotResourceClick] = urlWS.aggregate(
            // 因为此函数需要反复用到 所以单独抽取出来 封装为一个公共的类
            new SimpleAggregateFunction[ApacheLog],
            new ProcessWindowFunction[Long, HotResourceClick, String, TimeWindow] {
                override def process(key: String,
                                     context: Context, elements: Iterable[Long],
                                     out: Collector[HotResourceClick]): Unit = {
                    out.collect(HotResourceClick(key, elements.iterator.next(), context.window.getEnd))
                }
            }
        )

        // TODO 7. 根据结束时间对数据进行分组
        val resourceClickKS: KeyedStream[HotResourceClick, Long] = resourceClickDS.keyBy(_.windowEndTime)

        // TODO 8. 对分组后的数据进行排序处理，然后获取前三名
        resourceClickKS.process(
            new KeyedProcessFunction[Long, HotResourceClick, String] {
                private var hotResourceList: ListState[HotResourceClick] = _
                private var triggerTimer: ValueState[Long] = _

                override def open(parameters: Configuration): Unit = {
                    hotResourceList = getRuntimeContext.getListState(
                        new ListStateDescriptor[HotResourceClick]("hotResourceList", classOf[HotResourceClick])
                    )
                    triggerTimer = getRuntimeContext.getState(
                        new ValueStateDescriptor[Long]("triggerTimer", classOf[Long])
                    )
                }

                override def onTimer(timestamp: Long,
                                     ctx: KeyedProcessFunction[Long, HotResourceClick, String]#OnTimerContext,
                                     out: Collector[String]): Unit = {

                    val datas: util.Iterator[HotResourceClick] = hotResourceList.get().iterator()
                    val buffer = new ListBuffer[HotResourceClick]()
                    while (datas.hasNext) {
                        buffer.append(datas.next())
                    }
                    hotResourceList.clear()
                    triggerTimer.clear()

                    val top3: ListBuffer[HotResourceClick] = buffer.sortWith(
                        (left, right) => {
                            left.clickCount > right.clickCount
                        }
                    ).take(3)

                    out.collect(
                        s"""
                           | 时间：${new Timestamp(timestamp)}
                           | -------------------------------
                           | ${top3.mkString("\n ")}
                           | -------------------------------
                         """.stripMargin)
                }

                override def processElement(value: HotResourceClick,
                                            ctx: KeyedProcessFunction[Long, HotResourceClick, String]#Context,
                                            out: Collector[String]): Unit = {
                    hotResourceList.add(value)

                    if (triggerTimer.value() == 0) {
                        ctx.timerService().registerEventTimeTimer(value.windowEndTime)
                        triggerTimer.update(value.windowEndTime)
                    }
                }
            }
        ).print("top3>>>")

        env.execute()
    }

    case class HotResourceClick(url: String, clickCount: Long, windowEndTime: Long)

    case class ApacheLog(ip: String,
                         userId: String,
                         eventTime: Long,
                         method: String,
                         url: String)

}

```

```scala
package com.atguigu.bigdata.flink.common

import org.apache.flink.api.common.functions.AggregateFunction

/**
  * 简单聚合处理函数
  * 用于执行简单数据累加的功能
  */
class SimpleAggregateFunction[T] extends AggregateFunction[T, Long, Long]{
    override def createAccumulator(): Long = 0L

    override def add(value: T, accumulator: Long): Long = accumulator + 1L

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
}
```



### 6.5.2 基于埋点日志数据的网络流量统计

#### 指定时间范围内网站总浏览量（PV - Page View）的统计

实现一个网站总浏览量的统计。我们可以设置**滚动时间**窗口，实时统计**每小时**内的网站PV。此前我们已经完成了该需求的流数据操作，当前需求是在之前的基础上增加了窗口信息，所以其他代码请参考之前的实现。 

```scala
package com.atguigu.bigdata.flink.chapter06

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object Flink32_Req_PV_WindowAnalyses {

    def main(args: Array[String]): Unit = {

        // TODO 案例实操 - 需求1 - PV统计
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        // 设定时间语义 EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // 1. 读取日志数据
        val dataDS: DataStream[String] =
            env.readTextFile("input/UserBehavior.csv")

        // (pv, 1)
        val pv2OneDS: DataStream[(String, Int, Long)] = dataDS.flatMap(
            data => {
                val datas = data.split(",")
                if (datas(3) == "pv") {
                    List(("pv", 1, datas(4).toLong))
                } else {
                    Nil
                }
            }
        )
        val timeDS = pv2OneDS.assignAscendingTimestamps(_._3 * 1000L)

        timeDS
                .keyBy(_._1)
                .timeWindow(Time.hours(1))
                .sum(1).print("pv>>>")


        env.execute()

    }

    // 用户行为样例类
    case class UserBehavior(userId: Long,
                            itemId: Long,
                            categoryId: Int,
                            behavior: String,
                            timestamp: Long)

}
```



#### 指定时间范围内网站独立访客数（UV - User View）的统计

统计流量的重要指标是网站的独立访客数（Unique Visitor，UV）。UV指的是一段时间（比如一小时）内访问网站的总人数。此前我们已经完成了该需求的流数据操作，当前需求是在之前的基础上增加了窗口信息，所以其他代码请参考之前的实现。

> 问题 这里为什么没有用`.assignAscendingTimestamps(_._4 * 1000L)`指定时间戳呢

```scala
package com.atguigu.bigdata.flink.chapter06

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import scala.collection.mutable

object Flink33_Req_UV_WindowAnalyses {

    def main(args: Array[String]): Unit = {

        // TODO 案例实操 - 需求2 - UV统计
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // 1. 读取日志数据
        val dataDS: DataStream[String] =
            env.readTextFile("input/UserBehavior.csv")

        // 2. 将数据根据结构拆分 做成("UV", 用户id, 时间戳)的形式
        val uv2UserDS: DataStream[(String, Long, Long)] = dataDS.map(data => {
            val datas : Array[String] = data.split(",")
            ("uv", datas(0).toLong, datas(4).toLong)
        })

        // 3. 将数据根据UV进行分组
        val uvKS: KeyedStream[(String, Long, Long), String] =
            uv2UserDS.keyBy(_._1)

        // 4. 将用户数据进行去重
        val uvProcessDS: DataStream[String] =
        uvKS.timeWindow(Time.hours(1)).process(
            //                        In Element Out Window
            new ProcessWindowFunction[(String, Long, Long), String, String, TimeWindow] {
                private val uvSet = mutable.Set[Long]()

                override def process(key: String,
                                     context: Context,
                                     elements: Iterable[(String, Long, Long)],
                                     out: Collector[String]): Unit = {
                    for (e <- elements) {
                        uvSet.add(e._2)
                    }

                    out.collect("当前网站独立访客数为 = " + uvSet.size)
                }
            }
        )

        uvProcessDS.print("uv>>>>")
        env.execute()

    }

    // 用户行为样例类
    case class UserBehavior(userId: Long,
                            itemId: Long,
                            categoryId: Int,
                            behavior: String,
                            timestamp: Long)

}
```



### 6.5.3 市场营销商业指标统计分析

#### 页面广告点击量统计

电商网站的市场营销商业指标中，除了自身的APP推广，还会考虑到页面上的广告投放（包括自己经营的产品和其它网站的广告）。所以广告相关的统计分析，也是市场营销的重要指标。

对于广告的统计，最简单也最重要的就是页面广告的点击量，网站往往需要根据广告点击量来制定定价策略和调整推广方式，而且也可以借此收集用户的偏好信息。

更加具体的应用是，我们可以根据用户的地理位置进行划分，从而总结出不同省份用户对不同广告的偏好，这样更有助于广告的精准投放。

在之前的需求实现中，已经统计了广告的点击次数总和，但是没有实现窗口操作，并且也未增加排名处理，具体实现请参考“热门点击商品”需求。

#### 黑名单过滤

我们进行的点击量统计，同一用户的重复点击是会叠加计算的。

在实际场景中，同一用户确实可能反复点开同一个广告，这也说明了用户对广告更大的兴趣；但是如果用户在一段时间非常频繁地点击广告，这显然不是一个正常行为，有刷点击量的嫌疑。

所以我们可以对一段时间内（比如一天内）的用户点击行为进行约束，如果对同一个广告点击超过一定限额（比如100次），应该把该用户加入黑名单并报警，此后其点击行为不应该再统计。

##### 1. 数据准备

```scala
case class PriAdClick( key:String, clickCount:Long, windowTimeEnd:Long )
case class AdClickLog(
     userId: Long,
     adId: Long,
     province: String,
     city: String,
     timestamp: Long)

val logDS: DataStream[String] = env.readTextFile("input/AdClickLog.csv")
```

##### 2. 转换数据结构

```scala
val adClickDS: DataStream[AdClickLog] = logDS.map(
    data => {
        val datas = data.split(",")
        AdClickLog(
            datas(0).toLong,
            datas(1).toLong,
            datas(2),
            datas(3),
            datas(4).toLong
        )
    }
)
```

##### 3. 抽取时间戳

```scala
val timeDS = adClickDS.assignAscendingTimestamps(_.timestamp * 1000L)

```


##### 4. 转换数据结构用于统计分析

```scala
val logTimeDS = timeDS.map(
    log => {
        ( log.userId + "_" + log.adId, 1L )
    }
)
```


##### 5. 根据用户ID和广告ID进行分组

```scala
val logTimeDS = timeDS.map(
    log => {
        ( log.userId + "_" + log.adId, 1L )
    }
)
```


##### 6. 对分组后的数据进行统计

```scala
private var clickCount: ValueState[Long] = _
private var alarmStatus: ValueState[Boolean] = _

override def open(parameters: Configuration): Unit = {
    clickCount = getRuntimeContext.getState(
        new ValueStateDescriptor[Long]("clickCount", classOf[Long])
    )
    alarmStatus = getRuntimeContext.getState(
        new ValueStateDescriptor[Boolean]("alarmStatus", classOf[Boolean])
    )
}
```


##### 7. 超出点击阈值的数据，输出到侧输出流中进行预警

```scala
val count = clickCount.value()

if (count >= 99) {
    // 如果点击广告的数量超过了阈值，那么将数据输出到侧输出流中。可以进行预警功能
    if (!alarmStatus.value()) {
        val outputTag = new OutputTag[(String, Long)]("blackList")
        ctx.output(outputTag, (value._1, count))
        alarmStatus.update(true)
    }
} else {
    clickCount.update(count + 1)
    out.collect(value)
}

val outputTag = new OutputTag[(String, Long)]("blackList")
logProcessDS.getSideOutput(outputTag).print("blackList>>>")
```


##### 8. 第二天零时清空数据状态

```scala
// TODO 1. 获取当前数据的处理时间
var currentTime = ctx.timerService().currentProcessingTime()
// 2020-02-22 12:12:12
// 2020-02-22 00:00:00
var day = currentTime / (1000 * 60 * 60 * 24)

// TODO 2. 获取第二天零时时间
// 2020-02-23 00:00:00
val nextDay = day + 1
val nextDayTime = nextDay * (1000 * 60 * 60 * 24)

// TODO 3. 设定定时器
ctx.timerService().registerProcessingTimeTimer(nextDayTime)
```


##### 9. 其他数据进入正常数据流

```scala
logProcessDS
    .keyBy(_._1)
    .timeWindow(
        Time.hours(1),
        Time.minutes(5)
    )
    .aggregate(
        new SimpleAggregateFunction[(String, Long)],
        new ProcessWindowFunction[Long, PriAdClick,String, TimeWindow ] {
            override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[PriAdClick]): Unit = {
                out.collect(PriAdClick(key, elements.iterator.next(), context.window.getEnd))
            }
        }
    )
    .keyBy(_.windowTimeEnd)
    .process(
        new KeyedProcessFunction[Long, PriAdClick, String] {
            override def processElement(value: PriAdClick, ctx: KeyedProcessFunction[Long, PriAdClick, String]#Context, out: Collector[String]): Unit = {
                out.collect( "广告ID" + value.key + "点击量为" + value.clickCount )
            }
        }
    ).print("advClick>>>")
```

```scala
// 课堂
package com.atguigu.bigdata.flink.chapter06

import com.atguigu.bigdata.flink.common.SimpleAggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Flink38_Req_AdvClick_BlackList1 {

    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val logDS: DataStream[String] = env.readTextFile("input/AdClickLog.csv")

        val adClickDS: DataStream[AdClickLog] = logDS.map(
            data => {
                val datas: Array[String] = data.split(",")
                AdClickLog(
                    datas(0).toLong,
                    datas(1).toLong,
                    datas(2),
                    datas(3),
                    datas(4).toLong
                )
            }
        )

        val timeDS: DataStream[AdClickLog] = adClickDS.assignAscendingTimestamps(_.timestamp * 1000L)

        val logTimeDS: DataStream[(String, Long)] = timeDS.map(
            log => {
                (log.userId + "_" + log.adId, 1L)
            }
        )

        val logTimeKS: KeyedStream[(String, Long), String] =
            logTimeDS
                    .keyBy(_._1)

        val logProcessDS: DataStream[(String, Long)] = logTimeKS.process(
            new KeyedProcessFunction[String, (String, Long), (String, Long)] {
                private var clickCount: ValueState[Long] = _
                // 保存是否已经进行过报警的状态
                private var alarmStatus: ValueState[Boolean] = _

                override def open(parameters: Configuration): Unit = {
                    clickCount = getRuntimeContext.getState(
                        new ValueStateDescriptor[Long]("clickCount", classOf[Long])
                    )
                    alarmStatus = getRuntimeContext.getState(
                        new ValueStateDescriptor[Boolean]("alarmStatus", classOf[Boolean])
                    )
                }


                override def onTimer(timestamp: Long,
                                     ctx: KeyedProcessFunction[String, (String, Long), (String, Long)]#OnTimerContext,
                                     out: Collector[(String, Long)]): Unit = {
                    // 到了第二天的零点将有状态的值进行归零
                    clickCount.clear()
                    alarmStatus.clear()
                }

                override def processElement(value: (String, Long),
                                            ctx: KeyedProcessFunction[String, (String, Long), (String, Long)]#Context,
                                            out: Collector[(String, Long)]): Unit = {
                    val count: Long = clickCount.value()

                    // 在第一次获取到用户点击行为的时候 获取当前的日期并设定定时器 在第二天的0点触发 把之前统计的count进行清空
                    if (count == 0) {
                        // TODO 1. 获取当前数据的处理时间
                        // 此处获取的时间应该是当前的真实时间而不应该是EventTime
                        // 因为就算此时没有来数据 也应该把这个count归零
                        var currentTime: Long = ctx.timerService().currentProcessingTime()
                        // window => window start
                        // 22 => 20, 25
                        // 2-22 13:50
                        // 相当于把当前时间的时间戳 进行取整 类似于window窗口中的求起始时间值
                        var day: Long = currentTime / (1000 * 60 * 60 * 24)

                        // TODO 2. 获取第二天零时时间
                        // 2/23 00:00
                        val nextDay: Long = day + 1
                        val nextDayTime: Long = nextDay * (1000 * 60 * 60 * 24)

                        // TODO 3. 设定定时器
                        ctx.timerService().registerProcessingTimeTimer(nextDayTime)
                    }

                    // 取到的是当前key的点击总数 如果该数值已经到了99 那么加上这一条已经到了我们设定的阈值100
                    if (count >= 99) {
                        // 如果点击广告的数量超过了阈值，那么将数据输出到侧输出流中。可以进行预警功能
                        // 如果已经进行过报警 则alarmStatus的值为true 就不进行报警了
                        if (!alarmStatus.value()) {
                            val outputTag = new OutputTag[(String, Long)]("blackList")
                            ctx.output(outputTag, (value._1, count))
                            alarmStatus.update(true)
                        }
                    } else {
                        clickCount.update(count + 1)
                        out.collect(value)
                    }
                }
            }
        )
        val outputTag = new OutputTag[(String, Long)]("blackList")
        logProcessDS.getSideOutput(outputTag).print("blackList>>>")

        // logProcessDS 现在是: DataStream[(String, Long)] 类型 已经不是一个KeyedStream了
        // 所以现在如果需要做统计 要重新做一遍keyBy操作
        logProcessDS
                .keyBy(_._1)
                .timeWindow(
                    Time.hours(1),
                    Time.minutes(5)
                )
                .aggregate(
                    new SimpleAggregateFunction[(String, Long)],
                    new ProcessWindowFunction[Long, PriAdClick, String, TimeWindow] {
                        override def process(key: String,
                                             context: Context,
                                             elements: Iterable[Long],
                                             out: Collector[PriAdClick]): Unit = {
                            out.collect(PriAdClick(key, elements.iterator.next(), context.window.getEnd))
                        }
                    }
                )
                .keyBy(_.windowTimeEnd)
                .process(
                    new KeyedProcessFunction[Long, PriAdClick, String] {
                        override def processElement(value: PriAdClick,
                                                    ctx: KeyedProcessFunction[Long, PriAdClick, String]#Context,
                                                    out: Collector[String]): Unit = {
                            out.collect("广告ID" + value.key + "点击量为" + value.clickCount)
                        }
                    }
                ).print("advClick>>>")

        env.execute()
    }

    case class PriAdClick(key: String, clickCount: Long, windowTimeEnd: Long)

    case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

}

```



### 6.5.4 恶意登录监控

#### 恶意登录监控

对于网站而言，用户登录并不是频繁的业务操作。如果一个用户短时间内频繁登录失败，就有可能是出现了程序的恶意攻击，比如密码暴力破解。因此我们考虑，应该对用户的登录失败动作进行统计，具体来说，如果同一用户（可以是不同IP）在2秒之内连续两次登录失败，就认为存在恶意登录的风险，输出相关的信息进行报警提示。这是电商网站、也是几乎所有网站风控的基本一环。

当前需求的数据来源于LoginLog.csv，使用时可转换为样例类LoginEvent

```scala
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)
```

### 6.5.5 订单支付实时监控

#### 订单支付实时监控

在电商网站中，订单的支付作为直接与营销收入挂钩的一环，在业务流程中非常重要。对于订单而言，为了正确控制业务流程，也为了增加用户的支付意愿，网站一般会设置一个支付失效时间，超过一段时间不支付的订单就会被取消。另外，对于订单的支付，我们还应保证用户支付的正确性，这可以通过第三方支付平台的交易数据来做一个实时对账。

当前需求的数据来源于OrderLog.csv，使用时可转换为样例类OrderEvent

```scala
case class OrderEvent( orderId: Long, eventType: String, txId: String, eventTime: Long )
```





#### 订单支付超时需求的实现

```scala
package com.atguigu.bigdata.flink.chapter06

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

object Flink40_Req_OrderPayTimeout {

    def main(args: Array[String]): Unit = {

        // TODO 订单支付超时
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // TODO 1. 读取订单日志数据
        val dataDS: DataStream[String] =
            env.readTextFile("input/OrderLog.csv")

        // TODO 2. 转换数据结构便于访问
        val orderDS: DataStream[OrderEvent] = dataDS.map(
            data => {
                val datas: Array[String] = data.split(",")
                OrderEvent(
                    datas(0).toLong,
                    datas(1),
                    datas(2),
                    datas(3).toLong
                )
            }
        )

        // TODO 3. 抽取时间戳
        val orderTimeDS: DataStream[OrderEvent] =
            orderDS.assignAscendingTimestamps(_.eventTime * 1000)

        // TODO 4. 将数据根据订单ID进行分组
        val orderTimeKS: KeyedStream[OrderEvent, Long] = orderTimeDS.keyBy(_.orderId)

        // TODO 5. 对每一条数据进行处理
        val orderProcessDS: DataStream[String] = orderTimeKS.process(
            new KeyedProcessFunction[Long, OrderEvent, String] {

                private var payData: ValueState[OrderEvent] = _
                private var orderData: ValueState[OrderEvent] = _
                private var alarmTimer: ValueState[Long] = _

                override def open(parameters: Configuration): Unit = {
                    payData = getRuntimeContext.getState(
                        new ValueStateDescriptor[OrderEvent]("payData", classOf[OrderEvent])
                    )
                    orderData = getRuntimeContext.getState(
                        new ValueStateDescriptor[OrderEvent]("orderData", classOf[OrderEvent])
                    )
                    alarmTimer = getRuntimeContext.getState(
                        new ValueStateDescriptor[Long]("alarmTimer", classOf[Long])
                    )
                }

                // 缺少数据就会触发定时器
                override def onTimer(timestamp: Long,
                                     ctx: KeyedProcessFunction[Long, OrderEvent, String]#OnTimerContext,
                                     out: Collector[String]): Unit = {
                    val outputTag = new OutputTag[String]("timeout")
                    if (payData.value() != null) {
                        ctx.output(outputTag, "订单" + payData.value().orderId + "支付数据不完整，支付失败！！！")
                    }
                    payData.clear()
                    if (orderData.value() != null) {
                        ctx.output(outputTag, "订单" + orderData.value().orderId + "支付数据不完整，支付失败！！！")
                    }
                    orderData.clear()
                    alarmTimer.clear()
                }

                // 读取文件时，如果读取完毕。Flink会将watermark设定为Long的最大值
                // 要将所有的未触发的计算进行触发
                override def processElement(value: OrderEvent,
                                            ctx: KeyedProcessFunction[Long, OrderEvent, String]#Context,
                                            out: Collector[String]): Unit = {

                    // 如果数据到达后，应该设置定时器
                    if (alarmTimer.value() == 0) {
                        // 下面一行代码是我们为了演示触发定时器使用的 实际生产中 应该使用ProcessingTimeTimer
                        // 从处理数据开始往后算15分钟
                        //ctx.timerService().registerEventTimeTimer(
                        ctx.timerService().registerProcessingTimeTimer(
                            ctx.timerService().currentProcessingTime() + 1000 * 60 * 15
                        )
                        alarmTimer.update(ctx.timerService().currentProcessingTime() + 1000 * 60 * 15)
                    } else {
                        ctx.timerService().deleteProcessingTimeTimer(alarmTimer.value())
                        //ctx.timerService().deleteEventTimeTimer(alarmTimer.value())
                    }

                    if (value.eventType == "create") {
                        // 下单数据来了
                        // 判断支付数据是否存在
                        val pay: OrderEvent = payData.value()
                        if (pay == null) {
                            // 支付数据没有来
                            // 将下单数据临时保存起来
                            orderData.update(value)
                        } else {
                            // 支付数据来了
                            if (pay.eventTime - value.eventTime >= 60 * 15) {
                                val outputTag = new OutputTag[String]("timeout")
                                //out.collect("订单"+value.orderId +"支付超时!!!!!!!!")
                                ctx.output(outputTag, "订单" + value.orderId + "支付超时!!!!!!!!")
                            } else {
                                out.collect("订单" + value.orderId + "支付成功")
                                payData.clear()
                            }
                        }
                    } else {
                        // 支付数据来了
                        // 判断下单数据是否来了
                        val order: OrderEvent = orderData.value()
                        if (order == null) {
                            // 下单数据没来
                            // 将支付数据临时保存起来，等待下单数据的到来
                            payData.update(value)
                        } else {
                            // 判断支付是否超时
                            if (value.eventTime - order.eventTime >= 60 * 15) {
                                val outputTag = new OutputTag[String]("timeout")
                                //out.collect("订单"+value.orderId +"支付超时!!!!!!!!")
                                ctx.output(outputTag, "订单" + order.orderId + "支付超时!!!!!!!!")
                            } else {
                                out.collect("订单" + order.orderId + "支付成功")
                                orderData.clear()
                            }
                        }
                    }
                }
            }
        )
        val outputTag = new OutputTag[String]("timeout")
        orderProcessDS.getSideOutput(outputTag).print("timeout>>>>")
        orderProcessDS.print("order>>>>")

        env.execute()
    }

    case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)

}
```

### 补充 UV去重的优化

```scala
package com.atguigu.bigdata.flink.chapter08

import java.lang

import com.atguigu.bigdata.flink.common.MockBloomFilter
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

object Flink14_UV_Redis_Bitmap {

    def main(args: Array[String]): Unit = {

        // TODO 使用Redis的位图操作实现网站的UV统计
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val dataDS: DataStream[String] =
            env.readTextFile("input/UserBehavior.csv")

        val userBehaviorDS: DataStream[UserBehavior] = dataDS.map(
            data => {
                val datas: Array[String] = data.split(",")
                UserBehavior(
                    datas(0).toLong,
                    datas(1).toLong,
                    datas(2).toInt,
                    datas(3),
                    datas(4).toLong
                )
            }
        )

        val timeDS: DataStream[UserBehavior] = userBehaviorDS.assignAscendingTimestamps(_.timestamp * 1000L)

        val useridDS: DataStream[Long] = timeDS.map(
            data => data.userId
        )

        // 这里拿到的是全窗口数据 重复的不重复的 所有userid全都在里面
        val useridWS: AllWindowedStream[Long, TimeWindow] = useridDS.timeWindowAll(Time.hours(1))
        useridWS
                // 触发器，可以决定窗口计算何时触发
                .trigger(
                    new Trigger[Long, TimeWindow] {
                        // 当数据到达窗口时，如何处理？
                        // FIRE_AND_PURGE 处理数据 并清除数据
                        override def onElement(element: Long, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
                            TriggerResult.FIRE_AND_PURGE
                        }

                        // 当处理时间到达时，如何处理？
                        override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
                            TriggerResult.CONTINUE
                        }

                        // 当事件时间到达时，如何处理？
                        override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
                            TriggerResult.CONTINUE
                        }

                        // 清除触发器
                        override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {

                        }
                    }
                )
                .process(
                    new ProcessAllWindowFunction[Long, String, TimeWindow] {

                        private var jedis: Jedis = _

                        // 初始化
                        override def open(parameters: Configuration): Unit = {
                            jedis = new Jedis("linux4", 6379)
                        }

                        //elements表示当前时间窗口中所有的数据
                        override def process(context: Context, elements: Iterable[Long], out: Collector[String]): Unit = {

                            // 获取当前窗口的计算时间
                            val windowEndTime: String = context.window.getEnd.toString

                            // 获取用户ID
                            val userid: String = elements.iterator.next().toString

                            // 获取数据在位置中的位置
                            val offset: Long = MockBloomFilter.offset(userid, 10)

                            // 从Redis中获取位图的状态
                            val boolean: lang.Boolean = jedis.getbit(windowEndTime, offset)

                            if (boolean) {
                                // 如果状态是存在（1），什么都不做
                            } else {
                                // 如果状态为不存在（0），更新UV值，并且更新位图的状态（1）
                                jedis.setbit(windowEndTime, offset, true)
                                // 获取UV的值
                                var uvcount: String = jedis.hget("uvcount", windowEndTime)
                                if (uvcount == null || "".equals(uvcount)) {
                                    uvcount = "1"
                                } else {
                                    uvcount = (uvcount.toLong + 1) + ""
                                }
                                // 更新UV值
                                jedis.hset("uvcount", windowEndTime, uvcount)
                                out.collect("新的独立访客：" + userid)
                            }
                        }
                    }
                )
                .print("uv>>>>")

        env.execute()
    }

    case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)


}
```

```scala
// --- 封装一个方法 用来生成随机offset ---
package com.atguigu.bigdata.flink.common

/**
 * 计算数据在位图中的偏移量
 */
object MockBloomFilter {

    val cap: Int = 1 << 29

    def main(args: Array[String]): Unit = {
        // 以下打印做测试用 并无实际意义
        println(offset("abc", 10)) // 10779
        println(offset("abc", 5))  // 3014
    }

    def offset(s: String, seed: Int): Long = {
        var hash = 0
        for (c <- s) {
            hash = hash * seed + c
        }
        hash & (cap - 1)
    }
}
```

