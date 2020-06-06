---

---

# **第1章** Flink CEP编程

## **7.1** 什么是CEP

`Complex Event Processing`

一个或多个由简单事件构成的事件流通过一定的规则匹配，然后输出用户想得到的数据，满足规则的复杂事件。

特征：

- 目标：从有序的简单事件流中发现一些高阶特征

- 输入：一个或多个由简单事件构成的事件流

- 处理：识别简单事件之间的内在联系，多个符合一定规则的简单事件构成复杂事件

- 输出：满足规则的复杂事件

![](https://img.lormer.cn/20200606005834.png)

## 7.2 CEP特点

CEP用于分析低延迟、频繁产生的不同来源的事件流。CEP可以帮助在复杂的、不相关的事件流中找出有意义的模式和复杂的关系，以接近实时或准实时的获得通知并阻止一些行为。

CEP支持在流上进行模式匹配，根据模式的条件不同，分为连续的条件或不连续的条件；模式的条件允许有时间的限制，当在条件范围内没有达到满足的条件时，会导致模式匹配超时。

看起来很简单，但是它有很多不同的功能：

- 输入的流数据，尽快产生结果

- 在2个event流上，基于时间进行聚合类的计算

- 提供实时/准实时的警告和通知

- 在多样的数据源中产生关联并分析模式

- 高吞吐、低延迟的处理

市场上有多种CEP的解决方案，例如Spark、Samza、Beam等，但他们都没有提供专门的library支持。

但是Flink提供了专门的CEP library。

## 7.3 CEP API

### 7.3.1 基本开发步骤

1. 如果想要使用Flink提供的CEP库，首先需要引入相关的依赖关系

```scala
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-cep-scala_2.11</artifactId>
    <version>1.10.0</version>
</dependency>
```

2. 定义数据流

```scala
val env =
    StreamExecutionEnvironment.getExecutionEnvironment
env.setParallelism(1)

val dataDS = env.readTextFile("input/sensor-data.log")

val sensorDS = dataDS.map(
    data => {
        val datas = data.split(",")
        WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
    }
)
```

3. 定义规则

```scala
val pattern =
    Pattern.begin[WaterSensor]("begin").where(_.id == "a")
```

4. 应用规则

```scala
val sensorPS =
    CEP.pattern(sensorDS, pattern)
```

5. 获取结果

```scala
val ds = sensorPS.select(
    map => {
        map.toString
    }
)
ds.print("cep>>>>")
```

```scala
// --- 课堂演示 ---
// 一个最简单的CEP编程
package com.atguigu.bigdata.flink.chapter07

import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

object Flink00_CEP {
    def main(args: Array[String]): Unit = {

        val env =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // TODO 1. 准备数据
        val dataDS: DataStream[String] =
            env.readTextFile("input/sensor-data1.log")

        val sensorDS = dataDS.map(
            data => {
                val datas = data.split(",")
                WaterSensor(
                    datas(0),
                    datas(1).toLong,
                    datas(2).toInt
                )
            }
        )

        // TODO 2. 定义规则
        // Scala语言应该准备对应的Scala CEP库
        val pattern =
        Pattern
                .begin[WaterSensor]("start")
                .where(_.id == "sensor_1")

        // TODO 3. 将规则应用到数据流中
        val sensorPS: PatternStream[WaterSensor] = CEP.pattern(sensorDS, pattern)

        // TODO 4. 从规则流获取符合规则的数据流
        val selectDS: DataStream[String] = sensorPS.select(
            map => {
                map.toString
            }
        )
        selectDS.print("cep>>>")

        env.execute()
    }


    case class WaterSensor(id: String, ts: Long, height: Int)

}

```



### 7.3.2 匹配规则

每个匹配规则都需要指定触发条件，作为是否接受事件进入的判断依据

按不同的调用方式，可以分成以下几类

#### 条件匹配

1. 简单条件

> where 相当于 且/and 可以进行多次调用 
>
> or 相当于 或/or 可以进行多次调用

```scala
Pattern
    .begin[(String, String)]("start")
        .where(_._1 == "a")// 并且条件
```

2. 组合条件

```scala
Pattern
    .begin[(String, String)]("start")
        .where(_._1 == "a")
        .or(_._1 == "b") // 或条件
```

3. 终止条件

```scala
Pattern
    .begin[(String, String)]("start")
        .where(_._1 == "b")
        .oneOrMore.until(_._2 == "4")
```

#### 模式序列

1. 严格近邻

   严格的满足联合条件, 当且仅当数据为连续的a,b时，模式才会被命中。如果数据为a,c,b，由于a的后面跟了c，所以a会被直接丢弃，模式不会命中。
   
   > 严格模式中 如果先来了a 后面紧跟来了c 这时候a会被丢弃

```scala
Pattern
    .begin[(String, String)]("start")
        .where(_._1 == "a")
	// 表示下一条数据的规则
    .next("next")
        .where(_._1 == "b")
```

2. 宽松近邻

   > 命中后不再匹配

   松散的满足联合条件, 当且仅当数据为a,b或者为a,c,b，模式均被命中，中间的c会被忽略掉。

```scala
Pattern
    .begin[(String, String)]("start")
        .where(_._1 == "a")
    .followedBy("followedBy")
        .where(_._1 == "b")
```

3. 非确定性宽松近邻

   > 一旦数据匹配成功  还是否会继续匹配

   非确定的松散满足条件, 当且仅当数据为a,c,b,b时，对于followedBy模式而言命中的为{a,b}，对于followedByAny而言会有两次命中{a,b},{a,b}

```scala
Pattern
    .begin[(String, String)]("start")
        .where(_._1 == "a")
    .followedByAny("followedByAny")
        .where(_._1 == "b")
```

---

```scala
// --- 课堂演示代码 ---
package com.atguigu.bigdata.flink.chapter07

import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

object Flink01_CEP {
    def main(args: Array[String]): Unit = {

        val env =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // TODO 1. 准备数据
        val dataDS: DataStream[String] =
            env.readTextFile("input/sensor-data1.log")

        val sensorDS = dataDS.map(
            data => {
                val datas = data.split(",")
                WaterSensor(
                    datas(0),
                    datas(1).toLong,
                    datas(2).toInt
                )
            }
        )

        // TODO 2. 定义规则
        // Scala语言应该准备对应的Scala CEP库
        // 函数链
        // where函数类似于SQL条件中 and, where函数可以多次调用
        // or 函数类似于SQL条件中的 or, or函数可以多次调用
        // next表示严格近邻
        // followedBy表示宽松近邻
        // followedByAny非确定性宽松近邻
        val pattern =
        Pattern
                .begin[WaterSensor]("start")
                .where(_.id == "sensor_1")
                //.where(_.height == 5)
                //.or(_.id == "sensor_2")
                //.next("next")
                //.followedBy("follow")
                .followedByAny("followAny")
                .where(_.id == "sensor_2")

        // TODO 3. 将规则应用到数据流中
        val sensorPS: PatternStream[WaterSensor] = CEP.pattern(sensorDS, pattern)

        // TODO 4. 从规则流获取符合规则的数据流
//        val selectDS: DataStream[String] = sensorPS.select(
//            map => {
//                map.toString
//            }
//        )
val outputTag = new OutputTag[String]("timeout")
        val selectDS =
            sensorPS.select(outputTag) {
                (map, ts) => {
                    map.toString
                }
            } {
                (map) => {
                    map.toString
                }
            }
        selectDS.getSideOutput(outputTag).print("timeout")
        selectDS.print("cep>>>")

        env.execute()
    }

    case class WaterSensor(id: String, ts: Long, height: Int)

}
```



#### 量词（Quantifier）

1. 固定次数(N)

   会把连续满足条件的情况返回 

```scala
Pattern
    .begin[(String, String)]("start")
        .where(_._1 == "sensor1")
        .times(3)
```

2. 多次数(N1, N2, N3)

```scala
Pattern
    .begin[(String, String)]("start")
        .where(_._1 == "sensor1")
		// 或者出现1次 或者连续出现3次
        .times(1,3)
```

```scala
// --- 课堂演示代码 ---
package com.atguigu.bigdata.flink.chapter07

import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

object Flink02_CEP1 {
    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // TODO 1. 准备数据
        val dataDS: DataStream[String] =
            env.readTextFile("input/sensor-data1.log")

        val sensorDS: DataStream[WaterSensor] = dataDS.map(
            data => {
                val datas: Array[String] = data.split(",")
                WaterSensor(
                    datas(0),
                    datas(1).toLong,
                    datas(2).toInt
                )
            }
        )

        // TODO 2. 定义规则

        // times 表示出现次数
        val pattern: Pattern[WaterSensor, WaterSensor] =
            Pattern
                    .begin[WaterSensor]("start")
                    .where(_.id == "sensor_1")
                    .times(2, 3)

        // TODO 3. 将规则应用到数据流中
        val sensorPS: PatternStream[WaterSensor] = CEP.pattern(sensorDS, pattern)

        // TODO 4. 从规则流获取符合规则的数据流
        val selectDS: DataStream[String] = sensorPS.select(
            map => {
                map.toString
            }
        )

        selectDS.print("cep>>>")

        env.execute()
    }

    case class WaterSensor(id: String, ts: Long, height: Int)

}

```



#### 超时

```scala
Pattern
    .begin[(String, String)]("start")
        .where(_._1 == "sensor1")
        .within(Time.minutes(5))
```

```scala
// --- 课堂演示代码 ---
package com.atguigu.bigdata.flink.chapter07

import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

object Flink03_CEP3_Timeout {
    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // TODO 1. 准备数据
        val dataDS: DataStream[String] =
            env.readTextFile("input/sensor-data1.log")

        val sensorDS: DataStream[WaterSensor] = dataDS.map(
            data => {
                val datas: Array[String] = data.split(",")
                WaterSensor(
                    datas(0),
                    datas(1).toLong,
                    datas(2).toInt
                )
            }
        )

        val timeDS: DataStream[WaterSensor] = sensorDS.assignAscendingTimestamps(_.ts * 1000L)

        // TODO 2. 定义规则

        // times 表示出现次数
        // within可以判断是否超时
        val pattern: Pattern[WaterSensor, WaterSensor] =
        Pattern
                .begin[WaterSensor]("start")
                .where(_.id == "sensor_1")
                .next("next")
                .where(_.id == "sensor_1")
                .within(Time.seconds(3))

        // TODO 3. 将规则应用到数据流中
        val sensorPS: PatternStream[WaterSensor] = CEP.pattern(timeDS, pattern)

        // TODO 4. 从规则流获取符合规则的数据流
        val selectDS: DataStream[String] = sensorPS.select(
            map => {
                map.toString
            }
        )

        selectDS.print("cep>>>")

        env.execute()
    }

    case class WaterSensor(id: String, ts: Long, height: Int)

}
```



## **7.4** **案例实操**

### 7.4.1 恶意登录监控

**恶意登录监控**

对于网站而言，用户登录并不是频繁的业务操作。如果一个用户短时间内频繁登录失败，就有可能是出现了程序的恶意攻击，比如密码暴力破解。因此我们考虑，应该对用户的登录失败动作进行统计，具体来说，如果同一用户（可以是不同IP）在2秒之内连续两次登录失败，就认为存在恶意登录的风险，输出相关的信息进行报警提示。这是电商网站、也是几乎所有网站风控的基本一环。

当前需求在上一章节的案例实操中已经完成，当前改善为CEP实现

```scala
// --- 课堂演示代码 使用CEP实现监测2秒内的失败登录 ---
package com.atguigu.bigdata.flink.chapter07

import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

object Flink04_Req_LoginErrorWithCEP {
    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // TODO 1. 准备数据
        val dataDS: DataStream[String] =
            env.readTextFile("input/LoginLog.csv")

        val loginDS: DataStream[LoginEvent] = dataDS.map(
            data => {
                val datas: Array[String] = data.split(",")
                LoginEvent(
                    datas(0).toLong,
                    datas(1),
                    datas(2),
                    datas(3).toLong
                )
            }
        )

        // TODO 2. 抽取时间戳（乱序）
        val timeDS: DataStream[LoginEvent] = loginDS.assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(10)) {
                override def extractTimestamp(element: LoginEvent): Long = {
                    element.eventTime * 1000L
                }
            }
        )

        // TODO 3. 将数据根据用户ID进行分组
        val loginKS: KeyedStream[LoginEvent, Long] = timeDS.keyBy(_.userId)

        // TODO 4. 定义CEP规则
        val pattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("start")
                .where(_.eventType == "fail")
                .next("next")
                .where(_.eventType == "fail")
                .within(Time.seconds(2))

        // TODO 5. 应用规则
        val loginPS: PatternStream[LoginEvent] = CEP.pattern(loginKS, pattern)

        // TODO 6. 获取符合规则的数据
        loginPS.select(
            map => {
                map.toString()
            }
        ).print("cep>>>")

        env.execute()
    }

    case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

}
// 即使数据是乱序的也可以正确监测
```



### 7.4.2 订单支付实时监控

**订单支付实时监控**

在电商网站中，订单的支付作为直接与营销收入挂钩的一环，在业务流程中非常重要。对于订单而言，为了正确控制业务流程，也为了增加用户的支付意愿，网站一般会设置一个支付失效时间，超过一段时间不支付的订单就会被取消。另外，对于订单的支付，我们还应保证用户支付的正确性，这可以通过第三方支付平台的交易数据来做一个实时对账。

当前需求在上一章节的案例实操中已经完成，当前改善为CEP实现

```scala
// --- 代码实现 ---
package com.atguigu.bigdata.flink.chapter07

import java.sql.Timestamp
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

object Flink05_Req_OrderPayTimeoutWithCEP {
    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // TODO 1. 准备数据
        val dataDS: DataStream[String] =
            env.readTextFile("input/OrderLog.csv")

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

        val timeDS: DataStream[OrderEvent] = orderDS.assignAscendingTimestamps(_.eventTime * 1000L)

        // TODO 将数据根据订单进行分组
        val orderKS: KeyedStream[OrderEvent, Long] = timeDS.keyBy(_.orderId)

        // TODO 定义规则
        val pattern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("start")
                .where(_.eventType == "create")
                .followedBy("follow")
                .where(_.eventType == "pay")
                .within(Time.minutes(15))

        // TODO 将规则应用到流中
        val orderPayPS: PatternStream[OrderEvent] = CEP.pattern(orderKS, pattern)

        // TODO 从流中获取数据
        orderPayPS.select(
            map => {
                val order: OrderEvent = map("start").iterator.next()
                val pay: OrderEvent = map("follow").iterator.next()

                val s: String =
                    s"""
                       | --------------------------
                       | 订单ID ： ${order.orderId}
                       | 创建时间：${new Timestamp(order.eventTime * 1000L)}
                       | 支付时间：${new Timestamp(pay.eventTime * 1000L)}
                       | 共耗时：${pay.eventTime - order.eventTime}秒
                       | --------------------------
                    """.stripMargin
                s
            }
        ).print("order<->pay >>>> ")

        env.execute()
    }

    case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)

}
```

```scala
// --- 对超时数据进行处理 输出到侧输出流 ---

package com.atguigu.bigdata.flink.chapter07

import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

object Flink06_Req_OrderPayTimeoutWithCEP1 {
    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // TODO 1. 准备数据
        val dataDS: DataStream[String] =
            env.readTextFile("input/OrderLog.csv")

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

        val timeDS: DataStream[OrderEvent] = orderDS.assignAscendingTimestamps(_.eventTime * 1000L)

        // TODO 将数据根据订单进行分组
        val orderKS: KeyedStream[OrderEvent, Long] = timeDS.keyBy(_.orderId)

        // TODO 定义规则
        val pattern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("start")
                .where(_.eventType == "create")
                .followedBy("follow")
                .where(_.eventType == "pay")
                .within(Time.minutes(15))

        // TODO 将规则应用到流中
        val orderPayPS: PatternStream[OrderEvent] = CEP.pattern(orderKS, pattern)

        // TODO 从流中获取数据
        // select方法可以支持数据超时处理，如果数据超时了，将数据放置在侧输出流中
        val outputTag = new OutputTag[String]("timeout")
        val selectDS: DataStream[String] =
            orderPayPS.select(outputTag)(
                (map, ts) => {
                    map.toString
                }
            )(
                map => {
                    map.toString()
                }
            )

        selectDS.getSideOutput(outputTag).print("timeout")

        env.execute()
    }

    case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)

}
```

## 补充 使用双流join实现对账功能

![](https://img.lormer.cn/20200606054815.png)

```scala
// 官网示例代码
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

...

val orangeStream: DataStream[Integer] = ...
val greenStream: DataStream[Integer] = ...

orangeStream
    .keyBy(elem => /* select key */)
    .intervalJoin(greenStream.keyBy(elem => /* select key */))
    .between(Time.milliseconds(-2), Time.milliseconds(1))
    .process(new ProcessJoinFunction[Integer, Integer, String] {
        override def processElement(left: Integer, right: Integer, ctx: ProcessJoinFunction[Integer, Integer, String]#Context, out: Collector[String]): Unit = {
         out.collect(left + "," + right); 
        }
      });
    });
```

```scala
package com.atguigu.bigdata.flink.chapter07

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object Flink07_Req_OrderTx_Analyses {

    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val logDS1: DataStream[String] = env.readTextFile("input/OrderLog.csv")
        val orderDS: DataStream[OrderEvent] = logDS1.map(
            log => {
                val datas: Array[String] = log.split(",")
                OrderEvent(
                    datas(0).toLong,
                    datas(1),
                    datas(2),
                    datas(3).toLong
                )
            }
        )
        val orderKS: KeyedStream[OrderEvent, String] = orderDS.assignAscendingTimestamps(_.eventTime * 1000L).keyBy(_.txId)

        val logDS2: DataStream[String] = env.readTextFile("input/ReceiptLog.csv")
        val txDS: DataStream[TxEvent] = logDS2.map(
            log => {
                val datas: Array[String] = log.split(",")
                TxEvent(
                    datas(0),
                    datas(1),
                    datas(2).toLong
                )
            }
        )
        val txKS: KeyedStream[TxEvent, String] = txDS.assignAscendingTimestamps(_.eventTime * 1000L).keyBy(_.txId)

        // TODO 将两个流连接在一起 Join
        // 一定是两个keyBy之后的流根据相同的key进行Join操作
        orderKS.intervalJoin(txKS)
                .between(Time.minutes(-5), Time.minutes(5))
                .process(
                    new ProcessJoinFunction[OrderEvent, TxEvent, (OrderEvent, TxEvent)] {
                        override def processElement(left: OrderEvent,
                                                    right: TxEvent,
                                                    ctx: ProcessJoinFunction[OrderEvent, TxEvent, (OrderEvent, TxEvent)]#Context,
                                                    out: Collector[(OrderEvent, TxEvent)]): Unit = {
                            out.collect((left, right))
                        }
                    }
                ).print("join>>>")

        env.execute()

    }

    case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)

    case class TxEvent(txId: String, payChannel: String, eventTime: Long)

}

```

