# 第8章 Flink SQL编程

如果用户需要同时流计算、批处理的场景下，用户需要维护两套业务代码，开发人员也要维护两套技术栈，非常不方便。 

Flink 社区很早就设想过将批数据看作一个有界流数据，将批处理看作流计算的一个特例，从而实现流批统一，Flink 社区的开发人员在多轮讨论后，基本敲定了Flink 未来的技术架构

 

![20190920114258518](https://img.lormer.cn/clip_image002.png)

从Flink 1.9开始，Blink就逐渐融合到了Flink当中，但是中间还是有很大的差别，所以Flink同时支持旧版本和Blink版本，使用时，需要注意引入不同的依赖

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-api-scala-bridge_2.11</artifactId>
  <version>1.10.0</version>
</dependency>
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-planner_2.11</artifactId>
  <version>1.10.0</version>
</dependency>
<!-- blink 
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-planner-blink_2.11</artifactId>
  <version>1.10.0</version>
</dependency>
-->
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-common</artifactId>
  <version>1.10.0</version>
</dependency>
<dependency>
    <groupId>de.unkrig.jdisasm</groupId>
    <artifactId>jdisasm</artifactId>
    <version>1.0.5</version>
</dependency>
```

生产中的数据源源不断地进入到Flink系统中，我们如何将这些数据作为二维表格来进行SQL统计呢，Flink如何进行操作的呢？

![stream-query-stream](https://img.lormer.cn/clip_image002-1591401256516.png)

## 8.1 Table API

### 8.1.1 创建环境

```scala
// 设置 
// 可以useOldPlanner 也有useBlinkPlanner 也有useAllPlanner
val settings =
    EnvironmentSettings
        .newInstance()
        .useOldPlanner()
        .inStreamingMode()
        .build()
// 上下文环境
val env =
    StreamExecutionEnvironment
        .getExecutionEnvironment
// 表环境
val tableEnv =
    StreamTableEnvironment
        .create(env, settings)
```

### 8.1.2 创建表

```scala
case class WaterSensor( id:String, ts:Long, height:Int )
...

val dataDS: DataStream[String] = env.readTextFile("input/sensor.txt")

val sensorDS: DataStream[WaterSensor] = dataDS.map(
    data => {
        val datas = data.split(",")
        WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
    }
)
val table: Table = tableEnv.fromDataStream(sensorDS)
```

### 8.1.3 查询表

```scala
table
    .filter('id === "sensor_1")
    .groupBy('id)
    .select('height.sum as 'sumHeight)
    .toRetractStream[Int]
    .print("table>>>")
```

### 8.1.4 保存数据

```scala
val schema = new Schema()
    .field("a", DataTypes.STRING())
    .field("b", DataTypes.BIGINT())
    .field("c", DataTypes.INT())
tableEnv.connect(new FileSystem().path("output/sensor.csv"))
    .withFormat(new OldCsv().fieldDelimiter("|"))
    .withSchema(schema)
    .createTemporaryTable("CsvTable")
table.insertInto("CsvTable")
```

---

```scala
// --- 课堂代码 ---
package com.atguigu.bigdata.flink.chapter08

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._

object Flink01_SQL1 {
    def main(args: Array[String]): Unit = {

        // 环境设置
        // 环境设定中可以指明执行的计划版本
        // Flink官方推荐使用1.9之前的版本
        val setting = EnvironmentSettings
                .newInstance()
                //.useAnyPlanner()
                //.useBlinkPlanner()
                .useOldPlanner() 
                //.inBatchMode() // 批处理模式
                .inStreamingMode() // 流处理模式
                .build()

        // 上下文环境
        val env =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 表环境
        val tableEnv: StreamTableEnvironment = StreamTableEnvironment
                .create(env, setting)

        // 准备数据流
        val dataDS: DataStream[String] =
            env.readTextFile("input/sensor-data1.log")

        // TODO 使用Table & SQL API时，需要引入相关的隐式转换
        // import org.apache.flink.table.api._
        // import org.apache.flink.table.api.scala._
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

        // TODO 表, 将流数据转换为动态表
        // stream => table
        val sensorTable: Table = tableEnv.fromDataStream(sensorDS)

        // 操作表
        // table => operator => table
        // 对flink表中的列做操作的时候，列名需要使用单引号开头
        // 判断相等使用三个等号

        // 对表的操作分为两种方式
        // 1. 追加数据 toAppendStream
        // 2. 修改数据 toRetractStream
        val table =
        sensorTable
                .filter('id === "sensor_1")
                .groupBy('id)
                .select('height.sum as 'sumHeight)

        // table => stream
        val ds = table
        // 如果对表做修改而不是简单的追加 需要用toRetractStream
        // 上面对表的操作如果仅仅是filter 可以用AppendStream
        // 如果加上了groupBy select等操作仍继续使用AppendStream 会报错Table is not an append-only table.Use the toRetractStream() in order...
                //.toAppendStream[WaterSensor]
                .toRetractStream[Int]

        ds.print("table>>>")

        env.execute()
    }

    case class WaterSensor(id: String, ts: Long, height: Int)

}
// 下图1为数据 图2为结果
// 第1条数据(第1行)来的时候 表格增加 为true
// 第2条数据(第3行)来的时候 表格求和为4 把1这条数据删除false 4标记为true
// 第3条数据(第5行)来的时候 表格求和为9 把4这条数据删除false 9标记为true
```

![](https://img.lormer.cn/20200606092630.png)

![](https://img.lormer.cn/20200606092558.png)



```scala
// ---- 课堂代码 将查询到的表数据转存到文件中 ---
package com.atguigu.bigdata.flink.chapter08

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{FileSystem, OldCsv, Schema}

object Flink02_SQL2 {
    def main(args: Array[String]): Unit = {

        // 环境设置
        // 环境设定中可以指明执行的计划版本
        // Flink官方推荐使用1.9之前的版本
        val setting: EnvironmentSettings = EnvironmentSettings
                        .newInstance()
                        //.useAnyPlanner()
                        //.useBlinkPlanner()
                        .useOldPlanner()
                        //.inBatchMode()
                        .inStreamingMode()
                        .build()

        // 上下文环境
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 表环境
        val tableEnv: StreamTableEnvironment = StreamTableEnvironment
                .create(env, setting)

        // 准备数据流
        val dataDS: DataStream[String] =
            env.readTextFile("input/sensor-data1.log")

        // TODO 使用Table & SQL API时，需要引入相关的隐式转换
        // import org.apache.flink.table.api._
        // import org.apache.flink.table.api.scala._
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

        // TODO 表, 将流数据转换为动态表
        // stream => table
        val sensorTable: Table = tableEnv.fromDataStream(sensorDS)

        // 表结构
        val schema = new Schema()
        schema.field("id", DataTypes.STRING())
        schema.field("ts", DataTypes.BIGINT())
        schema.field("height", DataTypes.INT())

        // 创建临时表
        tableEnv
            .connect(
                new FileSystem().path("output/sensor.csv")
            )
            .withFormat( new OldCsv().fieldDelimiter("|") )
            .withSchema( schema )
            .createTemporaryTable("SensorCSVTable")

        // 将数据插入到指定表中
        sensorTable.insertInto("SensorCSVTable")

        env.execute()
    }
    case class WaterSensor( id:String, ts:Long, height:Int )
}
```

![image-20200606095440475](https://img.lormer.cn/image-20200606095440475.png)

## 8.2 SQL API

### 8.2.1 创建环境

```scala
val settings =
    EnvironmentSettings
        .newInstance()
        .useOldPlanner()
        .inStreamingMode()
        .build()
val env =
    StreamExecutionEnvironment
        .getExecutionEnvironment
val tableEnv =
    StreamTableEnvironment
        .create(env, settings)
```

### 8.2.2 创建临时表

```scala
case class WaterSensor( id:String, ts:Long, height:Int )
...

val dataDS: DataStream[String] = env.readTextFile("input/sensor.txt")

val sensorDS: DataStream[WaterSensor] = dataDS.map(
    data => {
        val datas = data.split(",")
        WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
    }
)
val table: Table = tableEnv.fromDataStream(sensorDS)
tableEnv.createTemporaryView("sensor", table)
```

### 8.2.3 查询表

```scala
val stream: DataStream[(String, Long, Int)] =
    tableEnv
            .sqlQuery("select * from sensor")
			// 只是做查询 没有对数据进行修改
            .toAppendStream[(String, Long, Int )]
stream.print("sql>>>")
```

```scala
// --- 课堂代码 使用SQL方法查询数据
package com.atguigu.bigdata.flink.chapter08

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._

object Flink03_SQL3 {
    def main(args: Array[String]): Unit = {

        // 环境设置
        // 环境设定中可以指明执行的计划版本
        // Flink官方推荐使用1.9之前的版本
        val setting: EnvironmentSettings = EnvironmentSettings
                .newInstance()
                //.useAnyPlanner()
                //.useBlinkPlanner()
                .useOldPlanner()
                //.inBatchMode()
                .inStreamingMode()
                .build()

        // 上下文环境
        val env: StreamExecutionEnvironment =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 表环境
        val tableEnv: StreamTableEnvironment = StreamTableEnvironment
                .create(env, setting)

        // 准备数据流
        val dataDS: DataStream[String] =
            env.readTextFile("input/sensor-data1.log")

        // TODO 使用Table & SQL API时，需要引入相关的隐式转换
        // import org.apache.flink.table.api._
        // import org.apache.flink.table.api.scala._
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

        // TODO 表, 将流数据转换为动态表
        // stream => table
        val sensorTable: Table = tableEnv.fromDataStream(sensorDS)
        // 创建临时视图
        tableEnv.createTemporaryView("sensor", sensorTable)

        // TODO 使用SQL API 查询数据
        // 查询表的时候，可以直接使用动态表对象
        val resultDS: DataStream[WaterSensor] =
        tableEnv
                .sqlQuery(
                    // 下一行就是使用了动态表对象的方式查询
                    //"select * from " + sensorTable
                    "select * from sensor"
                ).toAppendStream[WaterSensor]

        resultDS.print("sql>>>")

        env.execute()
    }

    case class WaterSensor(id: String, ts: Long, height: Int)

}
```



### 8.2.4 保存数据

```scala
val dataDS1: DataStream[String] = env.readTextFile("input/sensor.txt")

val sensorDS1: DataStream[WaterSensor] = dataDS1.map(
    data => {
        val datas = data.split(",")
        WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
    }
)
val fsSettings =
    EnvironmentSettings
            .newInstance()
            .useOldPlanner()
            .inStreamingMode()
            .build()
val tableEnv = StreamTableEnvironment.create(env, fsSettings)
val table1: Table = tableEnv.fromDataStream(sensorDS1)
tableEnv.createTemporaryView("sensor1", table1)
val schema = new Schema()
        .field("a", DataTypes.STRING())
        .field("b", DataTypes.BIGINT())
        .field("c", DataTypes.INT())
tableEnv.connect(new FileSystem().path("output/sensor.csv"))
        .withFormat(new OldCsv())//.fieldDelimiter("|"))
        .withSchema(schema)
        .createTemporaryTable("sensor2")

tableEnv.sqlUpdate("insert into sensor2 select * from sensor1 ")
```

```scala
// --- 使用SQL的方式 进行插入数据 ---
package com.atguigu.bigdata.flink.chapter08

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{FileSystem, OldCsv, Schema}

object Flink04_SQL4_InsertSQL {
    def main(args: Array[String]): Unit = {

        // 环境设置
        // 环境设定中可以指明执行的计划版本
        // Flink官方推荐使用1.9之前的版本
        val setting = EnvironmentSettings
                .newInstance()
                //.useAnyPlanner()
                //.useBlinkPlanner()
                .useOldPlanner()
                //.inBatchMode()
                .inStreamingMode()
                .build()

        // 上下文环境
        val env =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 表环境
        val tableEnv: StreamTableEnvironment = StreamTableEnvironment
                .create(env, setting)

        // 准备数据流
        val dataDS: DataStream[String] =
            env.readTextFile("input/sensor-data1.log")

        // TODO 使用Table & SQL API时，需要引入相关的隐式转换
        // import org.apache.flink.table.api._
        // import org.apache.flink.table.api.scala._
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

        // TODO 表, 将流数据转换为动态表
        // stream => table
        val sensorTable: Table = tableEnv.fromDataStream(sensorDS)
        tableEnv.createTemporaryView("sensorData", sensorTable)

        val schema = new Schema()
        schema.field("id", DataTypes.STRING())
        schema.field("ts", DataTypes.BIGINT())
        schema.field("height", DataTypes.INT())

        // 创建临时表
        tableEnv
                .connect(
                    new FileSystem().path("output/sensor.csv")
                )
                .withFormat(new OldCsv())
                .withSchema(schema)
                .createTemporaryTable("SensorCSVTable")

        tableEnv.sqlUpdate(
            "insert into SensorCSVTable select * from sensorData"
        )

        env.execute()
    }

    case class WaterSensor(id: String, ts: Long, height: Int)

}
```

## 8.3 API

### 8.3.1 创建表

```scala
val table1: Table = tableEnv.from("sensor")
// 从流数据中创建表
val table2: Table = tableEnv.fromDataStream(stream)
// 从流数据中创建表 并且指定字段
val table3: Table = tableEnv.fromDataStream(stream, 'id, 'ts)
val table4: Table = tableEnv.createTemporaryTable("sensor")
```

```scala
package com.atguigu.bigdata.flink.chapter08

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object Flink05_SQL_Query {
    def main(args: Array[String]): Unit = {

        // 环境设置
        // 环境设定中可以指明执行的计划版本
        // Flink官方推荐使用1.9之前的版本
        val setting = EnvironmentSettings
                .newInstance()
                //.useAnyPlanner()
                //.useBlinkPlanner()
                .useOldPlanner()
                //.inBatchMode()
                .inStreamingMode()
                .build()

        // 上下文环境
        val env =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 表环境
        val tableEnv: StreamTableEnvironment = StreamTableEnvironment
                .create(env, setting)

        // 准备数据流
        val dataDS: DataStream[String] =
            env.readTextFile("input/sensor-data1.log")

        // TODO 使用Table & SQL API时，需要引入相关的隐式转换
        // import org.apache.flink.table.api._
        // import org.apache.flink.table.api.scala._
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

        // TODO 表, 将流数据转换为动态表
        // 将流转换为table时，可以动态指定列。
        val sensorTable: Table = tableEnv.fromDataStream(sensorDS, 'id, 'ts)
        tableEnv.createTemporaryView("sensorData", sensorTable)
        tableEnv
                // SQL API
                .sqlQuery(
                    "select id, ts from sensorData"
                )
                // 可以把上面得到的结果再进行查询  Table API
                .select('id)
                //.toAppendStream[(String, Long)]
                // 当我们不确定类型的时候 可以使用Row代表一行的类型
                .toAppendStream[Row]
                .print("sql>>>")

        env.execute()
    }

    case class WaterSensor(id: String, ts: Long, height: Int)

}
```

```scala
// --- 条件查询 ---
package com.atguigu.bigdata.flink.chapter08

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object Flink06_SQL_Condition {
    def main(args: Array[String]): Unit = {

        // 环境设置
        // 环境设定中可以指明执行的计划版本
        // Flink官方推荐使用1.9之前的版本
        val setting = EnvironmentSettings
                .newInstance()
                //.useAnyPlanner()
                //.useBlinkPlanner()
                .useOldPlanner()
                //.inBatchMode()
                .inStreamingMode()
                .build()

        // 上下文环境
        val env =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 表环境
        val tableEnv: StreamTableEnvironment = StreamTableEnvironment
                .create(env, setting)

        // 准备数据流
        val dataDS: DataStream[String] =
            env.readTextFile("input/sensor-data1.log")

        // TODO 使用Table & SQL API时，需要引入相关的隐式转换
        // import org.apache.flink.table.api._
        // import org.apache.flink.table.api.scala._
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

        // TODO 表, 将流数据转换为动态表
        // 将流转换为table时，可以动态指定列。
        val sensorTable: Table = tableEnv.fromDataStream(sensorDS)
        tableEnv.createTemporaryView("sensor", sensorTable)

        tableEnv.sqlQuery(
            """
              | select
              |     *
              | from sensor
              | where id = 'sensor_1' and height = 5
            """.stripMargin
        )
                .toAppendStream[Row]
                .print("sql>>>")

        env.execute()
    }

    case class WaterSensor(id: String, ts: Long, height: Int)

}

```

```scala
// SQL方式查询 groupBy 并求sum
package com.atguigu.bigdata.flink.chapter08

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object Flink07_SQL_Sum {
    def main(args: Array[String]): Unit = {

        // 环境设置
        // 环境设定中可以指明执行的计划版本
        // Flink官方推荐使用1.9之前的版本
        val setting = EnvironmentSettings
                .newInstance()
                //.useAnyPlanner()
                //.useBlinkPlanner()
                .useOldPlanner()
                //.inBatchMode()
                .inStreamingMode()
                .build()

        // 上下文环境
        val env =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 表环境
        val tableEnv: StreamTableEnvironment = StreamTableEnvironment
                .create(env, setting)

        // 准备数据流
        val dataDS: DataStream[String] =
            env.readTextFile("input/sensor-data1.log")

        // TODO 使用Table & SQL API时，需要引入相关的隐式转换
        // import org.apache.flink.table.api._
        // import org.apache.flink.table.api.scala._
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

        // TODO 表, 将流数据转换为动态表
        // 将流转换为table时，可以动态指定列。
        val sensorTable: Table = tableEnv.fromDataStream(sensorDS)
        tableEnv.createTemporaryView("sensor", sensorTable)

        tableEnv.sqlQuery(
            """
              | select
              |     id,
              |     sum( height ) as sumheight
              | from sensor
              | group by id
            """.stripMargin
        )
                .toRetractStream[Row]
                .print("sql>>>")

        env.execute()
    }

    case class WaterSensor(id: String, ts: Long, height: Int)

}
// 结果如下
```

![](https://img.lormer.cn/20200606102908.png)



```scala
// --- 课堂代码 测试join操作 ---
package com.atguigu.bigdata.flink.chapter08

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object Flink08_SQL_Join {
    def main(args: Array[String]): Unit = {

        // 环境设置
        // 环境设定中可以指明执行的计划版本
        // Flink官方推荐使用1.9之前的版本
        val setting = EnvironmentSettings
                .newInstance()
                //.useAnyPlanner()
                //.useBlinkPlanner()
                .useOldPlanner()
                //.inBatchMode()
                .inStreamingMode()
                .build()

        // 上下文环境
        val env =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 表环境
        val tableEnv: StreamTableEnvironment = StreamTableEnvironment
                .create(env, setting)

        // 准备数据流
        val dataDS1: DataStream[String] =
            env.readTextFile("input/sensor-data1.log")

        val dataDS2: DataStream[String] =
            env.readTextFile("input/sensor-data2.log")

        // TODO 使用Table & SQL API时，需要引入相关的隐式转换
        // import org.apache.flink.table.api._
        // import org.apache.flink.table.api.scala._
        val sensorDS1 = dataDS1.map(
            data => {
                val datas = data.split(",")
                WaterSensor(
                    datas(0),
                    datas(1).toLong,
                    datas(2).toInt
                )
            }
        )

        val sensorDS2 = dataDS2.map(
            data => {
                val datas = data.split(",")
                WaterSensor(
                    datas(0),
                    datas(1).toLong,
                    datas(2).toInt
                )
            }
        )

        // TODO 表, 将流数据转换为动态表
        // 将流转换为table时，可以动态指定列。
        val sensorTable1: Table = tableEnv.fromDataStream(sensorDS1)
        val sensorTable2: Table = tableEnv.fromDataStream(sensorDS2)
        tableEnv.createTemporaryView("sensor1", sensorTable1)
        tableEnv.createTemporaryView("sensor2", sensorTable2)

        tableEnv.sqlQuery(
            """
              | select
              |     *
              | from sensor1 s1
              | join sensor2 s2 on s1.id = s2.id
            """.stripMargin
        )
                .toRetractStream[Row]
                .print("sql>>>")
// --- 课堂代码 测试left join操作 ---
//        tableEnv.sqlQuery(
//            """
//              | select
//              |     *
//              | from sensor1 s1
//              | left join sensor2 s2 on s1.id = s2.id
//            """.stripMargin
//        )
//        .toRetractStream[Row]
//        .print("sql>>>")
// --- 课堂代码 测试union操作 ---  
//        tableEnv.sqlQuery(
//            """
//              | select
//              |     *
//              | from (
//              |     ( select * from sensor1 where id = 'sensor_4' )
//              |   union
//              |     ( select * from sensor2 )
//              | )
//            """.stripMargin
//        )
//        .toRetractStream[Row]
//        .print("sql>>>")
// --- 课堂代码 测试union操作 ---         
//        tableEnv.sqlQuery(
//            """
//              | select
//              |     *
//              | from sensor1
//              | where id in (
//              |     select id from sensor2
//              | )
//            """.stripMargin
//        )
//        .toRetractStream[Row]
//        .print("sql>>>")
        
        env.execute()
    }

    case class WaterSensor(id: String, ts: Long, height: Int)

}
```

### 8.3.2 表转换流

使用flinkSQL处理实时数据当我们把表转化成流的时候，需要使用

#### toAppendStream

```scala
table
    .select('height)
    .toAppendStream[Int]
.print("table>>>")
```

![append-mode](https://img.lormer.cn/clip_image002-1591401407691.png)

#### toRetractStream

在做sum操作的时候 因为累加过的数据是要进行删除 所以 不能用toAppendStream 而要用toRetractStream

```scala
table
    .filter('id === "sensor_1")
    .groupBy('id)
    .select('height.sum as 'sumHeight)
    .toRetractStream[Int]
```

![undo-redo-mode](https://img.lormer.cn/clip_image002-1591401431923.png)

### 8.3.3 数据类型

- 原子类型：Integer, Double, String

- 元组和样例类

- POJO

- Row

### 8.3.4 解释器

```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tEnv = StreamTableEnvironment.create(env)
// 直接把元素变成表
val table1 = env.fromElements((1, "hello")).toTable(tEnv, 'count, 'word)
val table2 = env.fromElements((1, "hello")).toTable(tEnv, 'count, 'word)
val table = table1
        .where('word.like("F%"))
        .unionAll(table2)
val explanation: String = tEnv.explain(table)
println(explanation)
env.execute()
```

![image-20200606113300519](https://img.lormer.cn/image-20200606113300519.png)

![image-20200606113337032](https://img.lormer.cn/image-20200606113337032.png)

### 8.3.5 SQL操作

#### 查询 扫描

```scala
tableEnv.createTemporaryView("sensor", table)
tableEnv.sqlQuery("select * from sensor")
tableEnv
    .sqlQuery("select id, ts as st from sensor")
    .select('st)
```

#### 条件

```scala
tableEnv
    .sqlQuery(
        s"""
           | select
           |    *
           | from sensor
           | where id = 'sensor_1' and height = 3
         """.stripMargin)
    .toAppendStream[WaterSensor]
```

#### 分组

```scala
tableEnv
    .sqlQuery(
        s"""
           | select
           |    id,
           |    sum(height) as sumHeight
           | from sensor
           | group by id
         """.stripMargin)
    .select('id, 'sumHeight)
    .toRetractStream[Row]
```

#### 内连接

```scala
tableEnv
    .sqlQuery(
        s"""
           | select
           |    *
           | from sensor1 s1
           | join sensor2 s2 on s1.id = s2.id
         """.stripMargin)
    .toAppendStream[Row]
```

#### 外连接

```scala
tableEnv
    .sqlQuery(
        s"""
           | select
           |    *
           | from sensor1 s1
           | left join sensor2 s2 on s1.id = s2.id
         """.stripMargin)
    .toRetractStream[Row]
```

#### 合并

```scala
tableEnv
    .sqlQuery(
        s"""
           | SELECT
           |    *
           | FROM (
           |    (SELECT id FROM sensor1 WHERE height > 4)
           | UNION
           |    (SELECT id FROM sensor2 )
           |)
         """.stripMargin)
    .toRetractStream[Row]
```

#### 范围

```scala
tableEnv
    .sqlQuery(
        s"""
           | SELECT
           |    *
           | FROM sensor1
           | WHERE id in (
           |    SELECT id FROM sensor2
           |)
         """.stripMargin)
    .toRetractStream[Row]
```

#### TopN

此功能仅仅在Blink中能使用，接下来我们将热门商品排行需求采用Flink SQL实现一下

**引入依赖关系**

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-planner-blink_2.11</artifactId>
  <version>1.10.0</version>
</dependency>
```

**代码实现**

```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment
env.setParallelism(1)
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

val settings = EnvironmentSettings.newInstance()
        .useBlinkPlanner()
        .inStreamingMode()
        .build()
val tEnv = StreamTableEnvironment.create(env, settings)

val stream = env
    .readTextFile("input/UserBehavior.csv")
    .map(line => {
        val arr = line.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong * 1000)
    })
    .filter(_.behavior == "pv")
    .assignAscendingTimestamps(_.timestamp)

val table = tEnv.fromDataStream(stream, 'timestamp.rowtime, 'itemId)
val t = table
        .window(Tumble over 60.minutes on 'timestamp as 'w)
        .groupBy('itemId, 'w)
        .aggregate('itemId.count as 'icount)
        .select('itemId, 'icount, 'w.end as 'windowEnd)
        .toAppendStream[(Long, Long, Timestamp)]

tEnv.createTemporaryView("topn", t, 'itemId, 'icount, 'windowEnd)

val result = tEnv.sqlQuery(
    """
      |SELECT *
      |FROM (
      |    SELECT *,
      |        ROW_NUMBER() OVER (PARTITION BY windowEnd ORDER BY icount DESC) as row_num
      |    FROM topn)
      |WHERE row_num <= 3
      |""".stripMargin
)
result.toRetractStream[(Long, Long, Timestamp, Long)].print()
```

```scala
// --- 课堂代码实现 ---
// TopN
package com.atguigu.bigdata.flink.chapter08

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._

object Flink12_SQL_TopN {
    def main(args: Array[String]): Unit = {

        // TODO Flink默认实现不了TopN问题, Blink可以实现
        // 1. 增加BLink依赖关系
        // 2. 采用useBlinkPlanner
        // 3. SQL

        // 环境设置
        // 环境设定中可以指明执行的计划版本
        // Flink官方推荐使用1.9之前的版本
        val setting = EnvironmentSettings
                .newInstance()
                //.useAnyPlanner()
                .useBlinkPlanner()
                //.useOldPlanner()
                //.inBatchMode()
                .inStreamingMode()
                .build()

        // 上下文环境
        val env =
            StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // 表环境
        val tableEnv: StreamTableEnvironment = StreamTableEnvironment
                .create(env, setting)

        val dataDS: DataStream[String] =
            env.readTextFile("input/UserBehavior.csv")

        val sensorDS = dataDS.map(
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

        val sensorFilterDS: DataStream[UserBehavior] = sensorDS.filter(_.behavior == "pv")

        val sensorTimeDS = sensorFilterDS.assignAscendingTimestamps(_.timestamp * 1000L)

        // 将数据流转换为表
        // 这里第二个参数 规定了时间戳 使用处理时间proctime还是rowtime
        val table = tableEnv.fromDataStream(sensorTimeDS, 'timestamp.rowtime, 'itemId)

        // 在表数据中 进行开窗口 分组 添加end时间信息 并重新查询进行封装 做成一个新的临时视图
        val result: DataStream[(Long, Long, _root_.java.sql.Timestamp)] = table
                .window(
                    Tumble over 60.minutes on 'timestamp as 'w
                ).groupBy('itemId, 'w)
                .aggregate('itemId.count as 'itemCount)
                .select('itemId, 'itemCount, 'w.end as 'windowEndTime)
                .toAppendStream[(Long, Long, _root_.java.sql.Timestamp)]
        tableEnv.createTemporaryView("topnView", result, 'itemId, 'itemCount, 'windowEndTime)

        // 然后用SQL语法进行开窗查询 类似于之前的实现方式 不过现在的操作放在了Flink SQL中
        tableEnv.sqlQuery(
            """
              | select
              |    *
              | from (
              |    select
              |        *,
              |        ROW_NUMBER() OVER ( PARTITION BY windowEndTime ORDER BY itemCount DESC ) as rownum
              |    from topnView
              | )
              | where rownum <= 3
            """.stripMargin
        ).toRetractStream[(Long, Long, _root_.java.sql.Timestamp, Long)]
                .print("topn>>>")

        env.execute()
    }

    case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

}
```

