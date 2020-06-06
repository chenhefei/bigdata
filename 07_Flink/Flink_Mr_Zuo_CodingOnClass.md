### 0527 115512

```scala
package com.atguigu

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time // 导入所有的隐式类型转换

object WordCount {

  case class WordWithCount(word: String,
                           count: Int)

  def main(args: Array[String]): Unit = {
    // 获取运行时环境
    // 相当于sparkContext
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并行度为1
    env.setParallelism(1)

    // 定义DAG
    // 数据流的来源（socket）=> 处理逻辑 => 数据流计算结果的去向，sink（print，打印到屏幕）
    val text = env
//      .fromElements(
//        "hello world",
//        "hello atguigu"
//      )
      .socketTextStream("localhost", 9999, '\n')

    val windowCount = text
      // 使用空格对字符串进行切分
      .flatMap(w => w.split("\\s"))
      // map操作
      .map(w => WordWithCount(w, 1))
      // 分组，shuffle操作
      .keyBy("word")
      // 开滚动窗口
      .timeWindow(Time.seconds(5))
      // reduce操作
      .sum("count")
    // 定义DAG结束

    // output操作，输出到标准输出，stdout
    windowCount
      .print()

    // 执行DAG
    env.execute()

  }
}
```

### 0527 115601

```scala
使用Intellij IDEA创建一个Maven新项目
勾选Create from archetype，然后点击Add Archetype按钮
GroupId中输入org.apache.flink，ArtifactId中输入flink-quickstart-scala，Version中输入1.10.0，然后点击OK
点击向右箭头，出现下拉列表，选中flink-quickstart-scala:1.10.0，点击Next
Name中输入FlinkTutorial，GroupId中输入com.atguigu，ArtifactId中输入FlinkTutorial，点击Next
最好使用IDEA默认的Maven工具：Bundled（Maven 3），点击Finish，等待一会儿，项目就创建好了
```

### 0527 142301

```scala
package com.atguigu

import org.apache.flink.streaming.api.scala._

object StreamingJob {
  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.fromElements(
      "hello world",
      "hello atguigu"
    )

    stream.print() //  stdout

    env.execute("Flink Streaming Scala API Skeleton")
  }
}
```


### 0529 090803

```scala
package com.atguigu.datastreamapi

case class SensorReading(id: String,
                         timestamp: Long,
                         temperature: Double)
```

### 0529 090814

```scala
package com.atguigu.datastreamapi

import java.util.Calendar

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.util.Random

class SensorSource extends RichParallelSourceFunction[SensorReading] {

  var running = true

  override def run(sourceContext: SourceContext[SensorReading]): Unit = {
    val rand = new Random()

    var curFTemp = (1 to 10).map(
      i => ("sensor_" + i, 65 + (rand.nextGaussian() * 20))
    )

    while (running) {

      curFTemp = curFTemp.map(
        t => (t._1, t._2 + (rand.nextGaussian() * 0.5))
      )

      val curTime = Calendar.getInstance.getTimeInMillis

      curFTemp.foreach(t => sourceContext.collect(
        SensorReading(t._1, curTime, t._2)
      ))

      Thread.sleep(100)
    }
  }

  override def cancel(): Unit = running = false
}
```

### 0529 090827

```scala
package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.scala._

object SourceExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

//    val stream = env
//      .fromElements(
//        SensorReading("sensor_1", 1547718199, 35.80018327300259),
//        SensorReading("sensor_6", 1547718199, 15.402984393403084),
//        SensorReading("sensor_7", 1547718199, 6.720945201171228),
//        SensorReading("sensor_10", 1547718199, 38.101067604893444)
//      )

    val stream = env
        .addSource(
          new SensorSource
        )

    stream.print()

    env.execute()
  }
}
```

### 0529 093311

```scala
package com.atguigu.datastreamapi

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._

object MapExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env
      .addSource(new SensorSource)
//      .map(new MyMapFunction)
        .map(
          new MapFunction[SensorReading, String] {
            override def map(value: SensorReading): String = value.id
          }
        )

    stream.print()

    env.execute()
  }

  class MyMapFunction extends MapFunction[SensorReading, String] {
    override def map(value: SensorReading): String = value.id
  }
}
```

### 0529 093714

```scala
package com.atguigu.datastreamapi

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

object FilterExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env
      .addSource(new SensorSource)
//      .filter(r => r.id == "sensor_1")
        .filter(new MyFilterFunction)

    stream.print()

    env.execute()
  }

  class MyFilterFunction extends FilterFunction[SensorReading] {
    override def filter(value: SensorReading): Boolean = value.id == "sensor_1"
  }

}
```

### 0529 094539

```scala
package com.atguigu.datastreamapi

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object FlatMapExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env
//      .addSource(new SensorSource)
        .fromElements(
          1L, 2L, 3L
        )
        .flatMap(new MyFlatMapFunction)

    stream.print()

    env.execute()
  }

  class MyFlatMapFunction extends FlatMapFunction[Long, Long] {
    override def flatMap(value: Long, out: Collector[Long]): Unit = {
      if (value == 2) {
        out.collect(value)
      } else if (value == 3) {
        out.collect(value)
        out.collect(value)
      }
    }
  }
}
```

### 0529 102012

```scala
package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.scala._

object KeybyExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream : DataStream[SensorReading] = env
      .addSource(new SensorSource)

    val keyed : KeyedStream[SensorReading, String] = stream.keyBy(_.id)

    val min : DataStream[SensorReading] = keyed.min("temperature")

    min.print()

    env.execute()
  }
}
```

### 0529 102711

```scala
package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.scala._

object RollingAggregateExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream: DataStream[(Int, Int, Int)] = env.fromElements(
      (1, 2, 2), (2, 3, 1), (2, 2, 4), (1, 5, 3))

    val resultStream: DataStream[(Int, Int, Int)] = inputStream
      .keyBy(0) // key on first field of the tuple
      .sum(1)   // sum the second field of the tuple in place

    resultStream.print()

    env.execute()
  }
}
```

### 0529 103257

```scala
package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.scala._

object ReduceExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env
      .addSource(new SensorSource)
    
    val keyed = stream.keyBy(r => r.id)

    val min = keyed
      .reduce((x, y) => SensorReading(x.id, x.timestamp, x.temperature.min(y.temperature)))

    min.print()

    env.execute()
  }
}
```

### 0529 103524

```scala
package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.scala._

object ReduceExample1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputStream: DataStream[(String, List[String])] = env.fromElements(
      ("en", List("tea")), ("fr", List("vin")), ("en", List("cake")))

    val resultStream: DataStream[(String, List[String])] = inputStream
      .keyBy(0)
      .reduce((x, y) => (x._1, x._2 ::: y._2))

    resultStream.print()

    env.execute()
  }
}
```

### 0529 111007

```scala
package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.scala._

object UnionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val parisStream: DataStream[SensorReading] = env
      .addSource(new SensorSource)
      .filter(r => r.id == "sensor_1")
    val tokyoStream: DataStream[SensorReading] = env
      .addSource(new SensorSource)
      .filter(r => r.id == "sensor_2")
    val rioStream: DataStream[SensorReading] = env
      .addSource(new SensorSource)
      .filter(r => r.id == "sensor_3")
    val allCities: DataStream[SensorReading] = parisStream
      .union(tokyoStream, rioStream)

    allCities.print()

    env.execute()
  }
}
```

### 0529 112857

```scala
package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala._

object ConnectExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val one: DataStream[(Int, Long)] = env
      .fromElements(
        (1, 1L),
        (2, 2L),
        (3, 3L)
      )
    val two: DataStream[(Int, String)] = env
      .fromElements(
        (1, "1"),
        (2, "2"),
        (3, "3")
      )

    val connected : ConnectedStreams[(Int, Long), (Int, String)] = one
      .keyBy(_._1)
      .connect(
        two
          .keyBy(_._1)
      )

    val comap = connected
      .map(new MyCoMap)

    comap.print()

    env.execute()
  }

  class MyCoMap extends CoMapFunction[(Int, Long),
    (Int, String), String] {
    override def map1(in1: (Int, Long)): String = {
      "key为 " + in1._1 + " 的数据来自第一条流"
    }

    override def map2(in2: (Int, String)): String = {
      "key为 " + in2._1 + " 的数据来自第二条流"
    }
  }
}
```

### 0529 113338

```scala
package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object CoFlatMapExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream1 = env
      .fromElements(
        (1, "first stream"),
        (2, "first stream")
      )

    val stream2 = env
      .fromElements(
        (1, "second stream"),
        (2, "second stream")
      )
    
    stream1
      .keyBy(_._1)
      .connect(stream2.keyBy(_._1))
      .flatMap(new MyFlatMap)
      .print()

    env.execute()
  }

  class MyFlatMap extends CoFlatMapFunction[(Int, String), (Int, String), String] {
    override def flatMap1(in1: (Int, String), collector: Collector[String]): Unit = {
      collector.collect(in1._2)
    }
    override def flatMap2(in2: (Int, String), collector: Collector[String]): Unit = {
      collector.collect(in2._2)
      collector.collect(in2._2)
    }
  }
}
```

### 0529 114100

```scala
package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.scala._

object SplitExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream: DataStream[(Int, String)] = env
      .fromElements(
        (1001, "1001"),
        (999, "999")
      )

    val splitted: SplitStream[(Int, String)] = inputStream
      .split(t => if (t._1 > 1000) Seq("large") else Seq("small"))

    val large: DataStream[(Int, String)] = splitted.select("large")
    val small: DataStream[(Int, String)] = splitted.select("small")
    val all: DataStream[(Int, String)] = splitted.select("small", "large")

    large.print()

    env.execute()
  }
}
```

### 0529 150946

```scala
package com.atguigu.datastreamapi

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object RichExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .fromElements(1,2,3)

    stream
      .map(
        new RichMapFunction[Int, Int] {
          override def open(parameters: Configuration): Unit = {
            println("开始map的生命周期")
          }

          override def map(value: Int): Int = value + 1

          override def close(): Unit = {
            println("结束map的生命周期")
          }
        }
      )
      .print()

    env.execute()
  }
}
```

### 0529 155423

```scala
package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WindowMinTemp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    
    val readings = env
      .addSource(new SensorSource)

    readings
      .map(r => (r.id, r.temperature))
      .keyBy(_._1)
//      .timeWindow(Time.seconds(5))
      .timeWindow(Time.seconds(10), Time.seconds(5))
      .reduce((x1, x2) => (x1._1, x1._2.min(x2._2)))
      .print()

    env.execute()
  }
}
```

### 0529 164244

```scala
package com.atguigu.datastreamapi

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object AggregateExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val readings = env
      .addSource(new SensorSource)
      .map(r => (r.id, r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .aggregate(new AvgTemp)

    readings.print()
    env.execute()
  }

  class AvgTemp extends AggregateFunction[(String, Double),
    (String, Double, Long), (String, Double)] {
    override def createAccumulator(): (String, Double, Long) = {
      ("", 0.0, 0L)
    }
    override def add(value: (String, Double), accumulator: (String, Double, Long)): (String, Double, Long) = {
      (value._1, accumulator._2 + value._2, accumulator._3 + 1)
    }
    override def getResult(accumulator: (String, Double, Long)): (String, Double) = {
      (accumulator._1, accumulator._2 / accumulator._3)
    }
    override def merge(a: (String, Double, Long), b: (String, Double, Long)): (String, Double, Long) = {
      (a._1, a._2 + b._2, a._3 + b._3)
    }
  }
}
```

### 0529 165407

```scala
package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object ProcessWindowFunctionAvgTemp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val readings = env
      .addSource(new SensorSource)
      .map(r => (r.id, r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .process(new MyAvgTemp)

    readings.print()
    env.execute()
  }

  class MyAvgTemp extends ProcessWindowFunction[
    (String, Double), (String, Double, Long), String, TimeWindow] {
    override def process(key: String,
                         context: Context,
                         elements: Iterable[(String, Double)],
                         out: Collector[(String, Double, Long)]): Unit = {
      var sum = 0.0
      val size = elements.size
      for (i <- elements) {
        sum += i._2
      }
      out.collect((key, sum / size, context.window.getEnd))
    }
  }
}
```

### 0530 090022

```scala
package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object HighLowTemp {

  case class MinMaxTemp(id: String,
                        min: Double,
                        max: Double,
                        endTs: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val readings = env.addSource(new SensorSource)

    readings
      .keyBy(r => r.id)
      .timeWindow(Time.seconds(5))
      .process(new HighLow)
      .print()

    env.execute()
  }

  class HighLow extends ProcessWindowFunction[SensorReading,
    MinMaxTemp, String, TimeWindow] {
    override def process(key: String,
                         context: Context,
                         elements: Iterable[SensorReading],
                         out: Collector[MinMaxTemp]): Unit = {
      val temps = elements.map(r => r.temperature)
      val endTs = context.window.getEnd
      out.collect(MinMaxTemp(key, temps.min, temps.max, endTs))
    }
  }
}
```

### 0530 091604

```scala
package com.atguigu.datastreamapi

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object HighLowTempOptimize {

  case class MinMaxTemp(id: String,
                        min: Double,
                        max: Double,
                        endTs: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val readings = env.addSource(new SensorSource)
    readings
      .keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .aggregate(new AggTemp, new WindowTemp)
      .print()

    env.execute()
  }

  class WindowTemp extends ProcessWindowFunction[(String, Double, Double),
    MinMaxTemp, String, TimeWindow] {
    override def process(key: String,
                         context: Context,
                         elements: Iterable[(String, Double, Double)],
                         out: Collector[MinMaxTemp]): Unit = {
      val agg = elements.head
      out.collect(MinMaxTemp(key, agg._2, agg._3, context.window.getEnd))
    }
  }

  class AggTemp extends AggregateFunction[SensorReading,
    (String, Double, Double), (String, Double, Double)] {
    override def createAccumulator(): (String, Double, Double) = {
      ("", Double.MaxValue, Double.MinValue)
    }
    override def add(value: SensorReading, accumulator: (String, Double, Double)): (String, Double, Double) = {
      (value.id, accumulator._2.min(value.temperature), accumulator._3.max(value.temperature))
    }
    override def getResult(accumulator: (String, Double, Double)): (String, Double, Double) = {
      accumulator
    }
    override def merge(a: (String, Double, Double), b: (String, Double, Double)): (String, Double, Double) = {
      (a._1, a._2.min(b._2), a._3.max(b._3))
    }
  }
}
```

### 0530 094608

```scala
package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object HighLowTempOptByReduce {
  case class MinMaxTemp(id: String,
                        min: Double,
                        max: Double,
                        endTs: Long)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val readings = env.addSource(new SensorSource)
    readings
      .map(r => (r.id, r.temperature, r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .reduce(
        (r1, r2) => {
          (r1._1, r1._2.min(r2._2), r1._3.max(r2._3))
        },
        new WindowTemp
      )
      .print()

    env.execute()
  }

  class WindowTemp extends ProcessWindowFunction[(String, Double, Double),
    MinMaxTemp, String, TimeWindow] {
    override def process(key: String,
                         context: Context,
                         elements: Iterable[(String, Double, Double)],
                         out: Collector[HighLowTempOptByReduce.MinMaxTemp]): Unit = {
      val agg = elements.head
      out.collect(MinMaxTemp(key, agg._2, agg._3, context.window.getEnd))
    }
  }
}
```

### 0530 095410

```scala
package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object HighLowTempOptByReduce {
  case class MinMaxTemp(id: String,
                        min: Double,
                        max: Double,
                        endTs: Long)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val readings = env.addSource(new SensorSource)
    readings
      .map(r => (r.id, r.temperature, r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .reduce(
        (r1: (String, Double, Double), r2: (String, Double, Double)) => {
          (r1._1, r1._2.min(r2._2), r1._3.max(r2._3))
        },
        new WindowTemp
      )
      .print()

    env.execute()
  }

  class WindowTemp extends ProcessWindowFunction[(String, Double, Double),
    MinMaxTemp, String, TimeWindow] {
    // process函数什么时候调用？
    // 当窗口闭合的时候调用
    override def process(key: String,
                         context: Context,
                         elements: Iterable[(String, Double, Double)],
                         out: Collector[HighLowTempOptByReduce.MinMaxTemp]): Unit = {
      val agg = elements.head
      out.collect(MinMaxTemp(key, agg._2, agg._3, context.window.getEnd))
    }
  }
}
```

### 0530 112821

```scala
package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object EventTimeExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env
      .socketTextStream("localhost", 9999, '\n')
      .map(line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000)
      })
      // 抽取时间戳（单位必须是ms），插入水位线
      // 水位线 = 观察到的最大事件时间 - 最大延迟时间
      .assignTimestampsAndWatermarks(
        // 设置最大延迟时间是5s
        new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(5)) {
          override def extractTimestamp(t: (String, Long)): Long = t._2
        }
      )
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .process(new EvenTimeProcess)

    stream.print()
    env.execute()
  }

  class EvenTimeProcess extends ProcessWindowFunction[(String, Long),
    String, String, TimeWindow] {
    override def process(key: String,
                         context: Context,
                         elements: Iterable[(String, Long)],
                         out: Collector[String]): Unit = {
      out.collect("窗口结束时间为：" + context.window.getEnd + " 窗口共有 " + elements.size + " 条数据")
    }
  }
}
```

### 0530 140627

```scala
package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object PeriodicWatermarkExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    
    val stream = env
      .addSource(new SensorSource)
      .assignTimestampsAndWatermarks(new MyAssigner)
        .keyBy(_.id)
        .timeWindow(Time.seconds(5))
        .process(new MyProcess)
    
    stream.print()
    env.execute()
  }
  
  class MyProcess extends ProcessWindowFunction[SensorReading, String, String, TimeWindow] {
    override def process(key: String,
                         context: Context,
                         elements: Iterable[SensorReading],
                         out: Collector[String]): Unit = {
      out.collect("hello world")
    }
  }
  
  class MyAssigner extends AssignerWithPeriodicWatermarks[SensorReading] {
    val bound: Long = 60 * 1000 // 最大延迟时间
    var maxTs: Long = Long.MinValue + bound // 用来保存观察到的最大事件时间
    
    // 每来一条数据，调用一次
    override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
      maxTs = maxTs.max(element.timestamp) // 更新观察到的最大事件时间
      element.timestamp // 提取事件时间戳
    }

    // 系统插入水位线的时候调用
    override def getCurrentWatermark: Watermark = {
      // 水位线 = 观察到的最大事件时间 - 最大延迟时间
      new Watermark(maxTs - bound)
    }
  }
}
```

### 0530 151045

```scala
package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object DistributedWatermark {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream1 = env
      .socketTextStream("localhost", 9999, '\n')
      .map(line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000)
      })
      .assignAscendingTimestamps(_._2)
      .keyBy(_._1)


    val stream2 = env
      .socketTextStream("localhost", 9998, '\n')
      .map(line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000)
      })
      .assignAscendingTimestamps(_._2)
      .keyBy(_._1)

    stream1.connect(stream2).process(new MyCoProcess).print()
    env.execute()
  }
  class MyCoProcess extends CoProcessFunction[(String, Long), (String, Long), String] {
    override def processElement1(value: (String, Long),
                                 ctx: CoProcessFunction[(String, Long), (String, Long), String]#Context,
                                 out: Collector[String]): Unit = {
      println("from stream1", ctx.timerService().currentWatermark())
      out.collect("from stream1")
    }

    override def processElement2(value: (String, Long),
                                 ctx: CoProcessFunction[(String, Long), (String, Long), String]#Context, out: Collector[String]): Unit = {
      println("from stream2", ctx.timerService().currentWatermark())
      out.collect("from stream2")

    }
  }
}
```

### 0530 160616

```scala
package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object TestOnTimerProcessingTime {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .addSource(new SensorSource)
      .keyBy(_.id)
      .process(new Key)

    stream.print()
    env.execute()
  }
  class Key extends KeyedProcessFunction[String, SensorReading, SensorReading] {
    override def processElement(value: SensorReading,
                                ctx: KeyedProcessFunction[String, SensorReading, SensorReading]#Context,
                                out: Collector[SensorReading]): Unit = {
      out.collect(value)
      ctx.timerService().registerProcessingTimeTimer(value.timestamp + 10 * 1000L)
    }
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, SensorReading]#OnTimerContext, out: Collector[SensorReading]): Unit = {
      println("定时事件执行了！！！")
    }
  }
}
```

### 0530 160642

```scala
package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object TestOnTimer {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env
      .socketTextStream("localhost", 9999, '\n')
      .map(line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000)
      })
      .assignAscendingTimestamps(_._2)
      .keyBy(_._1)
      .process(new Key)

    stream.print()
    env.execute()
  }
  class Key extends KeyedProcessFunction[String, (String, Long), String] {
    override def processElement(value: (String, Long),
                                ctx: KeyedProcessFunction[String, (String, Long), String]#Context,
                                out: Collector[String]): Unit = {
      ctx.timerService().registerEventTimeTimer(value._2 + 10 * 1000L - 1)
    }
    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[String, (String, Long), String]#OnTimerContext,
                         out: Collector[String]): Unit = {
      println("定时事件发生的时间：" + timestamp)
      out.collect("定时事件发生了！！！")
    }
  }
}
```

### 0530 165622

```scala
package com.atguigu.datastreamapi

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object TempIncreaseAlert {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val readings = env
      .addSource(new SensorSource)
      .keyBy(_.id)
      .process(new TempIncreaseAlertFunction)

    readings.print()
    env.execute()
  }

  class TempIncreaseAlertFunction extends KeyedProcessFunction[String, SensorReading, String] {
    lazy val lastTemp = getRuntimeContext.getState(
      new ValueStateDescriptor[Double]("last-temp", Types.of[Double])
    )
    lazy val currentTimer = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("timer", Types.of[Long])
    )
    override def processElement(r: SensorReading,
                                ctx: KeyedProcessFunction[String, SensorReading, String]#Context,
                                out: Collector[String]): Unit = {
      val prevTemp = lastTemp.value()
      lastTemp.update(r.temperature)

      val curTimerTimestamp = currentTimer.value()

      if (prevTemp == 0.0 || r.temperature < prevTemp) {
        ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp)
        currentTimer.clear()
      } else if (r.temperature > prevTemp && curTimerTimestamp == 0L) {
        val timerTs = ctx.timerService().currentProcessingTime() + 1000L
        ctx.timerService().registerProcessingTimeTimer(timerTs)
        currentTimer.update(timerTs)
      }
    }
    override def onTimer(ts: Long,
                         ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext,
                         out: Collector[String]): Unit = {
      out.collect("温度连续1s上升了！" + ctx.getCurrentKey)
    }
  }
}
```

### 0601 090705

```scala
package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutputExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val readings = env
      .addSource(new SensorSource)
      .process(new FreezingMonitor)

    readings
        .getSideOutput(new OutputTag[String]("freezing-alarm"))
        .print()

//    readings.print()

    env.execute()
  }

  // 没有keyby，所以使用ProcessFunction
  class FreezingMonitor extends ProcessFunction[SensorReading, SensorReading] {
    lazy val freezingAlarmOutput = new OutputTag[String]("freezing-alarm")

    override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      if (value.temperature < 32.0) {
        ctx.output(freezingAlarmOutput, s"Freezing Alarm for ${value.id}")
      }
      out.collect(value)
    }
  }
}
```

### 0601 090815

```scala
package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutputExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val readings = env
      .addSource(new SensorSource)
      .keyBy(_.id)
      .process(new FreezingMonitor)

    readings
        .getSideOutput(new OutputTag[String]("freezing-alarm"))
        .print()

//    readings.print()

    env.execute()
  }

  // 没有keyby，所以使用ProcessFunction
  class FreezingMonitor extends KeyedProcessFunction[String, SensorReading, SensorReading] {
    lazy val freezingAlarmOutput = new OutputTag[String]("freezing-alarm")

    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      if (value.temperature < 32.0) {
        ctx.output(freezingAlarmOutput, s"Freezing Alarm for ${value.id}")
      }
      out.collect(value)
    }
  }
}
```

### 0601 090831

```scala
package com.atguigu.datastreamapi

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object TempIncreaseAlert {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val readings = env
      .addSource(new SensorSource)
      .keyBy(_.id)
      .process(new TempIncreaseAlertFunction)

    readings.print()
    env.execute()
  }

  class TempIncreaseAlertFunction extends KeyedProcessFunction[String, SensorReading, String] {
    var lastTemp : ValueState[Double] = _
    var currentTimer : ValueState[Long] = _

    override def open(parameters: Configuration): Unit = {
      lastTemp = getRuntimeContext.getState(
        new ValueStateDescriptor[Double]("last-temp", Types.of[Double])
      )
      currentTimer = getRuntimeContext.getState(
        new ValueStateDescriptor[Long]("timer", Types.of[Long])
      )
    }

    override def processElement(r: SensorReading,
                                ctx: KeyedProcessFunction[String, SensorReading, String]#Context,
                                out: Collector[String]): Unit = {
      val prevTemp = lastTemp.value()
      lastTemp.update(r.temperature)

      val curTimerTimestamp = currentTimer.value()

      if (prevTemp == 0.0 || r.temperature < prevTemp) {
        ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp)
        currentTimer.clear()
      } else if (r.temperature > prevTemp && curTimerTimestamp == 0L) {
        val timerTs = ctx.timerService().currentProcessingTime() + 1000L
        ctx.timerService().registerProcessingTimeTimer(timerTs)
        currentTimer.update(timerTs)
      }
    }
    override def onTimer(ts: Long,
                         ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext,
                         out: Collector[String]): Unit = {
      out.collect("温度连续1s上升了！" + ctx.getCurrentKey)
    }
  }
}
```

### 0601 100658

```scala
package com.atguigu.datastreamapi

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SwitchFilterExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val readings = env.addSource(new SensorSource)

    val switch = env.fromElements(
      ("sensor_2", 10 * 1000L),
      ("sensor_7", 20 * 1000L)
    )

    val forwardReadings = readings
      .keyBy(_.id)
      .connect(switch.keyBy(_._1))
      .process(new ReadingFilter)

    forwardReadings.print()

    env.execute()
  }

  class ReadingFilter extends CoProcessFunction[SensorReading,
    (String, Long), SensorReading] {
    lazy val forwardingEnabled = getRuntimeContext.getState(
      new ValueStateDescriptor[Boolean]("switch", Types.of[Boolean])
    )
    lazy val disableTimer = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("timer", Types.of[Long])
    )

    override def processElement1(value: SensorReading,
                                 ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context,
                                 out: Collector[SensorReading]): Unit = {
      if (forwardingEnabled.value()) {
        out.collect(value)
      }
    }

    override def processElement2(value: (String, Long),
                                 ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context,
                                 out: Collector[SensorReading]): Unit = {
      // 开启读数的转发开发
      forwardingEnabled.update(true)

      val timerTimestamp = ctx.timerService().currentProcessingTime() + value._2
      val curTimerTimestamp = disableTimer.value()
      if (timerTimestamp > curTimerTimestamp) {
        ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp)
        ctx.timerService().registerProcessingTimeTimer(timerTimestamp)
        disableTimer.update(curTimerTimestamp)
      }
    }

    override def onTimer(timestamp: Long,
                         ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#OnTimerContext,
                         out: Collector[SensorReading]): Unit = {
      forwardingEnabled.clear()
      disableTimer.clear()
    }
  }
}
```

### 0601 103227

```scala
package com.atguigu.datastreamapi

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SwitchFilterExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val readings = env.addSource(new SensorSource)

    val switch = env.fromElements(
      ("sensor_2", 10 * 1000L),
      ("sensor_7", 20 * 1000L)
    )

    val forwardReadings = readings
      .keyBy(_.id)
      .connect(switch.keyBy(_._1))
      .process(new ReadingFilter)

    forwardReadings.print()

    env.execute()
  }

  class ReadingFilter extends CoProcessFunction[SensorReading,
    (String, Long), SensorReading] {
    lazy val forwardingEnabled = getRuntimeContext.getState(
      new ValueStateDescriptor[Boolean]("switch", Types.of[Boolean])
    )

    override def processElement1(value: SensorReading,
                                 ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context,
                                 out: Collector[SensorReading]): Unit = {
      if (forwardingEnabled.value()) {
        out.collect(value)
      }
    }

    override def processElement2(value: (String, Long),
                                 ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context,
                                 out: Collector[SensorReading]): Unit = {
      // 开启读数的转发开发
      forwardingEnabled.update(true)

      val timerTimestamp = ctx.timerService().currentProcessingTime() + value._2
      ctx.timerService().registerProcessingTimeTimer(timerTimestamp)
    }

    override def onTimer(timestamp: Long,
                         ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#OnTimerContext,
                         out: Collector[SensorReading]): Unit = {
      forwardingEnabled.clear()
    }
  }
}
```

### 0601 111017

```scala
package com.atguigu.datastreamapi

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object TriggerExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val readings = env
      .addSource(new SensorSource)
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .trigger(new OneSecondIntervalTrigger)
      .process(new MyWindow)

    readings.print()
    env.execute()
  }

  class OneSecondIntervalTrigger extends Trigger[SensorReading, TimeWindow] {
    override def onElement(element: SensorReading,
                           timestamp: Long,
                           window: TimeWindow,
                           ctx: TriggerContext): TriggerResult = {

      // 初始值为false
      val firstSeen = ctx.getPartitionedState(
        new ValueStateDescriptor[Boolean]("first-seen", Types.of[Boolean])
      )

      // 只为第一个元素注册定时器
      // 例如第一个元素的时间戳是：1234ms
      // 那么t = ？
      if (!firstSeen.value()) {
        val t = ctx.getCurrentWatermark + (1000 - (ctx.getCurrentWatermark % 1000))
        ctx.registerEventTimeTimer(t) // 注册一个2000ms的定时器
        ctx.registerEventTimeTimer(window.getEnd) // 注册一个窗口结束时间的定时器
        firstSeen.update(true)
      }
      TriggerResult.CONTINUE
    }

    override def onProcessingTime(time: Long,
                                  window: TimeWindow,
                                  ctx: TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def onEventTime(timestamp: Long,
                             window: TimeWindow,
                             ctx: TriggerContext): TriggerResult = {
      if (timestamp == window.getEnd) {
        TriggerResult.FIRE_AND_PURGE
      } else {
        val t = ctx.getCurrentWatermark + (1000 - (ctx.getCurrentWatermark % 1000))
        if (t < window.getEnd) {
          ctx.registerEventTimeTimer(t)
        }
      }
      TriggerResult.FIRE
    }

    override def clear(window: TimeWindow,
                       ctx: Trigger.TriggerContext): Unit = {
      val firstSeen = ctx.getPartitionedState(
        new ValueStateDescriptor[Boolean]("first-seen", Types.of[Boolean])
      )
      firstSeen.clear()
    }
  }

  class MyWindow extends ProcessWindowFunction[SensorReading, String, String, TimeWindow] {
    override def process(key: String,
                         context: Context, elements: Iterable[SensorReading],
                         out: Collector[String]): Unit = {
      out.collect("现在窗口有" + elements.size + "个元素")
    }
  }
}
```

### 0601 115310

```scala
package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object LateSideOutput {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val readings = env
      .socketTextStream("localhost", 9999, '\n')
      .map(line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000)
      })
      .assignAscendingTimestamps(_._2)

    val countPer10Secs = readings
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .sideOutputLateData(
        new OutputTag[(String, Long)]("late-readings")
      )
      .process(new CountFunction())

    val lateStream = countPer10Secs
      .getSideOutput(
        new OutputTag[(String, Long)]("late-readings")
      )

    lateStream.print()

    env.execute()
  }

  class CountFunction extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    override def process(key: String,
                         context: Context,
                         elements: Iterable[(String, Long)],
                         out: Collector[String]): Unit = {
      out.collect("窗口共有" + elements.size + "条数据")
    }
  }

}
```

### 0601 120132

```scala
package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object LateSideOutput1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val readings = env
      .socketTextStream("localhost", 9999, '\n')
      .map(line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000)
      })
      .assignAscendingTimestamps(_._2)

    val countPer10Secs = readings
      .keyBy(_._1)
      .process(new CountFunction())

    countPer10Secs
      .getSideOutput(new OutputTag[(String, Long)]("late"))
        .print()

    env.execute()
  }

  class CountFunction extends KeyedProcessFunction[String, (String, Long), (String, Long)] {
    val lateReadingOut = new OutputTag[(String, Long)]("late")

    override def processElement(value: (String, Long),
                                ctx: KeyedProcessFunction[String, (String, Long), (String, Long)]#Context,
                                out: _root_.org.apache.flink.util.Collector[(String, Long)]): Unit = {
      if (value._2 < ctx.timerService().currentWatermark()) {
        ctx.output(lateReadingOut, value)
      } else {
        out.collect(value)
      }
    }
  }
}
```

### 0601 141107

```scala
package com.atguigu.datastreamapi

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object UpdateWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val readings = env
      .socketTextStream("localhost", 9999, '\n')
      .map(line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000)
      })
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(5)) {
          override def extractTimestamp(element: (String, Long)): Long = element._2
        }
      )
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .allowedLateness(Time.seconds(5))
      .process(new UpdatingWindowCountFunction)

    readings.print()
    env.execute()
  }

  class UpdatingWindowCountFunction extends ProcessWindowFunction[(String, Long),
    String, String, TimeWindow] {
    override def process(key: String,
                         context: Context, elements: Iterable[(String, Long)],
                         out: Collector[String]): Unit = {
      val cnt = elements.size

      val isUpdate = context
        .windowState
        .getState(
          new ValueStateDescriptor[Boolean]("isUpdate", Types.of[Boolean])
        )

      if (!isUpdate.value()) {
        out.collect("当水位线超过窗口结束时间时，第一次触发窗口计算，窗口中共有" +
          cnt + "条数据")
        isUpdate.update(true)
      } else {
        out.collect("迟到元素来更新窗口计算结果了！" +
          "窗口中共有" + cnt + "条数据")
      }
    }
  }
}
```

### 0601 150528

```scala
package com.atguigu.datastreamapi

import org.apache.flink.api.common.state.{ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object ListStateExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val readings = env
      .addSource(new SensorSource)
      .keyBy(_.id)
      .process(new MyKey)

    readings.print()
    env.execute()
  }

  class MyKey extends KeyedProcessFunction[String, SensorReading, String] {
    lazy val listState = getRuntimeContext.getListState(
      new ListStateDescriptor[SensorReading]("list-state", Types.of[SensorReading])
    )

    lazy val timerTs = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("timer", Types.of[Long])
    )

    override def processElement(value: SensorReading,
                                ctx: KeyedProcessFunction[String, SensorReading, String]#Context,
                                out: Collector[String]): Unit = {
      listState.add(value)
      if (timerTs.value() == 0L) {
        ctx.timerService().registerProcessingTimeTimer(value.timestamp + 10 * 1000L)
        timerTs.update(value.timestamp + 10 * 1000L)
      }
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext,
                         out: Collector[String]): Unit = {
      val buffer: ListBuffer[SensorReading] = ListBuffer()
      import scala.collection.JavaConversions._
      for (reading <- listState.get) {
        buffer += reading
      }
      listState.clear()
      timerTs.clear()
      out.collect("传感器ID为" + ctx.getCurrentKey +"共" + buffer.size + "条数据")
    }
  }
}
```

### 0601 154737

```scala
package com.atguigu.datastreamapi

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object TempDiffExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val readings = env
      .addSource(new SensorSource)
      .keyBy(_.id)
      .flatMap(new TemperatureAlertFunction(1.7))

    readings.print()
    env.execute()
  }

  class TemperatureAlertFunction(val threshold: Double)
    extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {
    lazy val lastTempState = getRuntimeContext
      .getState(
        new ValueStateDescriptor[Double]("last", Types.of[Double])
      )

    override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
      val lastTemp = lastTempState.value()

      val tempDiff = (value.temperature - lastTemp).abs
      if (tempDiff > threshold) {
        out.collect((value.id, value.temperature, tempDiff))
      }
      this.lastTempState.update(value.temperature)
    }
  }
}
```

### 0601 155655

```scala
package com.atguigu.datastreamapi

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object RefactorTempDiffExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val readings = env
      .addSource(new SensorSource)
      .keyBy(_.id)
      .flatMapWithState[(String, Double, Double), Double] {
        case (in: SensorReading, None) => {
          (List.empty, Some(in.temperature))
        }
        case (r: SensorReading, lastTemp: Some[Double]) => {
          val tempDiff = (r.temperature - lastTemp.get).abs
          if (tempDiff > 1.7) {
            (List((r.id, r.temperature, tempDiff)), Some(r.temperature))
          } else {
            (List.empty, Some(r.temperature))
          }
        }
      }

    readings.print()
    env.execute()
  }
}
```

### 0601 161624

```scala
package com.atguigu.datastreamapi

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SelfCleaningTempDiff {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val readings = env
      .addSource(new SensorSource)
      .keyBy(_.id)
      .process(new SelfCleaningTemperatureAlertFunction(1.7))

    readings.print()
    env.execute()
  }

  class SelfCleaningTemperatureAlertFunction(val threshold: Double)
    extends KeyedProcessFunction[String, SensorReading, (String, Double, Double)] {
    lazy val lastTemp = getRuntimeContext.getState(
      new ValueStateDescriptor[Double]("last-temp", Types.of[Double])
    )
    lazy val lastTimer = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("last-timer", Types.of[Long])
    )

    override def processElement(value: SensorReading,
                                ctx: KeyedProcessFunction[String, SensorReading, (String, Double, Double)]#Context, out: Collector[(String, Double, Double)]): Unit = {
      val newTimer = value.timestamp + (3600 * 1000)
      val curTimer = lastTimer.value()
      ctx.timerService().deleteProcessingTimeTimer(curTimer)
      ctx.timerService().registerProcessingTimeTimer(newTimer)
      lastTimer.update(newTimer)

      val last = lastTemp.value()
      val tempDiff = (value.temperature - last).abs
      if (tempDiff > threshold) {
        out.collect((value.id, value.temperature, tempDiff))
      }
      lastTemp.update(value.temperature)
    }

    override def onTimer(ts: Long,
                         ctx: KeyedProcessFunction[String, SensorReading, (String, Double, Double)]#OnTimerContext,
                         out: Collector[(String, Double, Double)]): Unit = {
      lastTemp.clear()
      lastTimer.clear()
    }
  }



}
```

### 0602 111420

```scala
package com.atguigu.datastreamapi

import java.util.Properties

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object KafkaExample {
  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.setProperty(
      "bootstrap.servers",
      "localhost:9092"
    )
    properties.setProperty(
      "group.id",
      "consumer-group"
    )
    properties.setProperty(
      "key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer"
    )
    properties.setProperty(
      "value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer"
    )
    properties.setProperty("auto.offset.reset", "latest")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .addSource(
        new FlinkKafkaConsumer011[String](
          "test",
          new SimpleStringSchema(),
          properties
        )
      )

    stream.print()

    stream.addSink(
      new FlinkKafkaProducer011[String](
        "localhost:9092",
        "test",
        new SimpleStringSchema()))
    env.execute()
  }
}
```

### 0602 111433

```scala
package com.atguigu.datastreamapi

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducerUtil {
  def main(args: Array[String]): Unit = {
    writeToKafka("test")
  }

  def writeToKafka(topic: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put(
      "key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer"
    )
    props.put(
      "value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer"
    )
    val producer = new KafkaProducer[String, String](props)
    val record = new ProducerRecord[String, String](topic, "hello world")
    producer.send(record)
    producer.close()
  }
}
```

### 0602 115431

```scala
package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .addSource(new SensorSource)

    val conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build()

    stream.addSink(new RedisSink[SensorReading](conf, new RedisExampleMapper))

    env.execute()

  }

  class RedisExampleMapper extends RedisMapper[SensorReading] {
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(
        RedisCommand.HSET,
        "sensor_temperature")
    }

    override def getKeyFromData(t: SensorReading): String = t.id

    override def getValueFromData(t: SensorReading): String = {
      t.temperature.toString
    }
  }
}
```

### 0602 141841

```scala
package com.atguigu.datastreamapi

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

object ElasticSearchExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .addSource(new SensorSource)

    // es的主机和端口
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost", 9200))

    // 定义了如何将数据写入到es中去
    val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading](
      httpHosts, // es的主机名
      // 匿名类，定义如何将数据写入到es中
      new ElasticsearchSinkFunction[SensorReading] {
        override def process(t: SensorReading,
                             runtimeContext: RuntimeContext,
                             requestIndexer: RequestIndexer): Unit = {
          // 哈希表的key为string，value为string
          val json = new util.HashMap[String, String]()
          json.put("data", t.toString)
          // 构建一个写入es的请求
          val indexRequest = Requests
            .indexRequest()
            .index("sensor") // 索引的名字是sensor
            .source(json)

          requestIndexer.add(indexRequest)
        }
      }
    )

    // 用来定义每次写入多少条数据
    esSinkBuilder.setBulkFlushMaxActions(10)

    stream.addSink(esSinkBuilder.build())

    env.execute()
  }
}
```

### 0602 151625

```scala
package com.atguigu.datastreamapi

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

object MySQLExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)
    stream.addSink(new MyJdbcSink)

    env.execute()

  }

  // TwoPhaseCommitSinkFunction
  class MyJdbcSink extends RichSinkFunction[SensorReading] {
    var conn: Connection = _
    var insertStmt: PreparedStatement = _
    var updateStmt: PreparedStatement = _

    override def open(parameters: Configuration): Unit = {
      conn = DriverManager.getConnection(
        "jdbc:mysql://localhost:3306/test",
        "root","root"
      )

      insertStmt = conn.prepareStatement(
        "INSERT INTO temperatures (sensor, temp) VALUES (?, ?)"
      )

      updateStmt = conn.prepareStatement(
        "UPDATE temperatures SET temp = ? WHERE sensor = ?"
      )
    }

    override def invoke(value: SensorReading,
                        context: SinkFunction.Context[_]): Unit = {
      updateStmt.setDouble(1, value.temperature)
      updateStmt.setString(2, value.id)
      updateStmt.execute()

      if (updateStmt.getUpdateCount == 0) {
        insertStmt.setString(1, value.id)
        insertStmt.setDouble(2, value.temperature)
        insertStmt.execute()
      }
    }

    override def close(): Unit = {
      insertStmt.close()
      updateStmt.close()
      conn.close()
    }
  }
}
```

### 0602 165848

```scala
package com.atguigu.datastreamapi

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.Map

object CEPExample {

  case class LoginEvent(userId: String,
                        ip: String,
                        eventType: String,
                        eventTime: String)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 登录流
    val stream = env
      .fromElements(
        LoginEvent("1", "192.168.0.1", "fail", "1558430842"),
        LoginEvent("1", "192.168.0.2", "fail", "1558430843"),
        LoginEvent("1", "192.168.0.3", "fail", "1558430844"),
        LoginEvent("2", "192.168.10.10", "success", "1558430845")
      )
      .assignAscendingTimestamps(_.eventTime.toLong * 1000L)
      .keyBy(_.userId)

    // 10s内用户连续三次登录失败的模板
    val pattern = Pattern
      .begin[LoginEvent]("begin")
      .where(_.eventType.equals("fail"))
      .next("next")
      .where(_.eventType.equals("fail"))
      .next("end")
      .where(_.eventType.equals("fail"))
      .within(Time.seconds(10))

    // 在流中匹配模板
    val loginStream = CEP.pattern(stream, pattern)

    // 将匹配到的数据组打印出来
    loginStream
      .select((pattern: Map[String, Iterable[LoginEvent]]) => {
        val first = pattern.getOrElse("begin", null).iterator.next()
        val second = pattern.getOrElse("next", null).iterator.next()
        val third = pattern.getOrElse("end", null).iterator.next()

        (first.ip, second.ip, third.ip)
      })
      .print()

    env.execute()
  }
}
```

### 0603 091506

```scala
package com.atguigu.datastreamapi

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.Map

object CEPExample {

  case class LoginEvent(userId: String,
                        ip: String,
                        eventType: String,
                        eventTime: String)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 登录流
    val stream = env
      .fromElements(
        LoginEvent("1", "192.168.0.1", "fail", "1558430842"),
        LoginEvent("1", "192.168.0.2", "fail", "1558430843"),
        LoginEvent("1", "192.168.0.3", "fail", "1558430844"),
        LoginEvent("2", "192.168.10.10", "success", "1558430845")
      )
      .assignAscendingTimestamps(_.eventTime.toLong * 1000L)
      .keyBy(_.userId)

    // 10s内用户连续三次登录失败的模板
//    val pattern = Pattern
//      .begin[LoginEvent]("begin")
//      .where(_.eventType.equals("fail"))
//      .next("next")
//      .where(_.eventType.equals("fail"))
//      .next("end")
//      .where(_.eventType.equals("fail"))
//      .within(Time.seconds(10))
    val pattern = Pattern
      .begin[LoginEvent]("begin")
      .times(3)

    // 在流中匹配模板
    val loginStream = CEP.pattern(stream, pattern)

    // 将匹配到的数据组打印出来
    loginStream
      .select((pattern: Map[String, Iterable[LoginEvent]]) => {
        val first = pattern.getOrElse("begin", null).iterator.next()
//        val second = pattern.getOrElse("next", null).iterator.next()
//        val third = pattern.getOrElse("end", null).iterator.next()

        first.userId
      })
      .print()

    env.execute()
  }
}
```

### 0603 101900

```scala
package com.atguigu.datastreamapi

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._

object FlinkTableExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val settings = EnvironmentSettings
      .newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(
      env,
      settings
    )

    val stream = env.addSource(new SensorSource)

    val table = tableEnv.fromDataStream(stream)

    table
      .select('id, 'temperature)
      .toAppendStream[(String, Double)]
      .print()

    env.execute()
  }
}
```

### 0603 105310

```scala
package com.atguigu.datastreamapi

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Tumble}
import org.apache.flink.table.api.scala._

object FlinkTableExample1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val settings = EnvironmentSettings
      .newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(
      env,
      settings
    )

    val stream = env
      .addSource(new SensorSource)
      .assignAscendingTimestamps(_.timestamp)

    val table = tableEnv
      .fromDataStream(stream, 'id, 'ts.rowtime, 'temperature)
      .window(Tumble over 10.seconds on 'ts as 'tw)
      .groupBy('id, 'tw)
      .select('id, 'id.count)

    table
        .toRetractStream[(String, Long)]
        .print()


    env.execute()
  }
}
```

### 0603 113806

```scala
package com.atguigu.datastreamapi

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Tumble}
import org.apache.flink.table.api.scala._

object FlinkTableExample1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val settings = EnvironmentSettings
      .newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(
      env,
      settings
    )

    val stream = env
      .addSource(new SensorSource)
//      .assignAscendingTimestamps(_.timestamp)

    val table = tableEnv
      // 将DataStream转换成动态表
      .fromDataStream(stream, 'id, 'temperature, 'pt.proctime)
      .window(Tumble over 10.seconds on 'pt as 'tw)
      .groupBy('id, 'tw)
      // 将动态表转换成了另一张动态表
      .select('id, 'id.count)

    println(tableEnv.explain(table))

    table
      // 将动态表转换成了DataStream
        .toRetractStream[(String, Long)]
        .print()


    env.execute()
  }
}
```

### 0603 115956

```scala
package com.atguigu.datastreamapi

import java.sql.Timestamp

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._

object FlinkTableExample2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val settings = EnvironmentSettings
      .newInstance()
//      .useOldPlanner()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(
      env,
      settings
    )

    val sinkDDL: String =
      """
        |create table dataTable (
        |  id varchar(20) not null,
        |  ts bigint,
        |  temperature double,
        |  pt AS PROCTIME()
        |) with (
        |  'connector.type' = 'filesystem',
        |  'connector.path' = '/Users/yuanzuo/Desktop/Flink1125SH/src/main/resources/sensor.txt',
        |  'format.type' = 'csv'
        |)
      """.stripMargin

    tableEnv.sqlUpdate(sinkDDL) // 执行 DDL
    tableEnv
      .sqlQuery("select id, ts from dataTable")
        .toAppendStream[(String, Long)]
        .print()

    env.execute()
  }
}
```

### 0603 144123

```scala
package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._

object EveryTenSecOnSlideWindow {
  def main(args: Array[String]): Unit = {
    // 初始化执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 初始化表环境
    val settings = EnvironmentSettings
      .newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment
      .create(env, settings)

    // 流数据
    val stream = env
      .addSource(new SensorSource)
      .assignAscendingTimestamps(_.timestamp)

    // 从流创建表
    val dataTable = tableEnv
      .fromDataStream(stream, 'id, 'ts.rowtime, 'temperature)

    // 写sql完成开窗统计
    tableEnv
      .sqlQuery(
        "select id, count(id) from " + dataTable +
          " group by id, hop(ts, interval '5' second, interval '10' second)"
      )
      // 只要使用了group by, 必须使用撤回流
      .toRetractStream[(String, Long)]
      .print()

    env.execute()
  }
}
```

### 0603 144134

```scala
package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._

object EveryTenSec {
  def main(args: Array[String]): Unit = {
    // 初始化执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 初始化表环境
    val settings = EnvironmentSettings
      .newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment
      .create(env, settings)

    // 流数据
    val stream = env
      .addSource(new SensorSource)
      .assignAscendingTimestamps(_.timestamp)

    // 从流创建表
    val dataTable = tableEnv
      .fromDataStream(stream, 'id, 'ts.rowtime, 'temperature)

    // 写sql完成开窗统计
    tableEnv
      .sqlQuery(
        "select id, count(id) from " + dataTable +
          " group by id, tumble(ts, interval '10' second)"
      )
      // 只要使用了group by, 必须使用撤回流
      .toRetractStream[(String, Long)]
      .print()

    env.execute()
  }
}
```

### 0603 150034

```scala
package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object EveryTenSecProcessFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .addSource(new SensorSource)
      .keyBy(_.id)
      .timeWindow(Time.seconds(10))
      .process(new ProWin)

    stream.print()
    env.execute()
  }

  class ProWin extends ProcessWindowFunction[SensorReading,
    String, String, TimeWindow] {
    override def process(key: String,
                         context: Context,
                         elements: Iterable[SensorReading],
                         out: Collector[String]): Unit = {
      out.collect(elements.size + "个元素")
    }
  }
}
```

### 0603 153929

```scala
package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction

object ScalarFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tEnv = StreamTableEnvironment.create(env, settings)

    val stream = env.addSource(new SensorSource)

    // 初始化udf函数
    val hashCode = new HashCode(10)
    tEnv
      // 注册udf函数
      .registerFunction("hashCode", hashCode)

    val table = tEnv.fromDataStream(stream, 'id)

    // 在table api中使用udf函数
    table
      .select('id, hashCode('id))
      .toAppendStream[(String, Int)]
      .print()

    // 在flink sql中使用udf函数

    // 创建视图
    tEnv.createTemporaryView("t", table, 'id)

    tEnv
        .sqlQuery("SELECT id, hashCode(id) FROM t")
        .toAppendStream[(String, Int)]
        .print()

    env.execute()

  }

  class HashCode(factor: Int) extends ScalarFunction {
    def eval(s: String): Int = {
      s.hashCode * factor
    }
  }
}
```

### 0603 162258

```scala
package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.api.scala._

object TableFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tEnv = StreamTableEnvironment.create(env, settings)
    val split = new Split("#")

    val stream = env.fromElements("hello#world", "zuoyuan#atguigu")

    val table = tEnv.fromDataStream(stream, 's)

    // table api使用udf
//    table
//      // 侧写
////      .joinLateral((split('s) as ('word, 'length)))
//      .leftOuterJoinLateral(split('s) as ('word, 'length))
//      .select('s, 'word, 'length)
//      .toAppendStream[(String, String, Long)]
//      .print()

    // sql中使用udf

    tEnv.registerFunction("split", new Split("#"))

    tEnv.createTemporaryView("t", table, 's)

    tEnv
      // 大写`T`是sql中元组
        .sqlQuery(
          """SELECT s, word, length
            | FROM t
            | LEFT JOIN LATERAL TABLE(split(s)) as T(word, length)
            | ON TRUE
            |""".stripMargin)
        .toAppendStream[(String, String, Long)]
        .print()

    tEnv
        .sqlQuery(
          """SELECT s, word, length
            | FROM t,
            | LATERAL TABLE(split(s)) as T(word, length)
            |""".stripMargin)
        .toAppendStream[(String, String, Long)]
        .print()

    env.execute()

  }

  class Split(separator: String) extends TableFunction[(String, Int)] {
    def eval(s: String): Unit = {
      // 使用collect方法来发射`一行`数据
      s.split(separator).foreach(x => collect((x, x.length)))
    }
  }
}
```

### 0603 170109

```scala
package com.atguigu.datastreamapi

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Types}
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row

object AggregateFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tEnv = StreamTableEnvironment.create(env, settings)

    // 实例化udf函数
    val agg = new MyMinMax

    val stream = env.fromElements(
      (1, -1),
      (1, 2)
    )

    val table = tEnv.fromDataStream(stream, 'key, 'a)
    table
      .groupBy('key) // keyBy
      .aggregate(agg('a) as ('x, 'y))
      .select('key, 'x, 'y)
      .toRetractStream[(Long, Long, Long)]
      .print()

    env.execute()
  }

  case class MyMinMaxAcc(var min: Int, var max: Int)

  // Row是动态表每一行的泛型
  class MyMinMax extends AggregateFunction[Row, MyMinMaxAcc] {
    override def createAccumulator(): MyMinMaxAcc = {
      MyMinMaxAcc(Int.MaxValue, Int.MinValue)
    }
    override def getValue(acc: MyMinMaxAcc): Row = {
      Row.of(Integer.valueOf(acc.min), Integer.valueOf(acc.max))
    }
    override def getResultType: TypeInformation[Row] = {
      new RowTypeInfo(Types.INT, Types.INT)
    }
    def accumulate(acc: MyMinMaxAcc, value: Int): Unit = {
      if (value < acc.min) {
        acc.min = value
      }
      if (value > acc.max) {
        acc.max = value
      }
    }
  }
}
```

### 0605 085503

```scala
package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.util.Collector

object TableAggregateFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tEnv = StreamTableEnvironment.create(env, settings)

    val table = tEnv.fromDataStream(stream, 'id, 'temperature)

    val top2Temp = new Top2Temp

    table
      .groupBy('id)
      .flatAggregate(top2Temp('temperature) as ('temp, 'rank))
      .select('id, 'temp, 'rank)
      .toRetractStream[(String, Double, Int)]
      .print("agg temp")

    env.execute()
  }

  class Top2TempAcc {
    var highestTemp: Double = Int.MinValue
    var secondHighestTemp: Double = Int.MinValue
  }

  class Top2Temp extends TableAggregateFunction[(Double, Int), Top2TempAcc] {
    override def createAccumulator(): Top2TempAcc = new Top2TempAcc

    def accumulate(acc: Top2TempAcc, temp: Double): Unit = {
      if (temp > acc.highestTemp) {
        acc.secondHighestTemp = acc.highestTemp
        acc.highestTemp = temp
      } else if (temp > acc.secondHighestTemp) {
        acc.secondHighestTemp = temp
      }
    }

    // 输出(温度，排名)信息
    def emitValue(acc: Top2TempAcc, out: Collector[(Double, Int)]): Unit = {
      out.collect(acc.highestTemp, 1)
      out.collect(acc.secondHighestTemp, 2)
    }
  }
}
```

### 0605 092300

```scala
package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.{AggregateFunction, TableAggregateFunction}
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

object UDFAggregateFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tEnv = StreamTableEnvironment.create(env, settings)

    val table = tEnv.fromDataStream(stream, 'id, 'temperature)

    val avgTemp = new AvgTemp

//    table
//        .groupBy('id)
//        .aggregate(avgTemp('temperature) as 'avgTemp)
//        .select('id, 'avgTemp)
//        .toRetractStream[(String, Double)]
//        .print()

    // sql实现
    tEnv.createTemporaryView("sensor", table)
    tEnv.registerFunction("avgTemp", avgTemp)

    tEnv
        .sqlQuery(
          """
            |SELECT
            |id, avgTemp(temperature)
            |FROM sensor
            |GROUP BY id
            |""".stripMargin
        )
        .toRetractStream[(String, Double)]
        .print()
    
    env.execute()
  }

  class AvgTempAcc {
    var sum: Double = 0.0
    var count: Int = 0
  }

  class AvgTemp extends AggregateFunction[Double, AvgTempAcc] {
    override def createAccumulator(): AvgTempAcc = new AvgTempAcc

    override def getValue(accumulator: AvgTempAcc): Double = {
      accumulator.sum / accumulator.count
    }

    def accumulate(acc: AvgTempAcc, temp: Double): Unit = {
      acc.sum += temp
      acc.count += 1
    }
  }

}
```

### 0605 104900

```scala
package com.atguigu.project

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object TopNItems {

  // 用户行为数据
  case class UserBehavior(userId: Long,
                          itemId: Long,
                          categoryId: Int,
                          behavior: String,
                          timestamp: Long)

  // 窗口聚合输出的样例类
  // 某个窗口中，某个商品的pv次数
  case class ItemViewCount(itemId: Long,
                           windowEnd: Long,
                           count: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .readTextFile("/Users/yuanzuo/Desktop/Flink1125SH/src/main/resources/UserBehavior.csv")
      .map(line => {
        val arr = line.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong * 1000)
      })
      .filter(_.behavior.equals("pv"))
      .assignAscendingTimestamps(_.timestamp) // 数据集提前按照时间戳排了一下序
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAgg, new WindowResult)
      .keyBy(_.windowEnd)
      .process(new TopN(3))

    stream.print()
    env.execute()
  }

  class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }

  class WindowResult extends ProcessWindowFunction[Long, ItemViewCount, Long, TimeWindow] {
    override def process(key: Long,
                         context: Context,
                         elements: Iterable[Long],
                         out: Collector[ItemViewCount]): Unit = {
      out.collect(ItemViewCount(key, context.window.getEnd, elements.head))
    }
  }

  class TopN(val n: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {
    lazy val itemList = getRuntimeContext.getListState(
      new ListStateDescriptor[ItemViewCount]("item-list", Types.of[ItemViewCount])
    )

    override def processElement(value: ItemViewCount,
                                ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context,
                                out: Collector[String]): Unit = {
      itemList.add(value)
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 100)
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext,
                         out: Collector[String]): Unit = {
      val allItems: ListBuffer[ItemViewCount] = ListBuffer()
      import scala.collection.JavaConversions._
      for (item <- itemList.get) {
        allItems += item
      }
      itemList.clear() // 清空状态变量，gc

      val sortedItems = allItems
        .sortBy(-_.count) // 降序排列
        .take(n)

      val result = new StringBuilder
      result
        .append("=====================================\n")
        .append("时间：")
        .append(new Timestamp(timestamp - 100))
        .append("\n")

      for (i <- sortedItems.indices) {
        val cur = sortedItems(i)
        result
          .append("No")
          .append(i + 1)
          .append(": 商品ID = ")
          .append(cur.itemId)
          .append(" 浏览量 = ")
          .append(cur.count)
          .append("\n")
      }
      result
        .append("=====================================\n\n\n")

      Thread.sleep(1000)
      out.collect(result.toString)

    }
  }
}
```

### 0605 114900

```scala
package com.atguigu.project

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._

object SQLTopNItems {

  // 用户行为数据
  case class UserBehavior(userId: Long,
                          itemId: Long,
                          categoryId: Int,
                          behavior: String,
                          timestamp: Long)

  // 窗口聚合输出的样例类
  // 某个窗口中，某个商品的pv次数
  case class ItemViewCount(itemId: Long,
                           windowEnd: Long,
                           count: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .readTextFile("/Users/yuanzuo/Desktop/Flink1125SH/src/main/resources/UserBehavior.csv")
      .map(line => {
        val arr = line.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong * 1000)
      })
      .filter(_.behavior.equals("pv"))
      .assignAscendingTimestamps(_.timestamp) // 数据集提前按照时间戳排了一下序

    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner() // top n 需求只有blink planner支持
      .inStreamingMode()
      .build()

    val tEnv = StreamTableEnvironment.create(env, settings)

    tEnv.createTemporaryView("t", stream, 'itemId, 'timestamp.rowtime as 'ts)

    tEnv
        .sqlQuery(
          """
            |SELECT icount, windowEnd, row_num
            |FROM (
            |      SELECT icount,
            |             windowEnd,
            |             ROW_NUMBER() OVER (PARTITION BY windowEnd ORDER BY icount DESC) as row_num
            |      FROM
            |           (SELECT count(itemId) as icount,
            |                   HOP_END(ts, INTERVAL '5' MINUTE, INTERVAL '1' HOUR) as windowEnd
            |            FROM t GROUP BY HOP(ts, INTERVAL '5' MINUTE, INTERVAL '1' HOUR), itemId) as topn
            |)
            |WHERE row_num <= 3
            |""".stripMargin)
        // (itemId, ts, rank)
        .toRetractStream[(Long, Timestamp, Long)]
        .print()

    env.execute()
  }

}
```

### 0605 152700

```scala
package com.atguigu.project

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.Set

// 一段时间有多少用户访问了网站，涉及到去重
// 滑动窗口，长度1小时，滑动距离5秒钟，每小时独立访问用户上亿
// 阿里双十一
// 这道题的来源：阿里负责双十一大屏的工程师问的
// 每个userid占用1KB空间，10 ^ 9 个userid占用多少？--> 1TB
// 海量数据去重只有一种办法：布隆过滤器
object UniqueVisitor {
  case class UserBehavior(userId: Long,
                          itemId: Long,
                          categoryId: Int,
                          behavior: String,
                          timestamp: Long)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env
      .readTextFile("/Users/yuanzuo/Desktop/Flink1125SH/src/main/resources/UserBehavior.csv")
      .map(line => {
        val arr = line.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong * 1000)
      })
      .filter(_.behavior.equals("pv"))
      .assignAscendingTimestamps(_.timestamp)
      .map(r => (r.userId, "key"))
      .keyBy(_._2)
      .timeWindow(Time.hours(1))
      .process(new ComputeUV)

    stream.print()
    env.execute()
  }

  class ComputeUV extends ProcessWindowFunction[(Long, String), String, String, TimeWindow] {
    override def process(key: String,
                         context: Context,
                         elements: Iterable[(Long, String)],
                         out: Collector[String]): Unit = {
      var s: Set[Long] = Set()

      for (e <- elements) {
        s += e._1
      }

      out.collect("窗口结束时间为：" + new Timestamp(context.window.getEnd) + "的窗口的UV数据是：" + s.size)
    }
  }
}
```

### 0605 153900

```scala
package com.atguigu.project

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction

import org.apache.flink.streaming.api.functions
.timestamps.BoundedOutOfOrdernessTimestampExtractor

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object ApacheLogAnalysis {

  case class ApacheLogEvent(ip: String,
                            userId: String,
                            eventTime: Long,
                            method: String,
                            url: String)

  case class UrlViewCount(url: String,
                          windowEnd: Long,
                          count: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val stream = env
      // 文件的绝对路径
      .readTextFile("apache.log的绝对路径")
      .map(line => {
        val linearray = line.split(" ")
        // 把时间戳ETL成毫秒
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = simpleDateFormat.parse(linearray(3)).getTime
        ApacheLogEvent(linearray(0),
                       linearray(2),
                       timestamp,
                       linearray(5),
                       linearray(6))
      })
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](
          Time.milliseconds(1000)
        ) {
          override def extractTimestamp(t: ApacheLogEvent): Long = {
            t.eventTime
          }
        }
      )
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .aggregate(new CountAgg(), new WindowResultFunction())
      .keyBy(_.windowEnd)
      .process(new TopNHotUrls(5))
      .print()

    env.execute("Traffic Analysis Job")
  }

  class CountAgg extends AggregateFunction[ApacheLogEvent, Long, Long] {
    override def createAccumulator(): Long = 0L
    override def add(apacheLogEvent: ApacheLogEvent, acc: Long): Long = acc + 1
    override def getResult(acc: Long): Long = acc
    override def merge(acc1: Long, acc2: Long): Long = acc1 + acc2
  }

  class WindowResultFunction
    extends ProcessWindowFunction[Long, UrlViewCount, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
      out.collect(UrlViewCount(key, context.window.getEnd, elements.iterator.next()))
    }
  }

  class TopNHotUrls(topSize: Int)
    extends KeyedProcessFunction[Long, UrlViewCount, String] {
    
    lazy val urlState = getRuntimeContext.getListState(
      new ListStateDescriptor[UrlViewCount](
        "urlState-state",
        Types.of[UrlViewCount]
      )
    )

    override def processElement(
      input: UrlViewCount,
      context: KeyedProcessFunction[Long, UrlViewCount, String]#Context,
      collector: Collector[String]
    ): Unit = {
      // 每条数据都保存到状态中
      urlState.add(input)
      context
        .timerService
        .registerEventTimeTimer(input.windowEnd + 1)
    }

    override def onTimer(
      timestamp: Long,
      ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext,
      out: Collector[String]
    ): Unit = {
      // 获取收到的所有URL访问量
      val allUrlViews: ListBuffer[UrlViewCount] = ListBuffer()
      import scala.collection.JavaConversions._
      for (urlView <- urlState.get) {
        allUrlViews += urlView
      }
      // 提前清除状态中的数据，释放空间
      urlState.clear()
      // 按照访问量从大到小排序
      val sortedUrlViews = allUrlViews.sortBy(_.count)(Ordering.Long.reverse)
        .take(topSize)
      // 将排名信息格式化成 String, 便于打印
      var result: StringBuilder = new StringBuilder
      result
        .append("====================================\n")
        .append("时间: ")
        .append(new Timestamp(timestamp - 1))
        .append("\n")

      for (i <- sortedUrlViews.indices) {
        val currentUrlView: UrlViewCount = sortedUrlViews(i)
        // e.g.  No1：  URL=/blog/tags/firefox?flav=rss20  流量=55
        result
          .append("No")
          .append(i + 1)
          .append(": ")
          .append("  URL=")
          .append(currentUrlView.url)
          .append("  流量=")
          .append(currentUrlView.count)
          .append("\n")
      }
      result
        .append("====================================\n\n")
      // 控制输出频率，模拟实时滚动结果
      Thread.sleep(1000)
      out.collect(result.toString)
    }
  }
}
```

### 0605 170100

```scala
package com.atguigu.project

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

import scala.collection.Set

// 一段时间有多少用户访问了网站，涉及到去重
// 滑动窗口，长度1小时，滑动距离5秒钟，每小时独立访问用户上亿
// 阿里双十一
// 这道题的来源：阿里负责双十一大屏的工程师问的
// 每个userid占用1KB空间，10 ^ 9 个userid占用多少？--> 1TB
// 海量数据去重只有一种办法：布隆过滤器
object UniqueVisitorByBloomFilter {

  case class UserBehavior(userId: Long,
                          itemId: Long,
                          categoryId: Int,
                          behavior: String,
                          timestamp: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty(
      "key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer"
    )
    properties.setProperty(
      "value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer"
    )
    properties.setProperty(
      "auto.offset.reset",
      "latest"
    )

    val stream = env
      .readTextFile("/Users/yuanzuo/Desktop/Flink1125SH/src/main/resources/UserBehavior.csv")
//      .addSource(new FlinkKafkaConsumer011[String](
//        "hotitems1",
//        new SimpleStringSchema(),
//        properties
//      ))
      .map(line => {
        val arr = line.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong * 1000)
      })
      .filter(_.behavior.equals("pv"))
      .assignAscendingTimestamps(_.timestamp)
      .map(r => (r.userId, "key"))
      .keyBy(_._2)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger)
      .process(new FilterWindow)

    env.execute()
  }

  // 触发器的作用：每来一条数据，就触发窗口的计算（去重的逻辑），并清空窗口
  // 类似于增量聚合
  class MyTrigger extends Trigger[(Long, String), TimeWindow] {
    override def onElement(element: (Long, String),
                           timestamp: Long,
                           window: TimeWindow,
                           ctx: TriggerContext): TriggerResult = {
      // 触发窗口计算，清空窗口
      TriggerResult.FIRE_AND_PURGE
    }

    override def onProcessingTime(time: Long,
                                  window: TimeWindow,
                                  ctx: TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def onEventTime(time: Long,
                             window: TimeWindow,
                             ctx: TriggerContext): TriggerResult = {
      if (ctx.getCurrentWatermark >= window.getEnd) {
        val jedis = new Jedis("localhost", 6379)
        val key = window.getEnd.toString
        println(new Timestamp(key.toLong), jedis.hget("UvCountHashTable", key))
        TriggerResult.FIRE_AND_PURGE
      }
      TriggerResult.CONTINUE
    }

    override def clear(window: TimeWindow, ctx: TriggerContext): Unit = {}
  }

  class FilterWindow extends ProcessWindowFunction[(Long, String), String, String, TimeWindow] {
    lazy val jedis = new Jedis("localhost", 6379)

    override def process(key: String,
                         context: Context,
                         elements: Iterable[(Long, String)],
                         out: Collector[String]): Unit = {
      // redis保存一张哈希表
      // `UvCountHashTable`: `windowEnd` -> `count`
      // 还需要维护很多键值对
      // `windowEnd` -> `bit array`

      var count = 0L

      val key = context.window.getEnd.toString // windowEnd

      // 如果窗口的uv值不为0，取出计数值
      if (jedis.hget("UvCountHashTable", key) != null) {
        count = jedis.hget("UvCountHashTable", key).toLong
      }

      val userId = elements.head._1.toString // 取出userid
      val offset = bloomHash(userId, 1 << 29) // userId经过hash以后在位数组中的下标

      val isExist = jedis.getbit(key, offset) // getbit会自动创建key对应的位数组，如果位数组不存在的话
      if (!isExist) {
        // 如果offset下标对应的比特数组的相应位为0，那么翻转为1
        jedis.setbit(key, offset, true)
        // 由于userid不存在，所以uv数量加一
        jedis.hset("UvCountHashTable", key, (count + 1).toString)
      }
    }

    def bloomHash(userId: String, bitArraySize: Long): Long = {
      var result = 0
      for (i <- 0 until userId.length) {
        result = result * 61 + userId.charAt(i)
      }
      (bitArraySize - 1) & result
    }
  }
}
```

### 0606 092400

```scala
package com.atguigu.project

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object TopNUrl {

  case class ApacheLogEvent(ip: String,
                            userId: String,
                            eventTime: Long,
                            method: String,
                            url: String)

  case class UrlViewCount(url: String,
                          windowEnd: Long,
                          count: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .readTextFile("/Users/yuanzuo/Desktop/Flink1125SH/src/main/resources/apachelog.txt")
      .map(line => {
        val arr = line.split(" ")
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val ts = simpleDateFormat.parse(arr(3)).getTime
        ApacheLogEvent(arr(0), arr(2), ts, arr(5), arr(6))
      })
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
          override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
        }
      )
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .aggregate(new CountAgg, new WindowResult)
      .keyBy(_.windowEnd)
      .process(new TopNUrls(5))

    stream.print()
    env.execute()
  }

  class CountAgg extends AggregateFunction[ApacheLogEvent, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }

  class WindowResult extends ProcessWindowFunction[Long, UrlViewCount, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
      out.collect(UrlViewCount(key, context.window.getEnd, elements.head))
    }
  }

  class TopNUrls(val topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {
    lazy val urlState = getRuntimeContext.getListState(
      new ListStateDescriptor[UrlViewCount]("url-state", Types.of[UrlViewCount])
    )

    override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
      urlState.add(value)
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 100)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      val allUrlViews: ListBuffer[UrlViewCount] = ListBuffer()
      import scala.collection.JavaConversions._
      for (urlView <- urlState.get) {
        allUrlViews += urlView
      }
      urlState.clear()
      val sortedUrlViews = allUrlViews.sortBy(-_.count).take(topSize)
      val result = new StringBuilder
      result
        .append("=======================================\n")
        .append("时间：")
        .append(new Timestamp(timestamp - 100))
        .append("\n")

      for (i <- sortedUrlViews.indices) {
        val curr = sortedUrlViews(i)
        result
          .append("No.")
          .append(i + 1)
          .append(": ")
          .append(" URL = ")
          .append(curr.url)
          .append(" 流量 = ")
          .append(curr.count)
          .append("\n")
      }
      result
        .append("=======================================\n\n\n")

      Thread.sleep(1000)
      out.collect(result.toString)
    }
  }
}
```

### 0606 095500

```scala
package com.atguigu.project

import java.sql.Timestamp
import java.util.{Calendar, UUID}

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

object AppMarketingByChannel {

  case class MarketingUserBehavior(userId: String,
                                   behavior: String,
                                   channel: String,
                                   ts: Long)

  class SimulatedEventSource extends RichParallelSourceFunction[MarketingUserBehavior] {
    var running = true

    // fake渠道信息
    val channelSet = Seq("AppStore", "XiaomiStore")
    // fake用户行为信息
    val behaviorTypes = Seq("BROWSE", "CLICK", "UNINSTALL", "INSTALL")

    val rand = new Random

    override def run(ctx: SourceContext[MarketingUserBehavior]): Unit = {
      while (running) {
        // UUID产生一个唯一的字符串，本质就是哈希
        val userId = UUID.randomUUID().toString
        val behaviorType = behaviorTypes(rand.nextInt(behaviorTypes.size))
        val channel = channelSet(rand.nextInt(channelSet.size))
        val ts = Calendar.getInstance().getTimeInMillis

        ctx.collect(MarketingUserBehavior(userId, behaviorType, channel, ts))

        Thread.sleep(10)
      }
    }

    override def cancel(): Unit = running = false

  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env
      .addSource(new SimulatedEventSource)
      .assignAscendingTimestamps(_.ts)
      .filter(_.behavior != "UNINSTALL")
      .map(r => {
        ((r.channel, r.behavior), 1L)
      })
      .keyBy(_._1)
      .timeWindow(Time.seconds(5), Time.seconds(1))
      .process(new MarketingCountByChannel)

    stream.print()
    env.execute()
  }

  class MarketingCountByChannel extends ProcessWindowFunction[((String, String), Long), (String, Long, Timestamp), (String, String), TimeWindow] {
    override def process(key: (String, String), context: Context, elements: Iterable[((String, String), Long)], out: Collector[(String, Long, Timestamp)]): Unit = {
      out.collect((key._1, elements.size, new Timestamp(context.window.getEnd)))
    }
  }
}
```

### 0606 100000

```scala
package com.atguigu.project

import java.sql.Timestamp

import com.atguigu.project.AppMarketingByChannel.SimulatedEventSource
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object AppMarketingStatistics {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env
      .addSource(new SimulatedEventSource)
      .assignAscendingTimestamps(_.ts)
      .filter(_.behavior != "UNINSTALL")
      .map(r => ("dummy", 1L))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5), Time.seconds(1))
      .process(new MarketingCountTotal)
    stream.print()
    env.execute()
  }

  class MarketingCountTotal extends ProcessWindowFunction[(String, Long), (String, Long, Timestamp), String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[(String, Long, Timestamp)]): Unit = {
      out.collect((key, elements.size, new Timestamp(context.window.getEnd)))
    }
  }
}
```

### 0606 104500

```scala
package com.atguigu.project

import java.sql.Timestamp

import com.atguigu.project.AppMarketingByChannel.{MarketingUserBehavior, SimulatedEventSource}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object AppMarketingStatistics {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env
      .addSource(new SimulatedEventSource)
      .assignAscendingTimestamps(_.ts)
      .filter(_.behavior != "UNINSTALL")
      .timeWindowAll(Time.seconds(5), Time.seconds(1))
      .process(new MarketingCountTotal)
    stream.print()
    env.execute()
  }

  class MarketingCountTotal extends ProcessAllWindowFunction[MarketingUserBehavior, (Long, Timestamp), TimeWindow] {
    override def process(context: Context, elements: Iterable[MarketingUserBehavior], out: Collector[(Long, Timestamp)]): Unit = {
      out.collect((elements.size, new Timestamp(context.window.getEnd)))
    }
  }
}
```

### 0606 111800

```scala
package com.atguigu.project

import org.apache.flink.api.common.state.{ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object LoginFailWithoutCEP {

  case class LoginEvent(userId: String,
                        ipAddr: String,
                        eventType: String,
                        eventTime: String)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .fromElements(
        LoginEvent("1", "0.0.0.0", "fail", "1"),
        LoginEvent("1", "0.0.0.0", "success", "2"),
        LoginEvent("1", "0.0.0.0", "fail", "3"),
        LoginEvent("1", "0.0.0.0", "fail", "4")
      )
      .assignAscendingTimestamps(_.eventTime.toLong * 1000)
      .keyBy(_.userId)
      .process(new MatchFunction)

    stream.print()
    env.execute()
  }

  class MatchFunction extends KeyedProcessFunction[String, LoginEvent, String] {
    lazy val loginState = getRuntimeContext.getListState(
      new ListStateDescriptor[LoginEvent]("login-state", Types.of[LoginEvent])
    )
    lazy val tsState = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("ts-state", Types.of[Long])
    )
    override def processElement(value: LoginEvent,
                                ctx: KeyedProcessFunction[String, LoginEvent, String]#Context,
                                out: Collector[String]): Unit = {
      if (value.eventType.equals("fail")) {
        loginState.add(value) // 用来保存登录失败的事件
        if (tsState.value() == 0L) { // 检查一下有没有定时器存在
          // 5s中之内连续2次登录失败，报警
          ctx.timerService().registerEventTimeTimer(value.eventTime.toLong * 1000 + 5000L)
          tsState.update(value.eventTime.toLong * 1000 + 5000L)
        }
      }

      if (value.eventType.equals("success")) {
        loginState.clear()
        ctx.timerService().deleteEventTimeTimer(tsState.value())
        tsState.clear()
      }
    }
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, LoginEvent, String]#OnTimerContext, out: Collector[String]): Unit = {
      val allLogins = ListBuffer[LoginEvent]()
      import scala.collection.JavaConversions._
      for (login <- loginState.get) {
        allLogins += login
      }
      loginState.clear()
      if (allLogins.length >= 2) {
        out.collect("5s之内连续两次登录失败")
      }
    }
  }
}
```

### 0606 142100

```scala
package com.atguigu.project

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.Map

object OrderTimeout {

  case class OrderEvent(orderId: String,
                        eventType: String,
                        eventTime: String)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env
      .fromElements(
        OrderEvent("1", "create", "2"),
        OrderEvent("2", "create", "3"),
        OrderEvent("2", "pay", "4")
      )
      .assignAscendingTimestamps(_.eventTime.toLong * 1000L)
      .keyBy(_.orderId)

    val pattern = Pattern
      .begin[OrderEvent]("start")
      .where(_.eventType.equals("create"))
      .next("end")
      .where(_.eventType.equals("pay"))
      .within(Time.seconds(5))

    val patternStream = CEP.pattern(stream, pattern)

    // 将超时的订单发送到侧输出标签
    val orderTimeoutOutputTag = OutputTag[String]("order-timeout")

    // 处理超时事件的函数
    val timeoutFunc = (map: Map[String, Iterable[OrderEvent]], ts: Long, out: Collector[String]) => {
      val order = map("start").head
      out.collect("超时订单的ID为：" + order.orderId)
    }

    // 处理没有超时的订单
    val selectFunc = (map: Map[String, Iterable[OrderEvent]], out: Collector[String]) => {
      val order = map("end").head
      out.collect("没有超时的订单ID为：" + order.orderId)
    }

    val timeoutOrder = patternStream
      .flatSelect(orderTimeoutOutputTag)(timeoutFunc)(selectFunc)

    timeoutOrder.print()
    timeoutOrder.getSideOutput(orderTimeoutOutputTag).print()

    env.execute()
  }
}
```

### 0606 145700

```scala
package com.atguigu.project

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object OrderTimeoutWIthoutCep {

  case class OrderEvent(orderId: String,
                        eventType: String,
                        eventTime: String)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env
      .fromElements(
        OrderEvent("1", "create", "2"),
        OrderEvent("2", "create", "3"),
        OrderEvent("2", "pay", "4")
      )
      .assignAscendingTimestamps(_.eventTime.toLong * 1000L)
      .keyBy(_.orderId)
      .process(new OrderMatchFunc)

    stream.print()
    env.execute()
  }

  class OrderMatchFunc extends KeyedProcessFunction[String, OrderEvent, String] {
    lazy val orderState = getRuntimeContext.getState(
      new ValueStateDescriptor[OrderEvent]("saved order", Types.of[OrderEvent])
    )

    override def processElement(value: OrderEvent,
                                ctx: KeyedProcessFunction[String, OrderEvent, String]#Context,
                                out: Collector[String]): Unit = {
      if (value.eventType.equals("create")) {
        if (orderState.value() == null) { // 为什么要判空？因为可能出现`pay`先到的情况
          // 如果orderState为空，保存`create`事件
          orderState.update(value)
        }
      } else {
        // 保存`pay`事件
        orderState.update(value)
      }

      ctx.timerService().registerEventTimeTimer(value.eventTime.toLong * 1000 + 5000L)
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[String, OrderEvent, String]#OnTimerContext,
                         out: Collector[String]): Unit = {
      val savedOrder = orderState.value()

      if (savedOrder != null && savedOrder.eventType.equals("create")) {
        out.collect("超时订单的ID为：" + savedOrder.orderId)
      }

      orderState.clear()
    }
  }
}
```

### 0606 155100

```scala
package com.atguigu.project

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object TwoStreamsJoin {

  // 订单事件
  case class OrderEvent(orderId: String,
                        eventType: String,
                        eventTime: String)

  // 支付事件
  case class PayEvent(orderId: String,
                      eventType: String,
                      eventTime: String)

  // 未被匹配的订单事件
  val unmatchedOrders = new OutputTag[String]("unmatched-orders"){}

  // 未被匹配的支付事件
  val unmatchedPays = new OutputTag[String]("unmatched-pays"){}


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 服务器端数据库的支付信息
    val orders = env
      .fromElements(
        OrderEvent("1", "pay", "4"),
        OrderEvent("2", "pay", "5"),
        OrderEvent("3", "pay", "9")
      )
      .assignAscendingTimestamps(_.eventTime.toLong * 1000L)
      .keyBy(_.orderId)

    // 远程的支付信息
    val pays = env
      .fromElements(
        PayEvent("1", "weixin", "7"),
        PayEvent("2", "zhifubao", "8"),
        PayEvent("4", "zhifubao", "10")
      )
      .assignAscendingTimestamps(_.eventTime.toLong * 1000L)
      .keyBy(_.orderId)

    val processed = orders
      .connect(pays)
      .process(new RealTimeCheck)

    processed.print() // 对账成功的
    processed.getSideOutput(unmatchedPays).print()
    processed.getSideOutput(unmatchedOrders).print()

    env.execute()
  }

  class RealTimeCheck extends CoProcessFunction[OrderEvent, PayEvent, String] {
    // 用来保存订单事件
    lazy val orderState = getRuntimeContext.getState(
      new ValueStateDescriptor[OrderEvent]("order-state", Types.of[OrderEvent])
    )
    // 用来保存支付事件
    lazy val payState = getRuntimeContext.getState(
      new ValueStateDescriptor[PayEvent]("pay-state", Types.of[PayEvent])
    )

    // 每来一条订单事件，调用一次
    override def processElement1(value: OrderEvent,
                                 ctx: CoProcessFunction[OrderEvent, PayEvent, String]#Context,
                                 out: Collector[String]): Unit = {
      val pay = payState.value()

      if (pay != null) { // 说明相同order-id的pay事件已经到了
        payState.clear()
        // value.orderId == pay.orderId
        out.collect("订单ID为：" + value.orderId + "的实时对账成功了！")
      } else { // 如果payState中为空，说明相同order-id的pay事件还没来
        orderState.update(value) // 保存当前来的订单，等待pay事件
        ctx.timerService().registerEventTimeTimer(value.eventTime.toLong * 1000 + 5000L)
      }
    }

    // 每来一条支付事件，调用一次
    override def processElement2(value: PayEvent,
                                 ctx: CoProcessFunction[OrderEvent, PayEvent, String]#Context,
                                 out: Collector[String]): Unit = {
      val order = orderState.value()

      if (order != null) {
        orderState.clear()
        out.collect("订单ID为：" + value.orderId + "的实时对账成功了！")
      } else {
        payState.update(value)
        ctx.timerService().registerEventTimeTimer(value.eventTime.toLong * 1000 + 5000L)
      }
    }

    override def onTimer(timestamp: Long,
                         ctx: CoProcessFunction[OrderEvent, PayEvent, String]#OnTimerContext,
                         out: Collector[String]): Unit = {
      if (payState.value != null) {
        ctx.output(unmatchedPays, "订单ID为：" + payState.value.orderId + "的实时对账失败了！")
      }
      if (orderState.value != null) {
        ctx.output(unmatchedOrders, "订单ID为：" + orderState.value.orderId + "的实时对账失败了！")
      }
      payState.clear()
      orderState.clear()
    }
  }
}
```

### 0606 163000

```scala
package com.atguigu.project

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object TwoStreamsIntervalJoin {
  // 订单事件
  case class OrderEvent(orderId: String,
                        eventType: String,
                        eventTime: String)

  // 支付事件
  case class PayEvent(orderId: String,
                      eventType: String,
                      eventTime: String)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 服务器端数据库的支付信息
    val orders = env
      .fromElements(
        OrderEvent("1", "pay", "4"),
        OrderEvent("2", "pay", "5"),
        OrderEvent("3", "pay", "9"),
        OrderEvent("5", "pay", "10")
      )
      .assignAscendingTimestamps(_.eventTime.toLong * 1000L)
      .keyBy(_.orderId)

    // 远程的支付信息
    val pays = env
      .fromElements(
        PayEvent("1", "weixin", "7"),
        PayEvent("2", "zhifubao", "8"),
        PayEvent("4", "zhifubao", "10"),
        PayEvent("5", "zhifubao", "20")
      )
      .assignAscendingTimestamps(_.eventTime.toLong * 1000L)
      .keyBy(_.orderId)

    orders
      .intervalJoin(pays)
      .between(Time.seconds(-5),Time.seconds(5))
      .process(new MyIntervalJoin)
      .print()

    env.execute()
  }

  class MyIntervalJoin extends ProcessJoinFunction[OrderEvent, PayEvent, String] {
    override def processElement(left: OrderEvent,
                                right: PayEvent,
                                context: ProcessJoinFunction[OrderEvent, PayEvent, String]#Context,
                                out: Collector[String]
                               ): Unit = {
      out.collect(left +" =Interval Join=> "+right)
    }
  }
}
```

### 0606 164100

```scala
package com.atguigu.project

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object WindowJoinExmaple {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val orangeStream = env
      .fromElements(
        (1, 1000L),
        (2, 2000L)
      )
      .assignAscendingTimestamps(_._2)

    val greenStream = env
      .fromElements(
        (1, 1200L),
        (1, 1500L),
        (2, 2000L)
      )
      .assignAscendingTimestamps(_._2)

    orangeStream
      .join(greenStream)
      .where(_._1)
      .equalTo(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .apply { (e1, e2) => e1 + "," + e2 }
      .print()

    env.execute()
  }
}
```

