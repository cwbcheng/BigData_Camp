# 第一题
[代码](./src/main/scala/App.scala)

[jar 包](./one.jar)

```scala
import org.apache.spark._
import org.apache.spark.sql.SparkSession
object App extends Serializable {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("yarn")
    conf.set("spark.app.name", "one")
    val sc = new SparkContext(conf)
    val files = sc.wholeTextFiles("/home/student3/chengwb/files")

    val r = files.
      flatMap(t => t._2.split(" ").map(m => (m, t._1.split("/").reverse(0)))).
      groupByKey().
      map(t => (t._1, t._2.toList.groupBy(k => k).map(k => (k._1, k._2.length)))).
      sortByKey().collect().toMap
    print(r)
  }
}
```
## 结果
![avatar](./one.png)