package wordCount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) {
        // 1. 初始化 Spark 配置
        SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("yarn");

        // 2. 创建 Spark 上下文对象
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 加载数据（从 HDFS 读取文本文件，每行文本作为一个 String 元素，形成 RDD）
        JavaRDD<String> lines = sc.textFile("hdfs://MapReduce:8020/input/data.txt");

        // 4. 转换操作（拆分单词，将每行文本按 \\s+ 拆分成单词，并展平成一个单词集合）（输入是一行文本，输出是多个单词）
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split("\\s+")).iterator());

        // 5. 为每个单词附加初始值1，形成（单词，1）键值对
        JavaPairRDD<String, Integer> wordOnes = words.mapToPair(word -> new Tuple2<>(word, 1));

        // 6. 统计聚合（将相同单词的计数值相加 Integer::sum 等价于 (a, b) -> a + b）
        JavaPairRDD<String, Integer> wordCounts = wordOnes.reduceByKey(Integer::sum);
        // JavaPairRDD<String, Integer> wordCounts = wordOnes.reduceByKey((a, b) -> a + b);

        // 7. 保存输出
        wordCounts.saveAsTextFile("hdfs://MapReduce:8020/spark_output");

        // 8. 关闭资源
        sc.close();
    }
}
