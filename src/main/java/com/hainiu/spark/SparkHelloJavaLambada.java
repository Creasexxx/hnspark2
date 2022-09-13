package com.hainiu.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author 苑志朋
 * @version 1.0
 */
public class SparkHelloJavaLambada {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkHelloJava");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> textFile = sc.textFile("");

        //过滤只要包含h的单词
        JavaRDD<String> filter = textFile.filter((String f) -> f.contains("h"));
//                new Function<String, Boolean>() {
//            @Override
//            public Boolean call(String v1) throws Exception {
//                return v1.contains("h");
//            }
//        });


        //这里用ArrayList是因为它可以有方法转到迭代器
        JavaRDD<String> flatMap = filter.flatMap(s -> {
                    ArrayList<String> strings = new ArrayList<>();
                    String[] s1 = s.split(" ");
                    for (String ss : s1) {
                        strings.add(ss);
                    }
                    Iterator<String> iterator = strings.iterator();
                    return iterator;
                }
        );


//                new FlatMapFunction<String, String>() {
//            @Override
//            public Iterator<String> call(String s) throws Exception {
//                ArrayList<String> strings = new ArrayList<>();
//                String[] s1 = s.split(" ");
//                for (String ss : s1) {
//                    strings.add(ss);
//                }
//                Iterator<String> iterator = strings.iterator();
//                return iterator;
//            }
//        });

        //map转换 hsp——(hsp,1)
        JavaRDD<Tuple2<String, Integer>> map = flatMap.map(s -> new Tuple2<String, Integer>(s, 1));

        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> groupBy = map.groupBy(s -> s._1);

        JavaPairRDD<String, Integer> wordCount = groupBy.mapValues(s -> {
            int count = 0;
            Iterator<Tuple2<String, Integer>> iterator = s.iterator();
            while (iterator.hasNext()) {
                iterator.next();
                count++;
            }
            return count;
        });

        JavaPairRDD<String, Integer> cache = wordCount.cache();
        List<Tuple2<String, Integer>> collect = wordCount.collect();
        List<Tuple2<String, Integer>> take = wordCount.take(10);

        System.out.println(take);


    }
}
