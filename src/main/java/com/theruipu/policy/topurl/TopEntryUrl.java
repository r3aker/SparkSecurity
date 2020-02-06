package com.theruipu.policy.topurl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


import java.io.Serializable;
import java.sql.SQLException;

import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.theruipu.mysql.MysqlUtils;
import com.theruipu.utils.Getconfig;
import com.theruipu.utils.Getlogdir;
import com.theruipu.utils.TimeTranslate;
import com.theruipu.utils.logparse.AccessLog;

import scala.Tuple2;



public class TopEntryUrl implements Serializable {
	private static final long serialVersionUID = 523113L;

	public void Count() throws SQLException {

		// SparkConf conf = new SparkConf().setAppName("TOPUrl").setMaster("local");

		String Environment = Getconfig.Getproperty("env");
		SparkConf conf = new SparkConf();
		if (Environment.equals("PROD")) {
			conf.setAppName("TopEntryUrl");
		} else {
			conf.setAppName("TopEntryUrl").setMaster("local");
		}

		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines;
		// 读取日志
		if (Environment.equals("PROD")) {
			lines = sc.textFile(Getconfig.Getproperty("hadoopfsurl") + Getlogdir.getdir(), 1);
		} else {
			lines = sc.textFile(Getconfig.Getproperty("hadoopfsurl") + Getconfig.Getproperty("testlogdir"), 1);
		}

		// refer 为空
		// refer 为其他域名
		JavaPairRDD<String, Integer> IPandNumber1 = lines.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String s) {
				AccessLog tmp = new AccessLog(s);
				if (tmp.Success == 2) {
					if (tmp.getRefer().equals("\"-\"")) {
						return new Tuple2<String, Integer>(tmp.getURL(), 1);
					} else if (!tmp.getRefer().contains("www.theruipu.com")) {
						return new Tuple2<String, Integer>(tmp.getURL(), 1);
					} else {
						return new Tuple2<String, Integer>("/test.ssss101586ssshtml", 1);
					}
				} else {
					return new Tuple2<String, Integer>("/test.ssss101586ssshtml", 1);
				}
			}

		});

		JavaPairRDD<String, Integer> IPandNumber1Filter = IPandNumber1
				.filter(new Function<Tuple2<String, Integer>, Boolean>() {
					private static final long serialVersionUID = 1L;

					public Boolean call(Tuple2<String, Integer> s) {
						if (s._1().equals(("/test.ssss101586ssshtml"))) {
							return false;
						} else {
							return true;
						}
					}

				});



		JavaPairRDD<String, Integer> URLAndNumber = IPandNumber1Filter
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}
				});

		JavaPairRDD<Integer, String> wordtop1 = URLAndNumber
				.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
					public Tuple2<Integer, String> call(Tuple2<String, Integer> item) throws Exception {
						return item.swap();
					}
				});

		JavaPairRDD<Integer, String> wordtop = wordtop1.sortByKey(false);

		JavaPairRDD<String, Integer> last = wordtop
				.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
					public Tuple2<String, Integer> call(Tuple2<Integer, String> item) throws Exception {
						return item.swap();
					}
				});

		String Timestr = TimeTranslate.DatetimetoString(new Date());
		String project = Getconfig.Getproperty("project");
		long size = 50;
		if (last.count() < 50) {
			size = last.count();
		}
		for (Tuple2<String, Integer> a : last.take((int) size)) {
			MysqlUtils.InsertTopEntryUrl(project, Timestr, a._1(), a._2());
		}

		sc.close();
	}

}
