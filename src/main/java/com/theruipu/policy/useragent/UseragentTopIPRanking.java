package com.theruipu.policy.useragent;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import com.theruipu.mysql.MysqlUtils;
import com.theruipu.regionapi.*;
import com.theruipu.utils.Getconfig;
import com.theruipu.utils.Getlogdir;
import com.theruipu.utils.TimeTranslate;
import com.theruipu.utils.logparse.AccessLog;

import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;


public class UseragentTopIPRanking implements Serializable {
	private static final long serialVersionUID = 523113L;

	public void Count() throws IOException, SQLException {

		// SparkConf conf = new
		// SparkConf().setAppName("FiveminuteLoginRanking").setMaster("local");
		String Environment = Getconfig.Getproperty("env");
		SparkConf conf = new SparkConf();
		if (Environment.equals("PROD")) {
			conf.setAppName("FiveminuteLoginRanking");
		} else {
			conf.setAppName("FiveminuteLoginRanking").setMaster("local");
		}

		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines;
		// 读取日志
		if (Environment.equals("PROD")) {
			lines = sc.textFile(Getconfig.Getproperty("hadoopfsurl") + Getlogdir.getdir(), 1);
		} else {
			lines = sc.textFile(Getconfig.Getproperty("hadoopfsurl") + Getconfig.Getproperty("testlogdir"), 1);
		}

		JavaPairRDD<String, String> IPAndUseragent = lines.mapToPair(new PairFunction<String, String, String>() {
			private static final long serialVersionUID = 1L;

			public Tuple2<String, String> call(String s) {
				AccessLog tmp = new AccessLog(s);
				if (tmp.Success == 2) {
					return new Tuple2<String, String>(tmp.getClientIP(), tmp.getUserAgent());
				} else {
					return new Tuple2<String, String>("1.1.1.1", "Useragent101896");
				}
			}

		});

		// 删除不匹配正则的行
		JavaPairRDD<String, String> TimeAndIPFilter = IPAndUseragent
				.filter(new Function<Tuple2<String, String>, Boolean>() {
					private static final long serialVersionUID = 1L;

					public Boolean call(Tuple2<String, String> s) {
						if (s._2().equals("Useragent101896")) {
							return false;
						} else {
							return true;
						}
					}

				});

		JavaPairRDD<String, Iterable<String>> IPAdnUserAgents = TimeAndIPFilter.groupByKey();

		/*
		 * 返回时间+IP key
		 */

		JavaPairRDD<String, Set<String>> IPAndDistinctUserAgentlist = IPAdnUserAgents
				.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, Set<String>>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<String, Set<String>> call(Tuple2<String, Iterable<String>> s) {
						String newip = s._1();
						Set<String> newstring = new HashSet<String>();
						for (String a : s._2()) {
							newstring.add(a);
						}
						return new Tuple2<String, Set<String>>(newip, newstring);
					}
				});

		JavaPairRDD<String, Integer> IPAndUseragentNumber = IPAndDistinctUserAgentlist
				.mapToPair(new PairFunction<Tuple2<String, Set<String>>, String, Integer>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<String, Integer> call(Tuple2<String, Set<String>> s) {

						return new Tuple2<String, Integer>(s._1(), s._2().size());
					}
				});

		Map<String, Integer> IPMap = IPAndUseragentNumber.collectAsMap();

		List<Map.Entry<String, Integer>> wordMap = new ArrayList<Map.Entry<String, Integer>>(IPMap.entrySet());

		Collections.sort(wordMap, new Comparator<Map.Entry<String, Integer>>() {// 根据value排序
			public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
				double result = o2.getValue() - o1.getValue();
				if (result > 0)
					return 1;
				else if (result == 0)
					return 0;
				else
					return -1;
			}
		});

		String Timestr = TimeTranslate.DatetimetoString(new Date());

		for (Map.Entry<String, Integer> entry : wordMap) {
			if (entry.getValue() < 5) {
				break;
			}
			String project = Getconfig.Getproperty("project");
			MysqlUtils.InsertUserAgentTopIP(project, Timestr, entry.getKey(), entry.getValue());

		}

		sc.close();
	}

}
