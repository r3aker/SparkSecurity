package com.theruipu.policy.login;

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
import com.theruipu.utils.Geturls;
import com.theruipu.utils.TimeTranslate;
import com.theruipu.utils.logparse.AccessLog;

import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/*
 * 
 * 
 */

public class FiveminuteLoginRanking implements Serializable {
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

		JavaRDD<String> linesLogin = lines.filter(new Function<String, Boolean>() {
			public Boolean call(String s) throws Exception {
				
				AccessLog tmp = new AccessLog(s);
				if (tmp.Success == 2) {
					List<String>  loginURL=Geturls.getkeylist("login");
					if(loginURL.contains(tmp.getURL()))
						return true;
					else
						return false;
				} else {
					return false;
				}
				
			}
		});

		JavaPairRDD<Date, String> TimeAndIP = linesLogin.mapToPair(new PairFunction<String, Date, String>() {
			private static final long serialVersionUID = 1L;

			public Tuple2<Date, String> call(String s) {
				AccessLog tmp = new AccessLog(s);
				if (tmp.Success == 2) {
					return new Tuple2<Date, String>(tmp.getTime(), tmp.getClientIP());
				} else {
					return new Tuple2<Date, String>(new Date(), "1.1.1.1");
				}
			}

		});

		// 删除不匹配正则的行
		JavaPairRDD<Date, String> TimeAndIPFilter = TimeAndIP.filter(new Function<Tuple2<Date, String>, Boolean>() {
			private static final long serialVersionUID = 1L;

			public Boolean call(Tuple2<Date, String> s) {
				if (s._2().equals("1.1.1.1")) {
					return false;
				} else {
					return true;
				}
			}

		});

		JavaPairRDD<Date, Iterable<String>> TimeAndIPs = TimeAndIPFilter.groupByKey();

		/*
		 * 返回时间+IP key
		 */

		JavaPairRDD<Date, Set<String>> TimeAndDistinctIPlist = TimeAndIPs
				.mapToPair(new PairFunction<Tuple2<Date, Iterable<String>>, Date, Set<String>>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<Date, Set<String>> call(Tuple2<Date, Iterable<String>> s) {
						Date newdate = s._1();
						Set<String> newstring = new HashSet<String>();
						for (String a : s._2()) {
							newstring.add(a);
						}
						return new Tuple2<Date, Set<String>>(newdate, newstring);
					}
				});

		JavaRDD<String> Time_IP = linesLogin.map(new Function<String, String>() {

			public String call(String s) {
				AccessLog tmp = new AccessLog(s);
				if (tmp.Success == 2) {
					String a = TimeTranslate.DatetimetoString(tmp.getTime()) + "_" + tmp.getClientIP();
					return a;
				} else {
					return "sssss";
				}

			}

		});

		JavaPairRDD<String, Integer> TimeIPNumber1 = Time_IP.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});

		/*
		 * {20180509092500_222.222.44.234,1} 返回每分钟，每个IP的登录次数
		 */
		final JavaPairRDD<String, Integer> TimeIPAndtimes = TimeIPNumber1
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}
				});

		// {20180509092500_222.222.44.234,n}
		// 每分钟，每个IP的登录次数
		final Map<String, Integer> newTime_ip_times = TimeIPAndtimes.collectAsMap();

		Map<String, Integer> ssss = new HashMap<String, Integer>();

		for (Tuple2<Date, Set<String>> a : TimeAndDistinctIPlist.collect()) {

			for (String ip : a._2()) {
				Integer sum = 0;
				for (Integer b = 0; b < 5; b++) {
					Calendar CalendarTime = Calendar.getInstance();
					CalendarTime.setTime(a._1);
					CalendarTime.add(Calendar.MINUTE, b);
					String mytimee = TimeTranslate.DatetimetoString(CalendarTime.getTime());
					String mykey = mytimee + "_" + ip;
					Integer value = newTime_ip_times.get(mykey);
					if (value != null) {
						sum = sum + value;
					}
					if (b == 4) {

						ssss.put(mykey, sum);
					}
				}

			}

		}

		List<Map.Entry<String, Integer>> wordMap = new ArrayList<Map.Entry<String, Integer>>(ssss.entrySet());

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
		long size = 20;
		if (wordMap.size() < 20) {
			size = wordMap.size();
		}
		for (Integer i = 0; i < size; i++) {
			String project = Getconfig.Getproperty("project");
			MysqlUtils.InsertFiveminuteLoginRanking(project, Timestr, wordMap.get(i).getKey(),
					wordMap.get(i).getValue());

		}

		sc.close();
	}

}
