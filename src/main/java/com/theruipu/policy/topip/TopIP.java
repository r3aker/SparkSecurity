package com.theruipu.policy.topip;

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
import java.util.Date;
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

public class TopIP implements Serializable {
	private static final long serialVersionUID = 523113L;

	public void Count()  throws IOException, SQLException {

		// SparkConf conf = new SparkConf().setAppName("WordCount-Region");
		//SparkConf conf = new SparkConf().setAppName("TopIP").setMaster("local");
		
		String Environment = Getconfig.Getproperty("env");
		SparkConf conf = new SparkConf();
		if(Environment.equals("PROD")) {
			conf.setAppName("TopIP");
		}
		else {
			conf.setAppName("TopIP").setMaster("local");
		}

		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines;
		// 读取日志
		if (Environment.equals("PROD")) {
			lines = sc.textFile(Getconfig.Getproperty("hadoopfsurl") + Getlogdir.getdir(), 1);
		} else {
			lines = sc.textFile(Getconfig.Getproperty("hadoopfsurl") + Getconfig.Getproperty("testlogdir"), 1);
		}

		JavaPairRDD<String, Integer> IPandNumber1 = lines.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String s) {
				AccessLog tmp = new AccessLog(s);
				if (tmp.Success == 2) {
					return new Tuple2<String, Integer>(tmp.getClientIP(), 1);
				} else {
					return new Tuple2<String, Integer>("1.1.1.1", 1);
				}
			}

		});
		
		
		JavaPairRDD<String, Integer> IPandNumber1Filter = IPandNumber1.filter(new Function<Tuple2<String, Integer>, Boolean>() {
			private static final long serialVersionUID = 1L;
			public Boolean call(Tuple2<String, Integer> s) {
				if (s._1().equals("1.1.1.1")) {
					return false;
				} else {
					return true;
				}
			}

		});
		
		

		JavaPairRDD<String, Integer> IPAndNumber = IPandNumber1Filter
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}
				});

		JavaPairRDD<String, Integer> bsbsbs = IPAndNumber.filter(new Function<Tuple2<String, Integer>, Boolean>() {
			public Boolean call(Tuple2<String, Integer> s) throws Exception {
				if (s._2 > 1000) {
					return true;
				} else {
					return false;
				}
			}
		});

		
		
		JavaPairRDD<Integer, String> wordtop1 = bsbsbs
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

		String Timestr= TimeTranslate.DatetimetoString(new Date());
		String project= Getconfig.Getproperty("project");
		long size=50;
		if(last.count()<50) {
			size=last.count();
		}
		for (Tuple2<String, Integer> a : last.take((int)size)) {
			MysqlUtils.InsertTopIP(project,Timestr,a._1(),a._2());
	
		}

		sc.close();
	}

}
