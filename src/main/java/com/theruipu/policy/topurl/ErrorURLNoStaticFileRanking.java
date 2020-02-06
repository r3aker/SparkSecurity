package com.theruipu.policy.topurl;



import java.io.IOException;
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



/*
 * 错误URL排名
 * 
 * 
 */

public class ErrorURLNoStaticFileRanking implements Serializable {
	private static final long serialVersionUID = 523113L;

	public void Count() throws SQLException {

		// 
		String Environment = Getconfig.Getproperty("env");
		SparkConf conf = new SparkConf();
		if(Environment.equals("PROD")) {
			conf.setAppName("ErrorURLRanking");
		}
		else {
			conf.setAppName("ErrorURLRanking").setMaster("local");
		}
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		//JavaRDD<String> lines = sc.textFile(Getconfig.Getproperty("hadoopfsurl") + Getlogdir.getdir(), 1);
		JavaRDD<String> lines;
		// 读取日志
		if (Environment.equals("PROD")) {
			lines = sc.textFile(Getconfig.Getproperty("hadoopfsurl") + Getlogdir.getdir(), 1);
		} else {
			lines = sc.textFile(Getconfig.Getproperty("hadoopfsurl") + Getconfig.Getproperty("testlogdir"), 1);
		}

		// URL and status <URL,status>
		// NO distinct
		JavaPairRDD<String, Integer> URLAndStatus = lines.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String s) {
				AccessLog tmp = new AccessLog(s);
				if (tmp.Success == 2) {
					return new Tuple2<String, Integer>(tmp.getURL(), tmp.getStatus());
				} else {
					return new Tuple2<String, Integer>("/test.sssssshtml", 200);
				}
			}

		});

		
		
		JavaPairRDD<String, Integer> URLAndStatusFilter = URLAndStatus.filter(new Function<Tuple2<String, Integer>, Boolean>() {
			private static final long serialVersionUID = 1L;
			public Boolean call(Tuple2<String, Integer> s) {
				if (s._1().equals("/test.sssssshtml")) {
					return false;
				} 	
				else if (s._1().endsWith(".jpg")) {
					return false;
				}
				else if (s._1().endsWith(".png")) {
					return false;
				} 
				else if (s._1().endsWith(".css")) {
					return false;
				} 
				else if (s._1().endsWith(".js")) {
					return false;
				} 
				else if (s._1().endsWith(".gif")) {
					return false;
				} else {
					return true;
				}
			}

		});
		
		
		
		
		
		JavaPairRDD<String, Integer> ErrorURLAndStatus = URLAndStatusFilter
				.filter(new Function<Tuple2<String, Integer>, Boolean>() {
					private static final long serialVersionUID = 1L;
					public Boolean call(Tuple2<String, Integer> s) {
						if (s._2 > 350) {
							return true;
						} else {
							return false;
						}
					}
				});

		JavaPairRDD<String, Integer> ErrorURLNumber1 = ErrorURLAndStatus
				.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
					public Tuple2<String, Integer> call(Tuple2<String, Integer> s) {
						return new Tuple2<String, Integer>(s._1, 1);
					}

				});

		final JavaPairRDD<String, Integer> ErrorURLTimes = ErrorURLNumber1
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}
				});
		//排序
		JavaPairRDD<Integer, String> NumberAndErrorURL = ErrorURLTimes
				.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {

					public Tuple2<Integer, String> call(Tuple2<String, Integer> item) throws Exception {
						return item.swap();
					}

				});

		JavaPairRDD<Integer, String> NumberAndErrorURLSorted = NumberAndErrorURL.sortByKey(false);

		JavaPairRDD<String, Integer> last = NumberAndErrorURLSorted
				.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {

					public Tuple2<String, Integer> call(Tuple2<Integer, String> item) throws Exception {
						return item.swap();
					}

				});

		
		String Timestr= TimeTranslate.DatetimetoString(new Date());
		long size=50;
		if(last.count()<50) {
			size=last.count();
		}
		
		for (Tuple2<String, Integer> a : last.take((int)size)) {
			String project= Getconfig.Getproperty("project");
			MysqlUtils.InsertErrorURL(project,Timestr,a._1(),a._2());
			
		}

		sc.close();
	}

}
