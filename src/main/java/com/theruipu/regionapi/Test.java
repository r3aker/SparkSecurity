package com.theruipu.regionapi;


import java.io.IOException;
import java.util.Arrays;

public class Test {
	private static final long serialVersionUID = 8533489548835413763L;

	public static void main(String[] args) {
		

		String geoIPFile = "D:\\GeoLiteCity.dat";
		
		
		try {
            City city = new City("D:\\17monipdb\\17monipdb.datx"); // 城市库

            System.out.println(Arrays.toString(city.find("180.76.76.76")));
            System.out.println(city.find("106.38.1.117")[2]);
            System.out.println(Arrays.toString(city.find("106.39.84.163")));
            System.out.println(Arrays.toString(city.find("111.196.240.178")));
            System.out.println(Arrays.toString(city.find("111.196.240.227")));
            System.out.println(Arrays.toString(city.find("111.196.241.180")));
            System.out.println(Arrays.toString(city.find("111.196.241.220")));
            System.out.println(Arrays.toString(city.find("111.196.241.249")));
            System.out.println(Arrays.toString(city.find("111.196.245.149")));
            System.out.println(Arrays.toString(city.find("111.40.217.120")));
            System.out.println(Arrays.toString(city.find("111.63.3.184")));
            System.out.println(Arrays.toString(city.find("111.63.3.191")));
            System.out.println(Arrays.toString(city.find("118.144.129.1")));
            System.out.println(Arrays.toString(city.find("121.22.249.85")));
            System.out.println(Arrays.toString(city.find("122.114.245.19")));
            System.out.println(Arrays.toString(city.find("123.180.88.114")));
            System.out.println(Arrays.toString(city.find("123.180.90.139")));
            System.out.println(Arrays.toString(city.find("123.180.91.118")));
            System.out.println(Arrays.toString(city.find("218.206.217.147")));
            System.out.println(Arrays.toString(city.find("218.98.53.96")));
            System.out.println(Arrays.toString(city.find("47.93.244.220")));
            System.out.println(Arrays.toString(city.find("61.135.194.49")));
            System.out.println(Arrays.toString(city.find("61.149.11.12")));
            System.out.println(Arrays.toString(city.find("61.149.11.13")));

           System.out.println("----------------------------------");
            
            
            System.out.println(Arrays.toString(city.find("106.39.84.163")));
            System.out.println(Arrays.toString(city.find("111.40.217.121")));
            System.out.println(Arrays.toString(city.find("111.40.217.123")));
            System.out.println(Arrays.toString(city.find("116.255.132.12")));
            System.out.println(Arrays.toString(city.find("121.22.249.84")));
            System.out.println(Arrays.toString(city.find("121.22.249.85")));
            System.out.println(Arrays.toString(city.find("218.206.217.144")));
            System.out.println(Arrays.toString(city.find("218.206.217.146")));
            

        } catch (IOException ioex) {
            ioex.printStackTrace();
        } catch (IPv4FormatException ipex) {
            ipex.printStackTrace();
        }
    }
	
}


