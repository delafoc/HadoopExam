package com.shixun_mapreduce;

public class demo {
    public static void main(String[] args) {
        String str = "2009/1/17, Yeezy,Adidas-Yeezy-Boost-350-Low-V2-Beluga,\"$1,097 \",$220 ,9/24/16,11,California";
        String[] split = str.split(",");

        for (String s : split) {
            System.out.println(s);
        }
        System.out.println(split[4] + split[5]);

        System.out.println(str.split(",").length);



    }
}
