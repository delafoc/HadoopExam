package com.hive;

import java.util.HashMap;

public class demo {
    public static void main(String[] args) {
        HashMap<String, String> map = new HashMap<>();
        map.put("男生", "qqq");
        map.remove("男生");
        System.out.println(map.get("男生"));
    }
}
