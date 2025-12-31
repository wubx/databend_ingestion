package com.databend;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class GenerateData {
    public static List<Map<String, Object>> genBatch(String batch, int n) {
        List<Map<String, Object>> data = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            Map<String, Object> d = new HashMap<>();
            d.put("id", ThreadLocalRandom.current().nextInt(10_000_000, 2_000_000_000));
            d.put("batch", batch);
            d.put("name", randomString(16));
            d.put("birthday", LocalDateTime.now().toLocalDate().toString());
            d.put("address", randomString(40));
            d.put("company", randomString(40));
            d.put("job", randomString(40));
            d.put("bank", "中国人民银行");
            d.put("password", randomString(20));
            d.put("phone_number", randomString(16));
            d.put("user_agent", randomString(40));
            d.put("c1", randomString(40));
            d.put("c2", randomString(40));
            d.put("c3", randomString(40));
            d.put("c4", randomString(40));
            d.put("c5", randomString(40));
            d.put("c6", randomString(40));
            d.put("c7", randomString(40));
            d.put("c8", randomString(40));
            d.put("c9", randomString(40));
            d.put("c10", randomString(40));
            d.put("d", LocalDateTime.now().toLocalDate().toString());
            d.put("t", LocalDateTime.now().toString());
            data.add(d);
        }
        return data;
    }

    private static String randomString(int len) {
        String chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            int idx = ThreadLocalRandom.current().nextInt(chars.length());
            sb.append(chars.charAt(idx));
        }
        return sb.toString();
    }
}
