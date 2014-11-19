package com.neu.sits.demo.putmap;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.UUID;

/**
 * @author yangfengby
 * @since Oct 21, 2014
 *
 */
public class DataGenerator {
    
    public static void main(String[] args) {
        try {
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("bigfile"), "UTF-8"));
            long start = System.currentTimeMillis();
            for (int i = 0; i < 10000000; i++) {
                writer.write(i + "\t" + UUID.randomUUID().toString() + "\n");
                if (i % 1000 == 0) {
                    writer.flush();
                }
            }
            writer.close();
            long end = System.currentTimeMillis();
            System.out.println((end - start) / 1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
