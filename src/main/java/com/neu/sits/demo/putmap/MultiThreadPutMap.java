package com.neu.sits.demo.putmap;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author gaofeihang
 * @since Oct 21, 2014
 *
 */
public class MultiThreadPutMap {
    
    private String fileName;
    private int batchSize;
    private int threadNum;
    
    private BufferedReader reader;
    private Map<String, String> map = new ConcurrentHashMap<String, String>();
    
    private ExecutorService executorService = Executors.newCachedThreadPool();
    private CountDownLatch latch;
    
    private int processCount = 0;
    
    public MultiThreadPutMap(String[] args) {
        fileName = args[0];
        batchSize = Integer.valueOf(args[1]);
        threadNum = Integer.valueOf(args[2]);
        
        latch = new CountDownLatch(threadNum);
        
        long start = System.currentTimeMillis();
        
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), "UTF-8"));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
        
        for (int i = 0; i < threadNum; i++) {
            executorService.execute(new Runnable() {
                
                @Override
                public void run() {
                    boolean finished = false;
                    while (!finished) {
                        List<String> lines = getLines();
                        for (String line : lines) {
                            String[] parts = line.split("\t");
                            map.put(parts[0], parts[1]);
                        }
                        
                        if (lines.isEmpty()) {
                            latch.countDown();
                            finished = true;
                        }
                    }
                }
            });
        }
        
        try {
            latch.await(3600, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
        try {
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        executorService.shutdown();
        
        System.out.println(map.keySet().size());
        System.out.println("cost " + (System.currentTimeMillis() - start)  + "ms");
    }
    
    private synchronized List<String> getLines() {
        List<String> lines = new ArrayList<String>();
        int count = 0;
        String line = null;
        
        try {
            while (count < batchSize) {
                line = reader.readLine();
                if (line == null) {
                    break;
                }
                lines.add(line);
                count++;
                processCount++;
                if (processCount % (batchSize * 10) == 0) {
                    System.out.println(processCount);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        return lines;
    }
    
    public static void main(String[] args) {
        args = new String[3];
        args[0] = "bigfile";
        args[1] = "10000";
        args[2] = "30";
        
        new MultiThreadPutMap(args);
    }

}
