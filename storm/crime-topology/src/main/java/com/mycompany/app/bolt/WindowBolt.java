package com.mycompany.app.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Sliding window counter using manual time tracking.
 * Counts crime events per district over a 5-minute sliding window.
 * Implements IRichBolt for consistency with other bolts.
 */
public class WindowBolt implements IRichBolt {
    private static final long WINDOW_MS = 5 * 60 * 1000;  // 5 minutes
    private static final long SLIDE_MS = 60 * 1000;        // Emit every 1 minute
    
    private OutputCollector collector;
    
    // district -> queue of event timestamps
    private final Map<String, Queue<Long>> districtWindows = new ConcurrentHashMap<>();
    private final Map<String, Long> lastEmitTime = new ConcurrentHashMap<>();

    @Override
    public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        try {
            String district = input.getStringByField("district");
            long now = System.currentTimeMillis();
            
            // Add current event timestamp
            districtWindows
                .computeIfAbsent(district, k -> new ConcurrentLinkedQueue<>())
                .add(now);
            
            // Remove expired events (older than window)
            Queue<Long> window = districtWindows.get(district);
            if (window != null) {
                long cutoff = now - WINDOW_MS;
                window.removeIf(ts -> ts < cutoff);
                
                // Emit count periodically per district
                long lastEmit = lastEmitTime.getOrDefault(district, 0L);
                if (now - lastEmit >= SLIDE_MS) {
                    int count = window.size();
                    collector.emit(input, new Values(district, count, now));
                    lastEmitTime.put(district, now);
                }
            }
            collector.ack(input);
        } catch (Exception e) {
            e.printStackTrace();
            collector.fail(input);
        }
    }

    @Override
    public void cleanup() {}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("district", "count", "window_end_ts"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
