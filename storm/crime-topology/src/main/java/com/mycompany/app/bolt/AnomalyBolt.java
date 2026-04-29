package com.mycompany.app.bolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class AnomalyBolt implements IRichBolt {
    private OutputCollector collector;
    private int threshold = 10;

    @Override
    public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        Object cfg = stormConf.get("anomaly.threshold");
        if (cfg instanceof Number) {
            this.threshold = ((Number) cfg).intValue();
        }
    }

    @Override
    public void execute(Tuple input) {
        try {
            String district = input.getStringByField("district");
            Integer count = input.getIntegerByField("count");
            Long windowEndTs = input.getLongByField("window_end_ts");
            
            if (count > threshold) {
                Map<String, Object> alert = new HashMap<>();
                alert.put("type", "CRIME_ANOMALY");
                alert.put("district", district);
                alert.put("count", count);
                alert.put("threshold", threshold);
                alert.put("timestamp", windowEndTs);
                alert.put("message", "High crime in district " + district + ": " + count + " events");
                
                System.out.println("🚨 ANOMALY: " + alert);
                collector.emit(input, new Values(alert));
            }
            collector.ack(input);
        } catch (Exception e) {
            e.printStackTrace();
            collector.fail(input);
        }
    }

    @Override public void cleanup() {}
    
    @Override 
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("alert"));
    }
    
    @Override 
    public Map<String, Object> getComponentConfiguration() { 
        return null; 
    }
}
