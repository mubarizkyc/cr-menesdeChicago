package com.mycompany.app.bolt;

import com.mycompany.app.model.Crime;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.Map;

public class ParseBolt implements IRichBolt {
    private OutputCollector collector;
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        try {
            // KafkaSpout default emits fields: "key", "value"
            String json = input.getStringByField("value");
            
            Crime crime = mapper.readValue(json, Crime.class);
            
            if (crime.getDistrict() == null || crime.getDistrict().isEmpty()) {
                collector.fail(input);
                return;
            }
            
            // Normalize district: "16" -> "016"
            String normalizedDistrict = String.format("%03d", Integer.parseInt(crime.getDistrict()));
            
            collector.emit(input, new Values(crime, normalizedDistrict));
            collector.ack(input);
        } catch (Exception e) {
            e.printStackTrace();
            collector.fail(input);
        }
    }

    @Override public void cleanup() {}
    
    @Override 
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("crime", "district"));
    }
    
    @Override 
    public Map<String, Object> getComponentConfiguration() { 
        return null; 
    }
}
