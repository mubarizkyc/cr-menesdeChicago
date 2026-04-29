package com.mycompany.app.bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class DistrictBolt implements IRichBolt {
    private OutputCollector collector;

    @Override 
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override 
    public void execute(Tuple input) {
        Object crime = input.getValueByField("crime");
        String district = input.getStringByField("district");
        collector.emit(input, new Values(crime, district));
        collector.ack(input);
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
