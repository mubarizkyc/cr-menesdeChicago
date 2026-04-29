package com.mycompany.app;

import com.mycompany.app.bolt.*;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;  // ← Added import

public class CrimeTopology {
    private static final String KAFKA_BOOTSTRAP = "localhost:9092";
    private static final String TOPIC = "crime_events";  // ✅ Your actual topic
    private static final String GROUP_ID = "crime-storm-group";

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = createTopologyBuilder();
        Config config = new Config();
        config.setDebug(true);
        config.put("anomaly.threshold", 15);
        config.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 60);
        config.setNumWorkers(1);

        if (args.length > 0 && args[0].equals("cluster")) {
            // Cluster mode
            StormSubmitter.submitTopology(args.length > 1 ? args[1] : "crime-prod", config, builder.createTopology());
            System.out.println("✅ Submitted to cluster");
        } else {
            // Local mode using Utils.simulateTopology (NO LocalCluster needed!)
            System.out.println("🧪 Running topology locally...");
            System.out.println("📡 Kafka topic: " + TOPIC);
            System.out.println("🚨 Press Ctrl+C to stop");
            
            // This runs the topology without needing LocalCluster class
            Utils.simulateTopology(builder.createTopology(), config);
        }
    }
    
    private static TopologyBuilder createTopologyBuilder() {
        TopologyBuilder builder = new TopologyBuilder();

        KafkaSpoutConfig<String, String> spoutConfig = KafkaSpoutConfig.builder(KAFKA_BOOTSTRAP, TOPIC)
            .setProp("group.id", GROUP_ID)
            .setProp("auto.offset.reset", "earliest")
            .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE)
            .build();
        
        builder.setSpout("kafka_spout", new KafkaSpout<>(spoutConfig), 2);
        builder.setBolt("parse_bolt", new ParseBolt(), 2).shuffleGrouping("kafka_spout");
        builder.setBolt("district_bolt", new DistrictBolt(), 2).fieldsGrouping("parse_bolt", new Fields("district"));
        builder.setBolt("window_bolt", new WindowBolt(), 2).fieldsGrouping("district_bolt", new Fields("district"));
        builder.setBolt("anomaly_bolt", new AnomalyBolt(), 1).shuffleGrouping("window_bolt");
        
        return builder;
    }
}
