package com.ml.storm.varnishlog.topologies;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift7.TProcessor;
import org.apache.thrift7.TProcessorFactory;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.transport.TServerSocket;
import org.apache.thrift7.transport.TTransport;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.ml.storm.varnishlog.bolts.JSONParserBolt;
import com.ml.storm.varnishlog.bolts.NewRelicMetricsLoader;
import com.ml.storm.varnishlog.bolts.VarnishHadoopSaver;
import com.ml.storm.varnishlog.bolts.VarnishMessageCreator;
import com.ml.storm.varnishlog.bolts.VarnishMetricsExtractor;
import com.ml.storm.varnishlog.spouts.VarnishReaderSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class Topology {

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, FileNotFoundException, IOException, ParseException {
		TopologyBuilder topology = new TopologyBuilder();
		Config conf = new Config();
		
		JSONParser p = new JSONParser();
		JSONObject json = (JSONObject)p.parse(new FileReader(args[0]));
		conf.putAll(json);
		int spouts = ((Number) json.get("spouts")).intValue();
		int workers = ((Number) json.get("workers")).intValue();
		
		int parsers = checkValue(((Number) json.get("parsers")).intValue());
		int messageCreater = checkValue(((Number)json.get("messageCreators")).intValue());
		int savers = checkValue(messageCreater);
		
		conf.setNumWorkers(workers);
		conf.setNumAckers(3);
		
		Logger.getLogger("httpclient").setLevel(Level.ERROR);
		
		topology.setSpout("varnishlog-spout", new VarnishReaderSpout(), spouts);

		topology.setBolt("json-parser", new JSONParserBolt(),parsers)
			.shuffleGrouping("varnishlog-spout");
		
			
		topology.setBolt("varnish-message-creator", new VarnishMessageCreator(),messageCreater)
			.shuffleGrouping("json-parser");
		
		topology.setBolt("varnish-hadoop-saver", new VarnishHadoopSaver(),savers)
			.shuffleGrouping("varnish-message-creator");
		
		topology.setBolt("varnish-metrics-extractor", new VarnishMetricsExtractor()).
			shuffleGrouping("varnish-message-creator");
		
		topology.setBolt("new-relic-emiter", new NewRelicMetricsLoader()).
			shuffleGrouping("varnish-metrics-extractor");
		

		if(json.containsKey("debug") && (Boolean)json.get("debug")){
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test1", conf, topology.createTopology());
		}else{
			StormSubmitter.submitTopology(json.get("topologyName").toString(),conf, topology.createTopology());
		}
	}

	private static int checkValue(int i) {
		return i>0?i:1;
	}
}
