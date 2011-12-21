package com.ml.storm.varnishlog.topologies;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

import com.ml.storm.varnishlog.bolts.BackendCollector;
import com.ml.storm.varnishlog.bolts.BackendMessageFilter;
import com.ml.storm.varnishlog.bolts.ClientMessageFilter;
import com.ml.storm.varnishlog.bolts.LogCollector;
import com.ml.storm.varnishlog.bolts.VarnishHadoopSaver;
import com.ml.storm.varnishlog.bolts.VarnishLineParser;
import com.ml.storm.varnishlog.bolts.VarnishMessageCreator;
import com.ml.storm.varnishlog.spouts.VarnishReaderSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class Topology {

	public static void main(String[] args) {
		TopologyBuilder topology = new TopologyBuilder();
		Map<String, Integer> hosts = new HashMap<String, Integer>();
		int i=0;
		for(String str : args[i].split(",")){
			String[] h = str.split(":");
			hosts.put(h[0], Integer.parseInt(h[1]));
		}
		
		Config conf = new Config();
		conf.put("namenodeUrl", args[++i]);
		conf.put("hadoopPath", args[++i]);
		conf.put("intervalInMinutes", Long.parseLong(args[++i]));
		
		int spouts = checkValue(hosts.size());
		int parsers = checkValue(spouts);
		int filter = checkValue(parsers / 2);
		int collector = checkValue(hosts.size());
		int messageCreater = checkValue(collector / 2);
		int savers = checkValue(messageCreater);
		
		Logger.getLogger("httpclient").setLevel(Level.ERROR);
		topology.setSpout("varnishlog-spout", new VarnishReaderSpout(hosts), spouts);
		
		topology.setBolt("varnish-line-parser", new VarnishLineParser(),parsers)
			.fieldsGrouping("varnishlog-spout", new Fields("host","session"));

		topology.setBolt("varnish-client-filter", new ClientMessageFilter(),filter)
		.fieldsGrouping("varnish-line-parser", new Fields("host","session"));
		
		topology.setBolt("varnish-backend-filter", new BackendMessageFilter(), filter)
			.fieldsGrouping("varnish-line-parser", new Fields("host","session"));
		
		
		topology.setBolt("varnish-backend-collector", new BackendCollector(),collector)
			.fieldsGrouping("varnish-backend-filter", new Fields("host","session"));
		
		topology.setBolt("varnish-log-collector", new LogCollector(),collector)
			.fieldsGrouping("varnish-backend-collector", new Fields("host","session"))
			.fieldsGrouping("varnish-client-filter", new Fields("host","session"));
			
		topology.setBolt("varnish-message-creator", new VarnishMessageCreator(),messageCreater)
			.shuffleGrouping("varnish-log-collector");
		
		topology.setBolt("varnish-hadoop-saver", new VarnishHadoopSaver(),savers)
			.shuffleGrouping("varnish-message-creator");
		
		LocalCluster cluster = new LocalCluster();
		
		cluster.submitTopology("test", conf, topology.createTopology());
		
		
	}

	private static int checkValue(int i) {
		return i>0?i:1;
	}
}
