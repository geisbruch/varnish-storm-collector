package com.ml.storm.varnishlog.spouts;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.ml.storm.varnishlog.dto.VarnishLog;


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class VarnishReaderSpout implements IRichSpout{

	
	private static final String QUEUE_NAME = "storm-varnish-spout";

	Queue<VarnishLog> queue = new ConcurrentLinkedQueue<VarnishLog>();
	
	private SpoutOutputCollector collector;

	Map<String,String> hosts;
	
	public VarnishReaderSpout() {
	}
	
	public boolean isDistributed() {
		return true;
	}

	public void ack(Object msgId) {
		
	}

	public void close() {
		
	}

	public void fail(Object msgId) {
		
	}

	public void nextTuple() {
		
		VarnishLog message;
		
		while((message = queue.poll())!=null){
			collector.emit(new Values(message.server,message.session,message.message));
		}
		
		/**
		 * Javadoc recommendation
		 */
		try {
			Thread.sleep(1);
		} catch (InterruptedException e) {}
		
	}

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		this.hosts = (Map<String, String>) conf.get("varnishHosts");
		
		List<Integer> tasks = context.getComponentTasks(context.getThisComponentId());
		
		Integer i = 0;
		for(Map.Entry<String, String> entry : hosts.entrySet()){
			LogsClient client = new LogsClient(queue, entry.getKey(), Integer.parseInt(entry.getValue()), QUEUE_NAME);
			new Thread(client,"Spout_"+context.getThisTaskId()+"_Client_"+(i++)).start();
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("host","session","line"));
	}

}
