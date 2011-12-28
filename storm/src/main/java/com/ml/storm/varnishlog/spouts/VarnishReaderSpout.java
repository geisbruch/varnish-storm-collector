package com.ml.storm.varnishlog.spouts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeperMain;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.mortbay.log.Log;

import com.ml.storm.varnishlog.dto.VarnishLog;


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class VarnishReaderSpout implements IRichSpout{

	static Logger LOG = Logger.getLogger(VarnishReaderSpout.class); 
	
	private static final String QUEUE_NAME = "storm-varnish-spout";

	private static final Integer COLLECTOR_PORT = 1337;

	Queue<VarnishLog> queue = new ConcurrentLinkedQueue<VarnishLog>();
	
	private SpoutOutputCollector collector;

	Map<String,LogsClient> readers = new HashMap<String, LogsClient>();

	List<String> childs = new ArrayList<String>();
	
	private String zkPath;
	
	Integer clients = 0;
	
	
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

	public void open(final Map conf, final TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		this.zkPath = (String) conf.get("zkPath");
		try {
			final ZooKeeper zk = new ZooKeeper((String) conf.get("zkHosts"),60000,new NullWatcher());
			
			if(zk.exists(this.zkPath, false) == null){
				zk.create(this.zkPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
			childs.addAll(zk.getChildren(zkPath, new Watcher() {
				@Override
				public void process(WatchedEvent event) {
					try{
							Stat stat = new Stat();
							List<String> newChilds = zk.getChildren(event.getPath(), this);
							
							ArrayList<String> deleted = new ArrayList<String>(childs);
							
							//Get deleted items
							deleted.removeAll(newChilds);
							for(String str : deleted){
								destroyLoader(str);
							}
							
							//Get added items
							newChilds.removeAll(childs);
							for(String str : newChilds){
								createLoader(str, context);
							}
					}catch(Exception e){
						throw new RuntimeException("Error updating nodes",e);
					}
				}
			}));
			for(Object child : childs.toArray()){
				createLoader(child.toString(), context);
			}
		} catch (Exception e) {
			throw new RuntimeException("Error connecting to zookeeper",e);
		} 
		
	}

	
	
	private void createLoader(String host,TopologyContext context) {
		LOG.info("Adding connection connection to ["+host+"]");
		childs.add(host);
		LogsClient client = new LogsClient(queue, host, COLLECTOR_PORT, QUEUE_NAME);
		Thread t = new Thread(client,"Spout_"+context.getThisTaskId()+"_Client_"+(clients++));
		t.start();
		readers.put(host, client);
	}

	private void destroyLoader(String host){
		LOG.info("Removing connection to ["+host+"]");
		LogsClient t = readers.get(host);
		if(t != null)
			t.setFinished();
		childs.remove(host);
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("host","session","line"));
	}

}
