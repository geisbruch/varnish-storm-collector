package com.ml.storm.varnishlog.spouts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.DataTree;

public class test {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		final ZooKeeper zk = new ZooKeeper("127.0.0.1:2182",1000,new NullWatcher());
		String zkPath = "/varnish";
		if(zk.exists(zkPath, false) == null){
			zk.create(zkPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		
		//Monitor Path modifications
		final List<String> childs = new ArrayList<String>();
		childs.addAll(zk.getChildren(zkPath, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				try{
						Stat stat = new Stat();
						List<String> newChilds = zk.getChildren(event.getPath(), this);
						
						ArrayList<String> deleted = new ArrayList<String>(childs);
						deleted.removeAll(newChilds);
						
						
						System.out.println("Deleted");
						for(String str : deleted){
							childs.remove(str);
							System.out.println(str);
						}
						
						newChilds.removeAll(childs);
						
						System.out.println("Added");
						for(String str : newChilds){
							childs.add(str);
							System.out.println(str);
						}
				}catch(Exception e){
					throw new RuntimeException("Error updating nodes",e);
				}
			}
		}));
		
		for(String child : childs){
			Stat stat = null;
			System.out.println(new String(zk.getData(zkPath+"/"+child, null, stat)));
		}
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				while(true){
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
			}
		}).start();
	}
}
