package com.ml.storm.varnishlog.spouts;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class NullWatcher implements Watcher {

	@Override
	public void process(WatchedEvent event) {
	}

}
