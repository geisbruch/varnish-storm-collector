package com.ml.storm.varnishlog.bolts;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public abstract class MessageFilter implements IBasicBolt {

	@Override
	public void cleanup() {

	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		if(this.passTuple(input))
			collector.emit(input.getValues());
	}

	protected abstract boolean passTuple(Tuple input);

	@Override
	public void prepare(Map stormConf, TopologyContext context) {}

}
