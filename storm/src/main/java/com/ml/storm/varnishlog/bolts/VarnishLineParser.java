package com.ml.storm.varnishlog.bolts;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ml.storm.varnishlog.dto.VarnishMessage;


import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class VarnishLineParser implements IBasicBolt{

	Pattern logRegex = Pattern.compile("(\\d+) ([\\w,_]+).*(c|b) (.*)");
	
	@Override
	public void cleanup() {
		
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String host = input.getStringByField("host");
		String session = input.getStringByField("session");
		VarnishMessage message = parseMessage(input.getStringByField("line"));
		if(message!=null)
			collector.emit(new Values(host,session,message));
	}

	private VarnishMessage parseMessage(String line) {
		Matcher m = logRegex.matcher(line.trim());
		if(m.find()){
			return new VarnishMessage(m.group(1).trim(),m.group(2).trim(),m.group(3).trim(),m.group(4).trim());
		}
		return null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("host","session","message"));
	}

}
