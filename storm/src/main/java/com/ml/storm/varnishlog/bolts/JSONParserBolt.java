package com.ml.storm.varnishlog.bolts;

import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class JSONParserBolt implements IBasicBolt{

	
	@Override
	public void cleanup() {
		
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String host = input.getStringByField("host");
		String message = input.getStringByField("line");
		JSONObject messageObj;
		try {
			messageObj = (JSONObject)JSONUtils.parseJson(message);
			messageObj.put("host", host);
			collector.emit(new Values(messageObj));
		} catch (Exception e) {
			collector.getOutputter().fail(input);
		}
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("message"));
	}

}
