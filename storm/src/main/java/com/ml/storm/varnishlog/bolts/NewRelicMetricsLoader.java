package com.ml.storm.varnishlog.bolts;

import java.util.Map;

import com.newrelic.api.agent.NewRelic;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class NewRelicMetricsLoader implements IBasicBolt{

	@Override
	public void cleanup() {
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String groupEntityName = input.getValueByField("groupEntityName").toString();
		String groupEntity = input.getValueByField("groupEntity").toString();
		String metricGroup = input.getValueByField("metricGroup").toString();
		String metricName = input.getValueByField("metricName").toString();
		Number counter = (Number) input.getValueByField("value");
		
		StringBuffer customMetricName = new StringBuffer();
		customMetricName.append("/Custom/");
		customMetricName.append(groupEntityName);
		customMetricName.append("/");
		customMetricName.append(groupEntity);
		customMetricName.append("/");
		customMetricName.append(metricGroup);
		customMetricName.append("/");
		customMetricName.append(metricName);
		
		if("responseTime".equals(metricGroup)){
			NewRelic.recordResponseTimeMetric(customMetricName.toString(), counter.longValue());
		}else{
			NewRelic.incrementCounter(customMetricName.toString(), counter.intValue());
		}
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
