package com.ml.storm.varnishlog.bolts;

import java.io.Serializable;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class JSONUtils implements Serializable{

	static JSONParser parser = new JSONParser();
	
	static Object parseJson(String str) throws ParseException{
		return parser.parse(str);
	}
	
}
