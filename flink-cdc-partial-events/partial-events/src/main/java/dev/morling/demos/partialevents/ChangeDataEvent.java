package dev.morling.demos.partialevents;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.node.ObjectNode;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ChangeDataEvent(Map<String, Object> before, Map<String, Object> after, Map<String, Object> source, Op op, long ts_ms) {
	
	public static enum Op {
		
		READ("r"),
		INSERT("c"),
		UPDATE("u"),
		DELETE("d");
		
		private final String code;
		
		Op(String code) {
			this.code = code;
		}
		
		@JsonCreator
	    public static Op forValue(String value) {
	        for (Op op : values()) {
				if(op.code.equals(value)) {
					return op;
				}
			}
	        throw new IllegalArgumentException("Unknown operation type: " + value);
	    }

	    @JsonValue
	    public String value() {
	    	return code;
	    }
	}
}
