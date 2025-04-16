package dev.morling.demos.partialevents;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.node.ObjectNode;

@JsonIgnoreProperties(ignoreUnknown = true)
public record SchematizedChangeDataEvent(Map<String, Object> schema, ChangeDataEvent payload){
}
