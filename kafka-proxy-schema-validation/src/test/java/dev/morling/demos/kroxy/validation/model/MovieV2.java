package dev.morling.demos.kroxy.validation.model;

import io.confluent.kafka.schemaregistry.annotations.Schema;

@Schema(value="""
		{
		  "$schema": "https://json-schema.org/draft/2020-12/schema",
		  "$id": "https://example.com/movieV2.schema.json",
		  "title": "Movie",
		  "description": "A movie",
		  "type": "object",
		  "properties": {
		    "id": {
		      "type": "number"
		    },
		    "title": {
		      "type": "string"
		    },
		    "yearOfRelease": {
		      "type": "string",
			  "format" : "date"
		    }
		  }
		}
		""",
        refs= {})
public record MovieV2(long id, String title, String yearOfRelease) {

}
