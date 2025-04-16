/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright Gunnar Morling
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.demos.partialevents.datastream;

import java.util.Map;

public record KafkaRecord(Map<String, Object> key, Map<String, Object> value) {
}
