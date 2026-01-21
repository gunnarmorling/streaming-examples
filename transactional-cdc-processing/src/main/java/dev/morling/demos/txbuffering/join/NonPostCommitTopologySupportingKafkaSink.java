/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.demos.txbuffering.join;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.TwoPhaseCommittingStatefulSink;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;

import dev.morling.demos.txbuffering.DataStreamV2Job;

/**
 * The {@code KafkaSink} currently is not compatible with DataStream V2:
 *
 * java.lang.UnsupportedOperationException: Sink with post-commit topology is not supported for DataStream v2 atm.
 *
 * So we're exposing a wrapper without the {@code SupportsPostCommitTopology} interface for now.
 */
public class NonPostCommitTopologySupportingKafkaSink implements InvocationHandler, Serializable {

	public static <T> Sink<T> getInstance(KafkaSink<T> sink) {
		return (Sink<T>) Proxy.newProxyInstance(
				DataStreamV2Job.class.getClassLoader(),
				  new Class[] { LineageVertexProvider.class, TwoPhaseCommittingStatefulSink.class },
				  new NonPostCommitTopologySupportingKafkaSink(sink));
	}

	private final KafkaSink<?> delegate;

	private NonPostCommitTopologySupportingKafkaSink(KafkaSink<?> delegate) {
		this.delegate = delegate;
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		return method.invoke(delegate, args);
	}
}
