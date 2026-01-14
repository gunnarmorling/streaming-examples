package dev.morling.demos.kroxy.validation.internal;

import dev.morling.demos.kroxy.validation.internal.model.PayloadValidationResult;

public interface PayloadValidator<S> {

	PayloadValidationResult validate(byte[] bytes, S schema);

	public static PayloadValidator<?> NO_OP = new PayloadValidator<>() {

		@Override
		public PayloadValidationResult validate(byte[] bytes, Object schema) {
			return PayloadValidationResult.valid();
		}
	};
}
