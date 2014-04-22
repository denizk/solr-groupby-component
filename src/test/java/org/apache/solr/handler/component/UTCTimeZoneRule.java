package org.apache.solr.handler.component;

import org.joda.time.DateTimeZone;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

class TimeZoneTestRule implements TestRule {

	private final DateTimeZone zone;

	TimeZoneTestRule(DateTimeZone zone) {
		this.zone = zone;
	}

	public Statement apply(final Statement base, Description description) {
		return new Statement() {

			@Override
			public void evaluate() throws Throwable {
				DateTimeZone defaultTimeZone = DateTimeZone.getDefault();
				try {
					DateTimeZone.setDefault(zone);
					base.evaluate();
				} finally {
					DateTimeZone.setDefault(defaultTimeZone);
				}
			}
		};
	}
}