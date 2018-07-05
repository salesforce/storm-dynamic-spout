/*
 * Copyright (c) 2018, Salesforce.com, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
 * following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this list of conditions and the following
 *   disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided with the distribution.
 *
 * * Neither the name of Salesforce.com nor the names of its contributors may be used to endorse or promote products
 *   derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
 * USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.salesforce.storm.spout.documentation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Document metric information.
 *
 * Used to auto-generate metric documentation in README files.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface MetricDocumentation {

    /**
     * Types of metrics.
     */
    enum Type {
        AVERAGE,
        COUNTER,
        GAUGE,
        TIMER;

        public String toString() {
            return name();
        }
    }

    /**
     * Categories for a metric.
     */
    enum Category {
        DYNAMIC_SPOUT("Dynamic Spout"),
        KAFKA("Kafka"),
        SIDELINE("Sideline");

        final String value;

        Category(final String value) {
            this.value = value;
        }

        public String toString() {
            return value;
        }
    }

    /**
     * Unit for a metric.
     */
    enum Unit {
        UNKNOWN("Unknown"),
        NUMBER("Number"),
        PERCENT("Percent 0.0 to 1.0"),
        TIME_MILLISECONDS("Time in milliseconds"),
        TIME_SECONDS("Time in seconds");

        final String value;

        Unit(final String value) {
            this.value = value;
        }

        public String toString() {
            return value;
        }
    }

    /**
     * Get the description of the metric.
     * @return description of the metric
     */
    String description() default "";

    /**
     * Get the values that should be replaced in the key.
     * @return values that should be replaced in the key
     */
    String[] dynamicValues() default {};

    /**
     * Get the unit of measurement for the metric.
     * @return unit of measurement for the metric
     */
    Unit unit() default Unit.UNKNOWN;

    /**
     * Get the category of the metric.
     * @return category of the metric
     */
    Category category();

    /**
     * Get the type of the metric.
     * @return type of the metric
     */
    Type type();
}
