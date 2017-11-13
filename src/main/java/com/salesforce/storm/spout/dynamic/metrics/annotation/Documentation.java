/**
 * Copyright (c) 2017, Salesforce.com, Inc.
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

package com.salesforce.storm.spout.dynamic.metrics.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for documenting spout configuration options.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Documentation {

    /**
     * Enum of the Metric types.
     */
    enum Type {
        AVERAGE,
        COUNTER,
        GAUGE,
        TIMER
    }

    /**
     * Enum of Categories.
     */
    enum Category {
        UNKNOWN,
        CONSUMER,
        DYNAMIC_SPOUT,
        KAFKA_CONSUMER,
        SPOUT_MONITOR,
        VIRTUAL_SPOUT
    }

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
    }

    /**
     * @return Description of the configuration setting.
     */
    String description() default "";

    String[] dynamicValues() default {};

    Unit unit() default Unit.UNKNOWN;

    /**
     * @return Category of the configuration setting.
     */
    Category category();

    /**
     * @return Category of the configuration setting.
     */
    Type type();
}
