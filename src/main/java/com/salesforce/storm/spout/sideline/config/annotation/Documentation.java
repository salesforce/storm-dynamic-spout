package com.salesforce.storm.spout.sideline.config.annotation;

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
     * Enum of the categories for the configuration setting.
     */
    enum Category {
        NONE(""),
        SIDELINE("Sideline"),
        KAFKA("Kafka"),
        PERSISTENCE("Persistence"),
        PERSISTENCE_ZOOKEEPER("Zookeeper Persistence");

        private final String value;

        Category(final String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    /**
     * @return Description of the configuration setting.
     */
    String description() default "";

    /**
     * @return Whether or not the configuration setting is required.
     */
    boolean required() default false;

    /**
     * @return Category of the configuration setting.
     */
    Category category() default Category.NONE;

    /**
     * @return Type of the value for the configuration setting.
     */
    Class type() default DEFAULT.class;

    /**
     * Default class type for use on the type field.
     */
    final class DEFAULT {}
}
