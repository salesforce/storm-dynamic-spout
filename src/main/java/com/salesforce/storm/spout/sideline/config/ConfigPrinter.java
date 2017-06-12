package com.salesforce.storm.spout.sideline.config;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.config.annotation.Category;
import com.salesforce.storm.spout.sideline.config.annotation.DefaultValue;
import com.salesforce.storm.spout.sideline.config.annotation.Description;
import com.salesforce.storm.spout.sideline.config.annotation.Required;
import com.salesforce.storm.spout.sideline.config.annotation.Type;
import sun.security.krb5.internal.crypto.Des;

import java.lang.reflect.Field;
import java.util.Map;

public class ConfigPrinter {

    public static String DELIMITER = " | ";

    public static void main(String[] args) throws Exception {
        Map<String, Object> config = SidelineSpoutConfig.setDefaults(Maps.newHashMap());

        Field[] fields = SidelineSpoutConfig.class.getDeclaredFields();

        for (Field field : fields) {
            // Presumably our configuration field...
            if (field.getType() == String.class) {
                final String configParam = (String) SidelineSpoutConfig.class.getField(field.getName()).get(SidelineSpoutConfig.class);

                Category category = field.getAnnotation(Category.class);
                Description description = field.getAnnotation(Description.class);
                Type type = field.getAnnotation(Type.class);
                Required required = field.getAnnotation(Required.class);
                DefaultValue defaultValue = field.getAnnotation(DefaultValue.class);

                StringBuilder builder = new StringBuilder();

                builder.append(configParam).append(DELIMITER);
                builder.append(type.value()).append(DELIMITER);
                builder.append(required != null ? "Required" : "").append(DELIMITER);
                builder.append(defaultValue != null ? defaultValue.value() : "").append(DELIMITER);
                builder.append(description.value());

                System.out.println(builder.toString());
            }
        }
    }
}
