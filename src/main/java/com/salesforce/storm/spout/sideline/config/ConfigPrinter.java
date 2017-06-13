package com.salesforce.storm.spout.sideline.config;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.config.annotation.Documentation;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConfigPrinter {

    public static String DELIMITER = " | ";

    public static void main(String[] args) throws Exception {
        Map<String, Object> config = SpoutConfig.setDefaults(Maps.newHashMap());

        Map<Documentation.Category, List<String>> lines = new HashMap<>();

        Field[] fields = SpoutConfig.class.getDeclaredFields();

        for (Field field : fields) {
            // Presumably our configuration field...
            if (field.getType() == String.class) {
                final String configParam = (String) SpoutConfig.class.getField(field.getName()).get(SpoutConfig.class);

                Documentation documentation = field.getAnnotation(Documentation.class);

                if (lines.get(documentation.category()) == null) {
                    lines.put(documentation.category(), new ArrayList<>());
                }

                String description = documentation.description();
                String type = documentation.type().getSimpleName();
                boolean required = documentation.required();

                StringBuilder builder = new StringBuilder();
                builder.append(configParam).append(DELIMITER);
                builder.append(type).append(DELIMITER);
                builder.append(required ? "Required" : "").append(DELIMITER);
                builder.append(config.get(configParam)).append(DELIMITER);
                builder.append(description);

                lines.get(documentation.category()).add(builder.toString());
            }
        }

        for (Documentation.Category category : lines.keySet()) {
            if (category != Documentation.Category.NONE) {
                System.out.println(category.toString());
            }

            Collections.sort(lines.get(category));

            for (String line : lines.get(category)) {
                System.out.println(line);
            }

            System.out.println("");
        }
    }
}
