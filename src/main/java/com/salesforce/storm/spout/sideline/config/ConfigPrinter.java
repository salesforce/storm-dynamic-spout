package com.salesforce.storm.spout.sideline.config;

import com.google.common.base.Preconditions;
import com.salesforce.storm.spout.sideline.config.annotation.Category;
import com.salesforce.storm.spout.sideline.config.annotation.DefaultValue;
import com.salesforce.storm.spout.sideline.config.annotation.Description;
import com.salesforce.storm.spout.sideline.config.annotation.Required;
import com.salesforce.storm.spout.sideline.config.annotation.Type;

import java.io.BufferedReader;
import java.io.File;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/**
 * 
 * When executed, this class will update the Configuration section of README.md file.
 * The old file will be saved as README.md.bak.
 *
 */
public class ConfigPrinter {

    public static String DELIMITER = " | ";
    private static final String CONFIGURATION_BEGIN_DELIMITER = "[//]: <> (CONFIGURATION_BEGIN_DELIMITER)";
    private static final String CONFIGURATION_END_DELIMITER = "[//]: <> (CONFIGURATION_END_DELIMITER)";

    public static void main(String[] args) throws Exception {

        // Assume folders and files are relative to the project root
        Path readmePath = Paths.get("README.md");
        Path readmeTempOutPath = Paths.get("target/README.md");

        // Optionally backup the existing README file (will not override the previous backup) or
        // delete this section if backup is not desired (that's what we have github for, right?)
        Path readmeBackupPath = Paths.get("README.md.bak");
        final File readmeBackupFile = readmeBackupPath.toFile();
        if (readmeBackupFile.exists()) {
            System.out.println("The backup file exists and cannot be overwtitten.");
            System.out.println("Manually delete it first: " + readmeBackupFile.getAbsolutePath());
            return;
        }

        Files.copy(readmePath, readmeBackupPath);

        final File readmeFile = readmePath.toFile();
        Preconditions.checkArgument(readmeFile.exists() && readmeFile.isFile(), "README.md file must exist: %s", readmeFile.getAbsolutePath());

        try (BufferedReader readmeReader = Files.newBufferedReader(readmePath, StandardCharsets.UTF_8);
            PrintWriter readmePrintWriter = new PrintWriter(Files.newBufferedWriter(readmeTempOutPath, StandardCharsets.UTF_8))) {
            String line;
            boolean insideConfigurationSection = false;
            boolean configurationSectionFound = false;

            while ((line = readmeReader.readLine()) != null) {
                if (CONFIGURATION_BEGIN_DELIMITER.equals(line)) {
                    insideConfigurationSection = true;
                    configurationSectionFound = true;
                    readmePrintWriter.println(line);
                } else if (CONFIGURATION_END_DELIMITER.equals(line)) {
                    mergeConfigSection(readmePrintWriter);
                    insideConfigurationSection = false;
                    readmePrintWriter.println(line);
                } else if (!insideConfigurationSection) {
                    readmePrintWriter.println(line);
                }
            }

            Preconditions.checkState(configurationSectionFound, "README.md did not have configuration section delimiters: %s", readmeFile.getAbsolutePath());
            Preconditions.checkState(!insideConfigurationSection, "README.md did not have closing configuration section delimiter: %s", readmeFile.getAbsolutePath());
        }
        Files.copy(readmeTempOutPath, readmePath, StandardCopyOption.REPLACE_EXISTING);
        System.out.println("Updated README file: " + readmeFile.getAbsolutePath());
    }

    /**
     * Injects configuration table into README
     * 
     * @throws IllegalAccessException
     * @throws NoSuchFieldException
     */
    private static void mergeConfigSection(PrintWriter readmePrintWriter) throws IllegalAccessException, NoSuchFieldException {
        readmePrintWriter.println();
        readmePrintWriter.println("Config Key   | Type | Description | Default Value |");
        readmePrintWriter.println("------------ | ---- | ----------- | --------------");

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
                // TODO: the original code did not category append.
                // Do we need it? If not delete the unused category variable above and the line below.
                // builder.append(category != null ? category.value() : "").append(DELIMITER);
                builder.append(defaultValue != null ? defaultValue.value() : "").append(DELIMITER);
                builder.append(description.value());

                readmePrintWriter.println(builder.toString());
            }
        }
        readmePrintWriter.println();
    }
}
