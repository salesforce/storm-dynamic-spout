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

import com.google.common.base.Preconditions;
import com.salesforce.storm.spout.dynamic.metrics.MetricDefinition;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Utility to generate documentation from Annotations.
 */
public class DocGenerator {

    private static String DELIMITER = " | ";

    private final Path inputPath;
    private final String delimiterStopTag;
    private final String delimiterStartTag;
    private final List<ClassSpec> classSpecs;

    /**
     * Constructor.
     * @param inputPath Path to input file to inject configuration documentation into.
     * @param delimiterTag What tag to inject into.
     * @param classSpecs What classes and defaults to generate and inject.
     */
    public DocGenerator(final Path inputPath, final String delimiterTag, final List<ClassSpec> classSpecs) {
        this.inputPath = inputPath;
        this.delimiterStartTag =  "<!-- " + delimiterTag + "_BEGIN_DELIMITER -->";
        this.delimiterStopTag = "<!-- " + delimiterTag + "_END_DELIMITER -->";
        this.classSpecs = classSpecs;
    }

    /**
     * Builds documentation around Configuration, injecting it into the document.
     * @throws IOException on IO Errors.
     */
    public void generateConfigDocs() throws IOException {
        try {
            final StringBuilder contentBuilder = new StringBuilder();
            for (final ClassSpec classSpec : classSpecs) {
                buildConfigSection(classSpec.getClazz(), classSpec.getDefaults(), contentBuilder);
            }
            injectContent(contentBuilder.toString());
        } catch (final Exception exception) {
            throw new RuntimeException(exception.getMessage(), exception);
        }
    }

    /**
     * Builds documentation around Metrics, injecting it into the document.
     * @throws IOException on IO Errors.
     */
    public void generateMetricDocs() throws IOException {
        try {
            final StringBuilder contentBuilder = new StringBuilder();
            for (final ClassSpec classSpec : classSpecs) {
                buildMetricSection(classSpec.getClazz(), contentBuilder);
            }
            injectContent(contentBuilder.toString());
        } catch (final Exception exception) {
            throw new RuntimeException(exception.getMessage(), exception);
        }
    }

    /**
     * Injects generated String content into the document between the start delimiter and ending delimiter.
     * @throws IOException on IO errors.
     */
    private void injectContent(final String content) throws IOException {
        // Validate we have an input file
        Preconditions.checkArgument(
            inputPath.toFile().exists() && inputPath.toFile().isFile(),
            "file must exist: %s", inputPath.toAbsolutePath()
        );

        // Copy to a temp file
        final Path tempOutPath = Paths.get(inputPath.toAbsolutePath().toString() + ".tmp");
        Files.deleteIfExists(tempOutPath);
        Files.copy(inputPath, tempOutPath);

        try (BufferedReader readmeReader = Files.newBufferedReader(inputPath, StandardCharsets.UTF_8);
            PrintWriter readmePrintWriter = new PrintWriter(Files.newBufferedWriter(tempOutPath, StandardCharsets.UTF_8))) {
            String line;
            boolean insideConfigurationSection = false;
            boolean configurationSectionFound = false;

            while ((line = readmeReader.readLine()) != null) {
                if (delimiterStartTag.equals(line)) {
                    insideConfigurationSection = true;
                    configurationSectionFound = true;
                    readmePrintWriter.println(line);
                } else if (delimiterStopTag.equals(line)) {
                    insideConfigurationSection = false;
                    readmePrintWriter.append(content);
                    readmePrintWriter.println(line);
                } else if (!insideConfigurationSection) {
                    readmePrintWriter.println(line);
                }
            }

            Preconditions.checkState(
                configurationSectionFound,
                "%s did not have configuration section delimiters: %s",
                inputPath.toAbsolutePath(),
                delimiterStartTag
            );
            Preconditions.checkState(
                !insideConfigurationSection,
                "%s did not have closing configuration section delimiter: %s",
                inputPath.toAbsolutePath(),
                delimiterStopTag
            );
        }
        Files.copy(tempOutPath, inputPath, StandardCopyOption.REPLACE_EXISTING);
        Files.deleteIfExists(tempOutPath);
        System.out.println("Updated README file: " + inputPath.toAbsolutePath());
    }

    /**
     * Injects configuration table into README.
     * @throws IllegalAccessException on error
     * @throws NoSuchFieldException on error
     */
    private void buildConfigSection(
        final Class configClass,
        final Map<String, Object> defaults,
        final StringBuilder outputBuilder
    ) throws IllegalAccessException, NoSuchFieldException {
        System.out.println("Processing " + configClass.getName());

        final Map<ConfigDocumentation.Category, List<String>> lines = new TreeMap<>();

        final Field[] fields = configClass.getDeclaredFields();

        for (Field field : fields) {
            // Presumably our configuration field...
            if (field.getType() == String.class) {
                // Not a documented field, so let's skip over it
                if (!field.isAnnotationPresent(ConfigDocumentation.class)) {
                    continue;
                }

                final String configParam = (String) configClass.getField(field.getName()).get(configClass);

                final ConfigDocumentation documentation = field.getAnnotation(ConfigDocumentation.class);

                final StringBuilder builder = new StringBuilder();

                lines.computeIfAbsent(documentation.category(), k -> new ArrayList<>());

                final String description = documentation.description();
                final String type = documentation.type().getSimpleName();
                final boolean required = documentation.required();
                final String defaultValue = String.valueOf(defaults.getOrDefault(configParam, ""));

                builder.append(configParam).append(DELIMITER);
                builder.append(type).append(DELIMITER);
                builder.append(required ? "Required" : "").append(DELIMITER);
                builder.append(description).append(DELIMITER);
                builder.append(defaultValue);

                lines.get(documentation.category()).add(builder.toString());

                System.out.println("Found lines " + lines);
            }
        }

        for (ConfigDocumentation.Category category : lines.keySet()) {
            if (category != ConfigDocumentation.Category.NONE) {
                outputBuilder
                    .append("### " + category.toString() + " Configuration Options")
                    .append(System.lineSeparator());
            }

            outputBuilder
                .append("Config Key | Type | Required | Description | Default Value |")
                .append(System.lineSeparator())
                .append("---------- | ---- | -------- | ----------- | ------------- |")
                .append(System.lineSeparator());

            Collections.sort(lines.get(category));

            for (String line : lines.get(category)) {
                outputBuilder
                    .append(line)
                    .append(System.lineSeparator());
            }
            outputBuilder.append(System.lineSeparator());
        }
    }

    private void buildMetricSection(
        final Class metricClass,
        final StringBuilder outputBuilder) throws NoSuchFieldException, IllegalAccessException {
        System.out.println("Processing " + metricClass.getName());

        final Map<MetricDocumentation.Category, List<String>> lines = new TreeMap<>();

        final Field[] fields = metricClass.getDeclaredFields();

        for (Field field : fields) {
            // Presumably our configuration field...
            if (!field.getType().isAssignableFrom(MetricDefinition.class)) {
                continue;
            }

            // Not a documented field, so let's skip over it
            if (!field.isAnnotationPresent(MetricDocumentation.class)) {
                continue;
            }

            final MetricDefinition metricDefinition = (MetricDefinition) metricClass.getField(field.getName()).get(metricClass);

            final MetricDocumentation documentation = field.getAnnotation(MetricDocumentation.class);

            final StringBuilder builder = new StringBuilder();
            lines.computeIfAbsent(documentation.category(), k -> new ArrayList<>());

            final String type = documentation.type().name();
            final String unit = documentation.unit().toString();
            final String description = documentation.description();

            // Generate the key with dynamicValues injected.
            String key = metricDefinition.getKey();
            for (final String dynamicValue : documentation.dynamicValues()) {
                key = key.replaceFirst("\\{\\}", "{" + dynamicValue + "}");
            }

            builder.append(key).append(DELIMITER);
            builder.append(type).append(DELIMITER);
            builder.append(unit).append(DELIMITER);
            builder.append(description).append(DELIMITER);
            lines.get(documentation.category()).add(builder.toString());

            System.out.println("Found lines " + lines);
        }

        for (MetricDocumentation.Category category : lines.keySet()) {
            outputBuilder
                .append("### " + category.toString() + " Metrics")
                .append(System.lineSeparator());

            outputBuilder
                .append("Key | Type | Unit | Description |")
                .append(System.lineSeparator())
                .append("--- | ---- | ---- | ----------- |")
                .append(System.lineSeparator());

            Collections.sort(lines.get(category));

            for (String line : lines.get(category)) {
                outputBuilder
                    .append(line)
                    .append(System.lineSeparator());
            }
            outputBuilder.append(System.lineSeparator());
        }
    }
}
