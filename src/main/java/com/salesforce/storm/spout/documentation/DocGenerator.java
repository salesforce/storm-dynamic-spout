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

package com.salesforce.storm.spout.documentation;

import com.google.common.base.Preconditions;

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
        this.delimiterStartTag =  "[//]: <> (" + delimiterTag + "_BEGIN_DELIMITER)";
        this.delimiterStopTag = "[//]: <> (" + delimiterTag + "_END_DELIMITER)";
        this.classSpecs = classSpecs;
    }

    /**
     * Generate the documentation.
     * @throws IOException on IO errors.
     */
    public void generate() throws IOException {
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
                    for (final ClassSpec classSpec : classSpecs) {
                        mergeConfigSection(classSpec.getClazz(), classSpec.getDefaults(), readmePrintWriter);
                    }
                    insideConfigurationSection = false;
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
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new RuntimeException(e.getMessage(), e);
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
    private void mergeConfigSection(
        final Class configClass,
        final Map<String, Object> defaults,
        final PrintWriter readmePrintWriter
    ) throws IllegalAccessException, NoSuchFieldException {
        System.out.println("Processing " + configClass.getName());

        readmePrintWriter.println();

        final Map<Documentation.Category, List<String>> lines = new TreeMap<>();

        final Field[] fields = configClass.getDeclaredFields();

        for (Field field : fields) {
            // Presumably our configuration field...
            if (field.getType() == String.class) {
                // Not a documented field, so let's skip over it
                if (!field.isAnnotationPresent(Documentation.class)) {
                    continue;
                }

                final String configParam = (String) configClass.getField(field.getName()).get(configClass);

                final Documentation documentation = field.getAnnotation(Documentation.class);

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

        for (Documentation.Category category : lines.keySet()) {
            if (category != Documentation.Category.NONE) {
                readmePrintWriter.println("### " + category.toString());
            }

            readmePrintWriter.println("Config Key | Type | Required | Description | Default Value |");
            readmePrintWriter.println("---------- | ---- | -------- | ----------- | ------------- |");

            Collections.sort(lines.get(category));

            for (String line : lines.get(category)) {
                readmePrintWriter.println(line);
            }
            readmePrintWriter.println();
        }

        readmePrintWriter.println();
    }
}
