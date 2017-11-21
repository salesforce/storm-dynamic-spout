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

package com.salesforce.storm.spout.dynamic.config;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.documentation.ClassSpec;
import com.salesforce.storm.spout.documentation.DocGenerator;
import com.salesforce.storm.spout.dynamic.kafka.KafkaConsumerConfig;
import com.salesforce.storm.spout.sideline.config.SidelineConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Generates Configuration Values for DynamicSpout.
 * For now this generates the configuration for ALL "sub" projects (DynamicSpout, Sideline, Kafka consumer)
 * but the idea is that in the future we'd define a task within each module and be able to generate separate READMEs
 * for each.
 */
public class DocTask {
    /**
     * Entry point into Doc generating task.
     * @throws IOException on error writing file.
     */
    public static void main(String[] args) throws IOException {
        final Path inputPath = Paths.get("README.md");
        final String tagArg = "CONFIGURATION";
        final List<ClassSpec> classSpecs = new ArrayList<>();
        classSpecs.add(new ClassSpec(SpoutConfig.class, SpoutConfig.setDefaults(Maps.newHashMap())));
        classSpecs.add(new ClassSpec(KafkaConsumerConfig.class));
        classSpecs.add(new ClassSpec(SidelineConfig.class));

        final DocGenerator docGenerator = new DocGenerator(inputPath, tagArg, classSpecs);
        docGenerator.generate();
    }
}
