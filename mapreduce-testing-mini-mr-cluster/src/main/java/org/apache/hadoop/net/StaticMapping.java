/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.net;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetworkTopology;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is a crooked nail for 2.0.0-mr1-cdh4.0.0 hadoop cluster work properly. StaticMapping class is missing while
 * starting MiniDFSCluster
 * Implements the {@link org.apache.hadoop.net.DNSToSwitchMapping} via static mappings. Used in testcases that simulate racks.
 */
public class StaticMapping extends Configured implements DNSToSwitchMapping
{
    public void setconf(Configuration conf)
    {
        String[] mappings = conf.getStrings("hadoop.configured.node.mapping");
        if (mappings != null) {
            //for (int counter = 0; counter < mappings.length; counter++) {
            for (String mapping : mappings) {
                //String str = mappings[counter];
                String host = mapping.substring(0, mapping.indexOf('='));
                String rack = mapping.substring(mapping.indexOf('=') + 1);
                addNodeToRack(host, rack);
            }
        }
    }

    /* Only one instance per JVM */
    private static final Map<String, String> NAME_TO_RACK = new HashMap<String, String>();


    public static synchronized void addNodeToRack(String name, String rackId)
    {
        NAME_TO_RACK.put(name, rackId);
    }


    @Override
    public List<String> resolve(List<String> names)
    {
        List<String> resolved = new ArrayList<String>();
        synchronized (NAME_TO_RACK) {
            for (String name : names) {
                String rackId;
                if ((rackId = NAME_TO_RACK.get(name)) == null) {
                    resolved.add(NetworkTopology.DEFAULT_RACK);
                } else {
                    resolved.add(rackId);
                }
            }
            return resolved;
        }
    }
}