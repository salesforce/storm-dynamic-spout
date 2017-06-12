package com.salesforce.storm.spout.sideline;

import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.handler.SidelineSpoutHandler;
import com.salesforce.storm.spout.sideline.handler.SidelineVirtualSpoutHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Spout that supports sidelining messages by filters.
 */
public class SidelineSpout extends DynamicSpout {

    private static final Logger logger = LoggerFactory.getLogger(SidelineSpout.class);

    /**
     * Used to overload and modify settings before passing them to the constructor.
     * @param config Supplied configuration.
     * @return Resulting configuration.
     */
    private static Map<String, Object> modifyConfig(Map<String, Object> _config) {
        final Map<String, Object> config = Tools.immutableCopy(_config);
        config.put(SidelineSpoutConfig.SPOUT_HANDLER_CLASS, SidelineSpoutHandler.class.getName());
        config.put(SidelineSpoutConfig.VIRTUAL_SPOUT_HANDLER_CLASS, SidelineVirtualSpoutHandler.class.getName());
        return config;
    }

    /**
     * Spout that supports sidelining messages by filters.
     * @param config Spout configuration.
     */
    public SidelineSpout(Map config) {
        super(modifyConfig(config));
    }
}
