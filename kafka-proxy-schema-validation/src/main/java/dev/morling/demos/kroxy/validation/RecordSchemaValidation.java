package dev.morling.demos.kroxy.validation;

import dev.morling.demos.kroxy.validation.internal.RecordSchemaValidationFilter;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.plugin.Plugins;

@Plugin(configType = RecordSchemaValidationConfig.class)
public class RecordSchemaValidation implements FilterFactory<RecordSchemaValidationConfig, RecordSchemaValidationConfig> {

    @Override
    public RecordSchemaValidationConfig initialize(FilterFactoryContext context, RecordSchemaValidationConfig config) throws PluginConfigurationException {
        return Plugins.requireConfig(this, config);
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, RecordSchemaValidationConfig config) {
        return new RecordSchemaValidationFilter(config);
    }
}
