package com.hevo.app;

import com.google.inject.AbstractModule;
import io.dropwizard.setup.Environment;

public class CloudSearchModule extends AbstractModule {

    private final Environment environment;
    private final CloudSearchConfig configuration;

    public CloudSearchModule(CloudSearchConfig configuration, Environment environment) {
        this.configuration = configuration;
        this.environment = environment;
    }

    @Override
    protected void configure() {
        bind(Environment.class).toInstance(environment);
        bind(CloudSearchConfig.class).toInstance(configuration);
    }
}
