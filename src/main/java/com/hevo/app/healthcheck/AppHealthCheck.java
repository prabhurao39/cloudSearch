package com.hevo.app.healthcheck;

import com.codahale.metrics.health.HealthCheck;
import com.hevo.app.exceptions.ElasticException;

public class AppHealthCheck extends HealthCheck {

    @Override
    protected Result check() throws ElasticException {
        if (!Result.healthy().isHealthy()) {
            throw new ElasticException("Cloud Search App is un healthy ");
        }
        return Result.healthy();
    }
}
