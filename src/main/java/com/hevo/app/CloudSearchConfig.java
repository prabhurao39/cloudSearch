package com.hevo.app;

import com.hevo.app.client.AwsFactory;
import io.dropwizard.Configuration;
import lombok.Getter;
import lombok.Setter;

import javax.validation.Valid;

@Setter
@Getter
public class CloudSearchConfig extends Configuration {

    @Valid
    private AwsFactory awsFactory;

    @Valid
    private String sqsEventNotifierName;
}
