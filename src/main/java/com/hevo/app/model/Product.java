package com.hevo.app.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;


@Accessors(chain = true)
@Getter
@Setter
@ToString
public class Product implements CloudDocument{
    private String filePath;
    private String httpUrl;
    private String productName;
    private int rating;
    private String productId;
    private double price;
}
