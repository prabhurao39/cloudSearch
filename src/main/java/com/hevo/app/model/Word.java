package com.hevo.app.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.List;


@Accessors(chain = true)
@Getter
@Setter
@ToString
public class Word implements CloudDocument {
    private String filePath;
    private String httpUrl;
    private List<String> words = new ArrayList<>();
}
