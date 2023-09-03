package com.hevo.app.resource;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.util.List;

@Accessors(chain = true)
@Getter
@Setter
@ToString
public class QueryResponse<T> {
    List<T> list;
    String message;
}
