package com.hevo.app.constants;

public enum DocType {
    TXT("txt"),
    JSON("json"),
    CDV("cdv"),
    PDF("pdf"),
    DOCX("docx");

    private final String lowercase;
    DocType(String  lowercase) {
        this.lowercase = lowercase;
    }

    public String getLowercase() {
        return lowercase;
    }

    public static DocType fromLowercase(String value) {
        for (DocType doc : DocType.values()) {
            if (doc.getLowercase().equals(value)) {
                return doc;
            }
        }
        throw new IllegalArgumentException("No enum constant with lowercase " + value);
    }
}
