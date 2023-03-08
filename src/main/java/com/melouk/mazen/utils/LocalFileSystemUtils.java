package com.melouk.mazen.utils;

public class LocalFileSystemUtils {
    private LocalFileSystemUtils() {
    }

    public static String getPathForResource(String fileName) {
        return LocalFileSystemUtils.class.getClassLoader().getResource(fileName).getPath();
    }
}
