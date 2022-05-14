package com.mcneilio.shokuyoku.driver;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * This StorageDriver implementation copies files to a specified local directory.
 */
public class LocalStorageDriver  implements StorageDriver{

    private final String basePath;

    /**
     *
     * @param basePath This is where files are copied to.
     */
    public LocalStorageDriver(String basePath) {
        this.basePath = basePath.endsWith("/") ? basePath : basePath + '/';
    }

    @Override
    public void addFile(String date, String eventName, String fileName, Path path) {
        try {
            Files.copy(path, Path.of(basePath + fileName), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
