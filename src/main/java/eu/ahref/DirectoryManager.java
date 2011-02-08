package eu.ahref;

import java.io.File;
import java.io.IOException;

/**
 * Class for directory management
 *
 * @author Martino Pizzol
 */
public class DirectoryManager {
    public static String createTempDirectory() throws IOException {
        final File temp;
        temp = File.createTempFile("temp", Long.toString(System.nanoTime()));
        if(!(temp.delete()))
            throw new IOException("Could not delete temp file: " + temp.getAbsolutePath());
        if(!(temp.mkdir()))
            throw new IOException("Could not create temp directory: " + temp.getAbsolutePath());
        return temp.getAbsolutePath();
    }


    public static boolean deleteDir(String dirname){
        return deleteDirectory(new File(dirname));
    }

    public static boolean deleteDirectory(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i=0; i<children.length; i++) {
                boolean success = deleteDirectory(new File(dir, children[i]));
                if (!success)
                    return false;
            }
        }
        return dir.delete();
    }




}
