package smt.test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.Scanner;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/**
 * Collection of useful static methods to help with testing.
 */
public class TestUtils {

    /**
     * NOTE - this method doesn't work (and isn't being used) as it doesn't include dependant libraries when it creates
     * a JAR file.
     */
    public static void createConnectorPlugin(String outputJarFilename) throws IOException {
        // NOTE THIS METHOD DOESN'T WORK AS IT DOESN'T INCLUDE DEPENDANT LIBRARIES
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        JarOutputStream target = new JarOutputStream(new FileOutputStream("build/" + outputJarFilename), manifest);
        addFileToJar(new File("build/classes/java/main/"), target);
        target.close();
    }

    /**
     * This method does work as it calls the gradle wrapper with "shadowJar" task and creates a shadow / uber JAR file
     * containing all dependant libraries.
     */
    public static File createConnectorPluginWithGradle() {
        // Check if file exists and delete if it does
        File existingJarFile = findFile("build/libs", "-all.jar");
        if (existingJarFile != null && existingJarFile.exists()) {
            existingJarFile.delete();
        }
        boolean isWindows = System.getProperty("os.name").toLowerCase().startsWith("windows");
        String gradleCmd = isWindows ? "gradlew.bat" : "./gradlew";
        String output = execCmd(gradleCmd + " shadowJar");
        if (output == null || !output.contains("BUILD SUCCESSFUL")) {
            throw new RuntimeException("Gradle build unsuccessful\n\n" + output);
        }
        File jarFile = findFile("build/libs", "-all.jar");
        if (!jarFile.exists()) {
            throw new RuntimeException("Shadow jar does not exist: " + jarFile.getAbsolutePath());
        }
        return jarFile;
    }

    public static String execCmd(String cmd) {
        try {
            Scanner s = new Scanner(Runtime.getRuntime().exec(cmd).getInputStream()).useDelimiter("\\A");
            return s.hasNext() ? s.next() : "";
        } catch (IOException ioEx) {
            throw new RuntimeException("Error executing: " + cmd, ioEx);
        }
    }

    public static int getRandomFreePort() {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        } catch (IOException e) {
            e.printStackTrace();
            return 0;
        }
    }

    // Private methods

    private static void addFileToJar(File source, JarOutputStream target) throws IOException {
        String name = source.getPath().replace("\\", "/")
            .replace("build/classes/java/main", "");
        if (source.isDirectory()) {
            if (!name.endsWith("/")) {
                name += "/";
            }
            JarEntry entry = new JarEntry(name);
            entry.setTime(source.lastModified());
            target.putNextEntry(entry);
            target.closeEntry();
            for (File nestedFile : source.listFiles()) {
                addFileToJar(nestedFile, target);
            }
        } else {
            JarEntry entry = new JarEntry(name);
            entry.setTime(source.lastModified());
            target.putNextEntry(entry);
            try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(source))) {
                byte[] buffer = new byte[1024];
                while (true) {
                    int count = in.read(buffer);
                    if (count == -1) {
                        break;
                    }
                    target.write(buffer, 0, count);
                }
                target.closeEntry();
            }
        }
    }

    private static File findFile(String folder, String filenameFilterEndsWith) {
        File file = new File(folder);
        if (!file.exists() || !file.isDirectory()) {
            return null;
        }
        return Arrays.stream(file.listFiles())
            .filter(f -> f.getName().endsWith(filenameFilterEndsWith))
            .findFirst().orElse(null);
    }

}
