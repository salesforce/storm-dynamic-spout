
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Look for LoggerFactory.getLogger(classname) where classname doesn't match the class its part of.
 * This is kind of a half assed way to do this.
 */
public class BadLoggersTest {
    private static final Logger logger = LoggerFactory.getLogger(BadLoggersTest.class);

    // Weak attempt
    private static final Pattern regexPattern = Pattern.compile("LoggerFactory.getLogger\\((.*)\\.class\\)");

    @Test
    public void doTest() throws FileNotFoundException {
        // Hacky way to determine root path
        final File currentPath = new File(getClass().getProtectionDomain().getCodeSource().getLocation().getPath());
        final File projectRootPath = currentPath.getParentFile().getParentFile();
        logger.info("Root Path: {}", projectRootPath);

        // Walk all the files in the path
        walk(projectRootPath);
    }

    public void walk(File root) throws FileNotFoundException {
        File[] list = root.listFiles();

        if (list == null) return;

        for (File f : list) {
            if (f.isDirectory()) {
                walk(f);
            } else {
                // Skip non java source files
                if (!f.getAbsoluteFile().getPath().endsWith(".java")) {
                    continue;
                }
                testFile(f);
            }
        }
    }

    public void testFile(File myFile) throws FileNotFoundException {
        String fileData = new Scanner(myFile).useDelimiter("\\Z").next();

        // Look for our pattern
        Matcher matches = regexPattern.matcher(fileData);

        // If we didn't find a match
        if (!matches.find()) {
            return;
        }

        // Grab out the Class name
        String loggerClassName = matches.group(1);
        if (loggerClassName == null) {
            return;
        }

        // Get class name from the file name
        // I bet this will be completely broken for inner classes...
        // if you run into that, just exclude it? or figure out a better solution to this :p
        String className = myFile.getName().replace(".java", "");
        if (!className.equals(loggerClassName)) {
            Assert.fail("Found instance of logger using wrong class? " + myFile.getPath() + " Using " + loggerClassName);
        }
    }
}
