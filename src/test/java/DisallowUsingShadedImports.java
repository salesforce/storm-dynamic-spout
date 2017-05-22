import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertTrue;

/**
 * Using imports from shaded libraries is a bad practice and makes upgrading difficult.
 */
public class DisallowUsingShadedImports {
    private static final Logger logger = LoggerFactory.getLogger(DisallowUsingShadedImports.class);

    // Find any imports using .shade.
    private static final Pattern regexPattern = Pattern.compile("import .*(\\.storm\\.shade\\.).*;");

    private List<String> failedFiles = new ArrayList<>();

    @Test
    public void doTest() throws FileNotFoundException {
        // Hacky way to determine root path
        final File currentPath = new File(getClass().getProtectionDomain().getCodeSource().getLocation().getPath());
        final File projectRootPath = currentPath.getParentFile().getParentFile();

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

        for (String errorStr: failedFiles) {
            logger.error(errorStr);
        }
        assertTrue("Should have not found any files", failedFiles.isEmpty());
    }

    public void testFile(File myFile) throws FileNotFoundException {
        String fileData = new Scanner(myFile).useDelimiter("\\Z").next();

        // Look for our pattern
        Matcher matches = regexPattern.matcher(fileData);

        // If we didn't find a match
        if (!matches.find()) {
            return;
        }

        // Found shade import usage!
        failedFiles.add("Found instance of logger using wrong class? " + myFile.getPath() + " Using " + matches.toMatchResult().toString());
    }
}
