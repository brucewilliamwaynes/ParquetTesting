/**
 * 
 */
package inputoutput;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author 1022773
 *
 */
public class FileReaderUtils {

  public static List<String> getFileNameInFolder(String inputURL) {
    File folder = new File(inputURL);
    File[] listOfFiles = folder.listFiles();
    List<String> fileNames = new ArrayList<String>();
    for (int i = 0; i < listOfFiles.length; i++) {
      if (listOfFiles[i].isDirectory()) {
        fileNames.add(listOfFiles[i].getName());
      }
    }
    Collections.sort(fileNames);
    return fileNames;
  }
}
