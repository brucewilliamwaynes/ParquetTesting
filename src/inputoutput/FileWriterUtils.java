/**
 * 
 */
package inputoutput;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * @author 1022773
 *
 */
public class FileWriterUtils {

  public static void writeListInFormat(List<String> files, String outputURL) throws IOException {
    FileWriter fw = new FileWriter(outputURL);
    StringBuilder sb = new StringBuilder();
    char doubleQuote = '"';
    for (String fileName : files) {
      sb.append(doubleQuote);
      sb.append(fileName);
      sb.append(doubleQuote);
      sb.append(",");
    }
    String ans = sb.toString();
    if (ans.length() > 0) {
      ans = ans.substring(0, ans.length() - 1);
    }
    ans = "[" + ans + "]";
    fw.write(ans);
    fw.close();
  }

}
