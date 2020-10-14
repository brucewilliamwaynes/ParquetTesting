/**
 * 
 */
package controller;

import java.io.IOException;
import java.util.List;

import inputoutput.FileReaderUtils;
import inputoutput.FileWriterUtils;

/**
 * @author 1022773
 *
 */
public class FileDriver {

  /**
   * File utility to extratc file names in a folder.
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    String inputURL = "/Users/1022773/Desktop/DCROFuncTests/dcroengineinput/";
    String outputURL = "/Users/1022773/Desktop/listOfTestFiles.txt";
    List<String> files = FileReaderUtils.getFileNameInFolder(inputURL);
    FileWriterUtils.writeListInFormat(files, outputURL);
  }

}
