package com.apptanium.api.bigds;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;

/**
 * @author sgupta
 * @since 1/24/15.
 */
public class MatrixBuilder {
  public static void main(String[] args) {
    List<List<String>> stringLists = new ArrayList<>();
    stringLists.add(Arrays.asList("Monday", "Tuesday", "Wednesday", "Thursday"));
    stringLists.add(Arrays.asList("Carrot", "Cucumber", "Cabbage", "Zucchini"));
//    stringLists.add(Arrays.asList("Cake", "Cookie", "Pie"));

    List<List<String>> matrix = new ArrayList<>();
    buildMatrix(stringLists, new Stack<String>(), 0, matrix);

    System.out.println("matrix.size() = " + matrix.size());
    System.out.println("matrix = " + matrix);
  }

  public static void buildMatrix(List<List<String>> source, Stack<String> stack, int index, List<List<String>> target) {
    if(index == source.size()) {
      return;
    }
    List<String> list = source.get(index);
    for (String string : list) {
      if(index + 1 < source.size()) {
        stack.push(string);
        buildMatrix(source, stack, index + 1, target);
        stack.pop();
      }
      else {
        List<String> line = new ArrayList<>();
        line.addAll(stack);
        line.add(string);
        target.add(line);
      }
    }
  }





}
