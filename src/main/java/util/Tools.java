package util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import rngstream.RngStream;

public class Tools {

  public static String generateSeedString() {
    long [] seed = new long[6];
    Random R = new Random();

    double getSeed = R.nextDouble()*1000000;
    getSeed = Math.ceil(getSeed);

    for(int m=0; m<6; m++) seed[m]=(long)getSeed;

    return RngStream.SeedToStr(seed);
  }

  public static List<List<Integer>> generateGroupsWithOptimal(int sampleOptimalIndex, Integer nSys) {
    List<Integer> indexList = new ArrayList<>();
    for (int i = 1; i < nSys; i++) {
      if (i != sampleOptimalIndex) {
        indexList.add(i);
      }
    }
    Collections.shuffle(indexList);
    List<List<Integer>> combinations = new ArrayList<>();
    int groups = indexList.size() / 2;
    if (indexList.size() % 2 == 1) {
      groups++;
    }
    for (int i = 0; i < groups; i++) {
      if (indexList.size() > 2) {
        List<Integer> group = new ArrayList<>(indexList.subList(0, 2));
        indexList = indexList.subList(2, indexList.size());
        group.add(sampleOptimalIndex);
        combinations.add(group);
      } else {
        List<Integer> group = new ArrayList<>(indexList);
        group.add(sampleOptimalIndex);
        combinations.add(group);
      }
    }
    return combinations;
  }

}
