package util;

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

}
