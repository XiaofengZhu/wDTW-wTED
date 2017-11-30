/*
 * Arrays.java   Jul 14, 2004
 *
 * Copyright (c) 2004 Stan Salvador
 * stansalvador@hotmail.com
 */

package com.spark.fastdtw.util;

import java.io.Serializable;
import java.util.Arrays;
public class EuclideanDistance implements DistanceFunction, Serializable
{
   public EuclideanDistance()
   {
      
   }
   
   public double calcDistance(double[] vector1, double[] vector2)
   {
      if (vector1.length != vector2.length) //return Double.MAX_VALUE;
         throw new RuntimeException("ERROR:  cannot calculate the distance "
                                 + "between vectors of different sizes. vector1.length: "
                                 + vector1.length + " vector2.length: " + vector2.length 
                                 + "\n vector1: " + Arrays.toString(vector1)
                                 + "\n vector2: " + Arrays.toString(vector2));

      double sqSum = 0.0;
      for (int x = 0; x < vector1.length; x++)
          sqSum += Math.pow(vector1[x] - vector2[x], 2.0);

      return Math.sqrt(sqSum);
   }  // end class euclideanDist(..)

}