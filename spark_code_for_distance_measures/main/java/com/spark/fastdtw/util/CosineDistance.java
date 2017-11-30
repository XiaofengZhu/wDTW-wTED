/*
 * Arrays.java   Jul 14, 2004
 *
 * Copyright (c) 2004 Stan Salvador
 * stansalvador@hotmail.com
 */

package com.spark.fastdtw.util;

import java.io.Serializable;

public class CosineDistance implements DistanceFunction, Serializable
{
   public CosineDistance()
   {
      
   }
   
   public double calcDistance(double[] vector1, double[] vector2)
   {
      if (vector1.length != vector2.length)
         throw new RuntimeException("ERROR:  cannot calculate the distance "
                                 + "between vectors of different sizes.");

      double product = 0.0;
      double sqSum1 = 0.0;
      double sqSum2 = 0.0;
      for (int x = 0; x < vector1.length; x++)
          product += vector1[x] *vector2[x];

      for (int x = 0; x < vector1.length; x++)
          sqSum1 += Math.pow(vector1[x], 2.0);
      
      for (int x = 0; x < vector2.length; x++)
          sqSum2 += Math.pow(vector2[x], 2.0);      
      return product / (Math.sqrt(sqSum1) * Math.sqrt(sqSum2));
   }  // end class euclideanDist(..)

}