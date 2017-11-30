/*
 * FastDtwTest.java   Jul 14, 2004
 *
 * Copyright (c) 2004 Stan Salvador
 * stansalvador@hotmail.com
 */

package com.spark.fastdtw.examples;

import com.spark.fastdtw.dtw.TimeWarpInfo;
import com.spark.fastdtw.timeseries.TimeSeries;
import com.spark.fastdtw.util.DistanceFunction;
import com.spark.fastdtw.util.DistanceFunctionFactory;


/**
 * This class contains a main method that executes the FastDTW algorithm on two
 * time series with a specified radius.
 *
 * @author Stan Salvador, stansalvador@hotmail.com
 * @since Jul 14, 2004
 */
public class FastDtwTest
{
   /**
    * This main method executes the FastDTW algorithm on two time series with a
    * specified radius. The time series arguments are file names for files that
    * contain one measurement per line (time measurements are an optional value
    * in the first column). After calculating the warp path, the warp
    * path distance will be printed to standard output, followed by the path
    * in the format "(0,0),(1,0),(2,1)..." were each pair of numbers in
    * parenthesis are indexes of the first and second time series that are
    * linked in the warp path
    *
    * @param args  command line arguments (see method comments)
    */
      public static void main(String[] args)
      {
         if (args.length!=1)
         {
            System.out.println("USAGE:  java FastDtwTest timeSeries1 timeSeries2 radius [EuclideanDistance|ManhattanDistance|BinaryDistance]");
            System.exit(1);
         }
         else
         {
             final TimeSeries tsI = new TimeSeries("[0.04878878965973854,0.04995628073811531,0.13439227640628815,-0.05697503313422203,0.046873342245817184,0.013826948590576649,-0.007900422438979149,-0.010530607774853706,-0.03809928521513939,-0.014140762388706207] [-0.010435627773404121,0.39358317852020264,-0.2105112075805664,0.12274198234081268,0.6048959493637085,-0.5590744018554688,-0.3862703740596771,0.3633899986743927,0.3021542429924011,0.22285611927509308] [0.04878878965973854,0.04995628073811531,0.13439227640628815,-0.05697503313422203,0.046873342245817184,0.013826948590576649,-0.007900422438979149,-0.010530607774853706,-0.03809928521513939,-0.014140762388706207] [-0.010435627773404121,0.39358317852020264,-0.2105112075805664,0.12274198234081268,0.6048959493637085,-0.5590744018554688,-0.3862703740596771,0.3633899986743927,0.3021542429924011,0.22285611927509308]");
             final TimeSeries tsJ = new TimeSeries("[0.04878878965973854,0.04995628073811531,0.13439227640628815,-0.05697503313422203,0.046873342245817184,0.013826948590576649,-0.007900422438979149,-0.010530607774853706,-0.03809928521513939,-0.014140762388706207] [-0.010435627773404121,0.39358317852020264,-0.2105112075805664,0.12274198234081268,0.6048959493637085,-0.5590744018554688,-0.3862703740596771,0.3633899986743927,0.3021542429924011,0.22285611927509308] [0.04878878965973854,0.04995628073811531,0.13439227640628815,-0.05697503313422203,0.046873342245817184,0.013826948590576649,-0.007900422438979149,-0.010530607774853706,-0.03809928521513939,-0.014140762388706207] [-0.010435627773404121,0.39358317852020264,-0.2105112075805664,0.12274198234081268,0.6048959493637085,-0.5590744018554688,-0.3862703740596771,0.3633899986743927,0.3021542429924011,0.22285611927509308]"); 
            
            final DistanceFunction distFn;
            if (args.length < 2)
            {
               distFn = DistanceFunctionFactory.getDistFnByName("EuclideanDistance"); 
            }
            else
            {
               distFn = DistanceFunctionFactory.getDistFnByName(args[1]);
            }   // end if
            
            final TimeWarpInfo info = com.spark.fastdtw.dtw.FastDTW.getWarpInfoBetween(tsI, tsJ, Integer.parseInt(args[0]), distFn);
            System.out.println("Warp Distance: " + info.getDistance());
            System.out.println("Warp Path:     " + info.getPath()); 
            
            double dist = com.spark.fastdtw.dtw.FastDTW.getWarpDistBetween(tsI, distFn);
            System.out.println("Warp Distance of tsI only: " + dist);
         }  // end if

      }  // end main()


}  // end class FastDtwTest
