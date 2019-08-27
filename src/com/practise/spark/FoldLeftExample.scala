package com.practise.spark

import org.apache.spark.sql.DataFrame

object FoldLeftExample {

  def snakecaseify(s: String): String = {
    s.toLowerCase().replace(" ", "_")
  }

  def snakeCaseColumns(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df) { (acc, cn) =>
      acc.withColumnRenamed(cn, snakecaseify(cn))
    }
  }

  // Further example found in Odd Occurences In Array


  // Below is the usage.

 /* val sourceDF = Seq(
    ("funny", "joke")
  ).toDF("A b C", "de F")

  val actualDF = Converter.snakeCaseColumns(sourceDF)

  val expectedDF = Seq(
    ("funny", "joke")
  ).toDF("a_b_c", "de_f")*/

}
