package samples
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Exercise1 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    //Question 1 : Lire le fichier "films.csv" sous forme de RDD[String]
    val rdd = sparkSession.sparkContext.textFile("data/donnees.csv")

    //Question 2 : Combien y a-t-il de films de Leonardo Di Caprio dans ce fichier ?
    val DiCapRDD = rdd.filter(elem => elem.contains("Di Caprio"))
    println("Il y a " + DiCapRDD.count() + " films de Leonardo Di Caprio")

    //Question 3 : Quelle est la moyenne des notes des films de Di Caprio ?

    val notesDiCap = DiCapRDD.map(item => (item.split(";")(2).toDouble))
    val meanNotes = notesDiCap.sum() / notesDiCap.count()
    println("La moyenne des notes des films de Di Caprio est : " + meanNotes)

    //Question 4 : Quel est le pourcentage de vues des films de Di Caprio par rapport à l'échantillon que nous avons ?

    val viewsDiCap = DiCapRDD.map(item => (item.split(";")(1).toDouble))
    val allViews = rdd.map(item => (item.split(";")(1).toDouble))
    val rateViewsDiCap = viewsDiCap.sum() / allViews.sum()
    println("Les vues de Di Caprio représentent " + rateViewsDiCap*100 + "% des vues totales de notre échantillon")

    //Question 5 : Quelle est la moyenne des notes et des vues par acteur dans cet échantillon ?

    println("Moyenne des notes par acteur : ")
    val pairRDDnotes = rdd.map(item => (item.split(";")(3).toString, (item.split(";")(2).toDouble)) )

    pairRDDnotes.groupByKey().collect().foreach(x => {
      val i = x._2.iterator
      var sum = 0d
      var count = 0
      while (i.hasNext) {
        sum += i.next().toDouble
        count += 1
      }
      println(x._1 + " : " + sum / count)
    })

    println("Moyenne des vues par acteur : ")
    val pairRDDViews = rdd.map(item => (item.split(";")(3).toString, (item.split(";")(1).toDouble)) )
    pairRDDViews.groupByKey().collect().foreach(x => {
      val i = x._2.iterator
      var sum = 0L
      var count = 0
      while (i.hasNext) {
        sum += i.next().toLong
        count += 1
      }
      println(x._1 + " : " + sum / count)
    })
  }
}







    //Question 6 :


    //    val rdd1 = sparkSession.sparkContext.textFile("data/people.csv")
    //    val suprdd = rdd1.filter(elem => !elem.startsWith("A") || elem.split(";")(1).toDouble > 2)
    //    suprdd.foreach(println)
    //    println(suprdd.count())
    //    val counts = suprdd.map(item => (item.split(";")(2).toDouble, (1.0, item.split(";")(1).toDouble)) )
    //    val countSums = counts.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2 ))
    //    val keyMeans = countSums.mapValues(avgCount => avgCount._2 / avgCount._1)
    //    keyMeans.foreach(println)


    //Récupérer des éléments d'une colonne
    //import sparkSession.implicits._

    //val stringValues = df.select(col("Country")).as[String].collect()(0)
    //stringValues.distinct.foreach(println)

    //    val rdd = sparkSession.sparkContext.textFile("data/people.csv")
    //    val nom = rdd.map((elem:String) => elem.split(";")(1))
    //    val rowsupp = nom.filter(elem => !elem.startsWith("B") || elem.split(";")(1).toDouble > 2)
    //    rowsupp.foreach(println)
    //    println(rowsupp.count())


