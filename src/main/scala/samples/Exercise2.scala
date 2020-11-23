import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Exercise2 {


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    //Question 1 : Lire le fichier "films.csv" en inférant les types
    val df:DataFrame = sparkSession.read.option("delimiter", ";").option("inferSchema", true).option("header", false).csv("data/donnees.csv")

    //Question 2 : Nommer les colonnes comme suit : nom_film, nombre_vues, note_film, acteur_principal
    val df_renamed = df.withColumnRenamed("_c0","nom_film")
      .withColumnRenamed("_c1", "nombre_vues")
      .withColumnRenamed("_c2", "note_film")
      .withColumnRenamed("_c3", "acteur_principal")

    //Question 3 : Combien y a-t-il de films de Leonardo Di Caprio dans ce fichier ?
    val DiCapMovies = df_renamed.filter(df_renamed("acteur_principal") === "Di Caprio")
    println("Il y a " + DiCapMovies.count() + " films de Leonardo Di Caprio")

    //Question 4 : Quelle est la moyenne des notes des films de Di Caprio ?
    val meanNoteDiCap = DiCapMovies.groupBy("acteur_principal").mean("note_film")
    meanNoteDiCap.show

    //Question 5 : Quel est le pourcentage de vues des films de Di Caprio par rapport à l'échantillon que nous avons ?

    val allViews = df_renamed.agg(sum("nombre_vues")).first.get(0).toString.toDouble

    val DiCapViews = DiCapMovies.agg(sum("nombre_vues")).first.get(0).toString.toDouble

    val DiCapRateViews = DiCapViews / allViews
    println("Le nombre de vues des films de Leonardo Di Caprio représentent " + DiCapRateViews * 100 + "% du total des vues")

    //Question 6 : Quelle est la moyenne des notes et des vues par acteur dans cet échantillon ?

    val meanNoteByActor = df_renamed.groupBy("acteur_principal").avg("note_film").withColumnRenamed("avg(note_film)", "Moyenne des notes par acteur")
    meanNoteByActor.show

    val meanViewsByActor = df_renamed.groupBy("acteur_principal").avg("nombre_vues").withColumnRenamed("avg(nombre_vues)", "Moyenne des vues par acteur")
    meanViewsByActor.show

    //Question 7 : Créer une nouvelle colonne dans ce DataFrame, "pourcentage de vues", contenant le pourcentage de vues pour chaque film
    //(combien de fois les films de cet acteur ont-ils été vus par rapport aux vues globales ?)

    val views_rates_df = df_renamed.withColumn("pourcentage_de_vues", (col("nombre_vues")/ allViews)*100 )
    views_rates_df.show
  }
}

