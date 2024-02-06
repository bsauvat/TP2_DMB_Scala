import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame

// Création d'une session Spark
val spark = SparkSession.builder.appName("BikeTripGraph").getOrCreate()

// Chargement des données CSV dans un DataFrame
val csvPath = "./data/JC-202112-citibike-tripdata.csv"
val df = spark.read.option("header", "true").csv(csvPath)

// Définition du schéma pour les stations
case class Station(station_id: String, name: String, location: String)

// Définition du schéma pour les trajets
case class Trip(trip_id: String, start_station: String, end_station: String, start_time: Long, end_time: Long)

// Fonction pour convertir la date en millisecondes depuis l'Epoch
def timeToLong(date_str: String): Long = {
  // Implémentez votre propre logique pour convertir la date en millisecondes
  // Cette fonction dépend du format de votre date dans le CSV
  0L // Remplacez par votre logique de conversion
}

// Création des noeuds (stations) et relations (trajets)
val stations = df.select("station_id", "name", "location").distinct().as[Station].rdd
val trips = df.as[Trip].rdd

// Création des DataFrames pour les noeuds et relations
val stationsDF = spark.createDataFrame(stations)
val tripsDF = spark.createDataFrame(trips)

// Création du graphframe
val g = GraphFrame(stationsDF, tripsDF)

// Affichage du graphe
g.vertices.show()
g.edges.show()

// Vous pouvez maintenant effectuer des opérations sur le graphe à l'aide de GraphFrames
// Par exemple, pour trouver tous les trajets partant d'une station donnée :
val station_id = "123"
val outgoingTrips = g.edges.filter(s"start_station = '$station_id'")
outgoingTrips.show()

// N'oubliez pas de fermer la session Spark à la fin
spark.stop()
