import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.ZoneOffset

// Définition des classes Station et Trip
case class BikeStation(id: Long, name: String, latitude: Double, longitude: Double)
case class BikeTrip(startTime: Long, endTime: Long, startStationId: Long, endStationId: Long)

object Main {


    def main(args: Array[String]): Unit = {

        // ________________________________________________________________________
        // INITIALISATION

        // Configuration de Spark
        val sparkConfig = new SparkConf().setAppName("Bike Share Analysis").setMaster("local")
        val sparkContext = new SparkContext(sparkConfig)
        sparkContext.setLogLevel("ERROR")

        // Lecture du fichier CSV
        val bikeData = sparkContext.textFile("src/data/JC-202112-citibike-tripdata.csv")
        val header = bikeData.first()
        val rows = bikeData.filter(_ != header)


        // ________________________________________________________________________
        // QUESTION 2

        // Conversion des données en objets BikeStation et BikeTrip
        val bikeTrips: RDD[BikeTrip] = rows.map { line =>
            val cols = line.split(",")
            val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
            val startTime = LocalDateTime.parse(cols(2), formatter).toEpochSecond(ZoneOffset.UTC) * 1000
            val endTime = LocalDateTime.parse(cols(3), formatter).toEpochSecond(ZoneOffset.UTC) * 1000
            BikeTrip(startTime, endTime, cols(4).hashCode.toLong, cols(6).hashCode.toLong)
        }

        val bikeTripsRDD: RDD[Edge[BikeTrip]] = bikeTrips.map(t => Edge(t.startStationId, t.endStationId, t))

        val bikeStations: RDD[(VertexId, BikeStation)] = rows.flatMap { line =>
            val cols = line.split(",").map(_.trim)

            // Vérification que les colonnes nécessaires ne sont pas vides
            if (cols.length > 11 && cols(4).nonEmpty && cols(6).nonEmpty && cols(8).nonEmpty && cols(9).nonEmpty && cols(10).nonEmpty && cols(11).nonEmpty) {
                try {
                    Array(
                        (cols(4).hashCode.toLong, BikeStation(cols(4).hashCode.toLong, cols(4), cols(8).toDouble, cols(9).toDouble)),
                        (cols(6).hashCode.toLong, BikeStation(cols(6).hashCode.toLong, cols(6), cols(10).toDouble, cols(11).toDouble))
                    )
                } catch {
                    case _: NumberFormatException => Array.empty[(VertexId, BikeStation)]
                }
            } else {
                Array.empty[(VertexId, BikeStation)]
            }
        }.distinct()

        // Création du graphe
        val defaultBikeStation = BikeStation(0, "Missing", 0.0, 0.0)
        val bikeGraph: Graph[BikeStation, BikeTrip] = Graph(bikeStations, bikeTripsRDD, defaultBikeStation)

        // Dates pour le filtrage
        val startEpoch = LocalDateTime.of(2021, 12, 5, 0, 0).toEpochSecond(ZoneOffset.UTC) * 1000
        val endEpoch = LocalDateTime.of(2021, 12, 25, 23, 59).toEpochSecond(ZoneOffset.UTC) * 1000

        // Extraction du sous-graphe
        val subGraph = bikeGraph.subgraph(epred = e => e.attr.startTime >= startEpoch && e.attr.endTime <= endEpoch)

        // Calcul du nombre de trajets entrants et sortants
        val inDegrees = subGraph.inDegrees
        val outDegrees = subGraph.outDegrees

        // Agréger les degrés par station
        val inDegreesAggregated = inDegrees.map { case (stationId, inDegree) => (stationId, inDegree) }.reduceByKey(_ + _)
        val outDegreesAggregated = outDegrees.map { case (stationId, outDegree) => (stationId, outDegree) }.reduceByKey(_ + _)

        // Joindre avec les stations pour obtenir les noms
        val inDegreesWithNames = inDegreesAggregated.join(bikeStations).map { case (_, (degree, station)) => (station.name, degree) }
        val outDegreesWithNames = outDegreesAggregated.join(bikeStations).map { case (_, (degree, station)) => (station.name, degree) }

        // Appliquer distinct après la jointure
        val distinctInDegrees = inDegreesWithNames.distinct()
        val distinctOutDegrees = outDegreesWithNames.distinct()

        // Affichage des 10 premières stations avec le plus de trajets entrants et sortants
        println("Top 10 stations avec le plus de trajets sortants:")
        distinctOutDegrees.sortBy(_._2, ascending = false).take(10).foreach(println)

        println("\nTop 10 stations avec le plus de trajets entrants:")
        distinctInDegrees.sortBy(_._2, ascending = false).take(10).foreach(println)


        // ________________________________________________________________________
        // QUESTION 3

        def getDistKilometers(lngDegreeA : Double, latDegreeA : Double, lngDegreeB : Double, latDegreeB: Double) : Double = {

            val longRadiansA = Math.toRadians(lngDegreeA)
            val latRadiansA = Math.toRadians(latDegreeA)
            val longRadiansB = Math.toRadians(lngDegreeB)
            val latRadiansB = Math.toRadians(latDegreeB)

            val deltaLon = longRadiansB - longRadiansA
            val deltaLat = latRadiansB - latRadiansA
            val a = Math.pow(Math.sin(deltaLat / 2), 2) +
              Math.cos(latRadiansA) *
                Math.cos(latRadiansB) *
                Math.pow(Math.sin(deltaLon / 2), 2)

            val c = 2 * Math.asin(Math.sqrt(a))

            val r = 6371 // Radius of earth in kilometers
            c*r
        }


        // Trouver la station la plus proche en termes de distance
        val closestStationByDistance = bikeGraph.aggregateMessages[(Long, Double)](
            triplet => {
                if ( triplet.srcId != triplet.dstId) {
                    val dist = getDistKilometers(
                        triplet.srcAttr.longitude, triplet.srcAttr.latitude,
                        triplet.dstAttr.longitude, triplet.dstAttr.latitude
                    )
                    triplet.sendToDst((triplet.srcId, dist))
                }

            },

            (a, b) => if (a._2 < b._2) a else b
        )

        // Trouver la station la plus proche en termes de durée de trajet
        val closestStationByDuration = bikeGraph.aggregateMessages[(Long, Double)](
            triplet => {
                val duration = triplet.attr.endTime - triplet.attr.startTime
                triplet.sendToDst((triplet.srcId, duration.toDouble))
            },
            (a, b) => if (a._2 < b._2) a else b
        )

        println("");

        // Trouver la station la plus proche en termes de distance
        val (closestStationIdByDistance, (_, closestDistanceByDistance)) = closestStationByDistance.filter { case (stationId, _) => stationId != -1622413831 }.min()(Ordering.by[(VertexId, (Long, Double)), Double](_._2._2))
        val closestStationNameByDistance2 = bikeStations.filter { case (stationId, _) => stationId == closestStationIdByDistance }.values.first().name
        println(s"La station la plus proche de JC013 en termes de distance est : $closestStationNameByDistance2, Distance: $closestDistanceByDistance km")

        // Trouver la station la plus proche en termes de durée de trajet
        val (closestStationIdByDuration, (_, closestDurationByDuration)) = closestStationByDuration.filter { case (stationId, _) => stationId != -1622413831 }.min()(Ordering.by[(VertexId, (Long, Double)), Double](_._2._2))
        val closestStationNameByDuration2 = bikeStations.filter { case (stationId, _) => stationId == closestStationIdByDuration }.values.first().name
        println(s"La station la plus proche de JC013 en termes de durée de trajet est : $closestStationNameByDuration2")

        println("")
        println("INFOS BONUS : TOP 10 STATIONS LES PLUS PROCHE DE JC013 EN TERME DE DISTANCE ET DE DUREE DE TRAJET");
        println("")

        // Trouver la station la plus proche en termes de distance

        val closestStationsByDistance = closestStationByDistance
          .filter { case (stationId, _) => stationId != -1622413831 }
          .sortBy { case (_, (_, dist)) => dist }
          .take(10)

        println("Top 10 stations les plus proches de JC013 en termes de distance:")
        closestStationsByDistance.foreach { case (stationId, (_, dist)) =>
            val stationName = bikeStations.lookup(stationId).head.name
            println(s"Station: $stationName, Distance: $dist km")
        }

        // Trouver la station la plus proche en termes de durée de trajet

        val closestStationsByDuration = closestStationByDuration
          .filter { case (stationId, _) => stationId != -1622413831 }
          .sortBy { case (_, (_, duration)) => duration }
          .take(10)

        println("\nTop 10 stations les plus proches de JC013 en termes de durée de trajet:")
        closestStationsByDuration.foreach { case (stationId, (_, duration)) =>
            val stationName = bikeStations.lookup(stationId).head.name
            println(s"Station: $stationName, Durée: $duration secondes")
        }

        // Arrêt de SparkContext
        sparkContext.stop()
    }
}
