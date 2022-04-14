import org.apache.log4j.Logger
import org.apache.log4j.Level

Logger.getLogger("org").setLevel(Level.ERROR)
Logger.getLogger("akka").setLevel(Level.ERROR)

import org.apache.spark.graphx._
import org.apache.spark.rdd._

val airports: RDD[(VertexId, String)] = sc.parallelize(
    List((1L, "Los Angeles International Airport"),
      (2L, "Narita International Airport"),
      (3L, "Singapore Changi Airport"),
      (4L, "Charles de Gaulle Airport"),
      (5L, "Toronto Pearson International Airport")))

val flights: RDD[Edge[String]] = sc.parallelize(
  List(Edge(1L,4L,"AA1123"),
    Edge(2L, 4L, "JL5427"),
    Edge(3L, 5L, "SQ9338"),
    Edge(1L, 5L, "AA6653"),
    Edge(3L, 4L, "SQ4521")))

val flightGraph = Graph(airports, flights)
flightGraph.triplets.foreach(t => println("Departs from: " + t.srcAttr + " - Arrives at: " + t.dstAttr + " - Flight Number: " + t.attr))

case class AirportInformation(city: String, code: String)
defined class AirportInformation
val airportInformation: RDD[(VertexId, AirportInformation)] = sc.parallelize(
  List((2L, AirportInformation("Tokyo", "NRT")),
    (3L, AirportInformation("Singapore", "SIN")),
    (4L, AirportInformation("Paris", "CDG")),
    (5L, AirportInformation("Toronto", "YYZ")),
    (6L, AirportInformation("London", "LHR")),
    (7L, AirportInformation("Hong Kong", "HKG"))))

// Join
def appendAirportInformation(id: VertexId, name: String, airportInformation: AirportInformation): String = name + ":"+ airportInformation.city
val flightJoinedGraph =  flightGraph.joinVertices(airportInformation)(appendAirportInformation)
flightJoinedGraph.vertices.foreach(println)

val flightOuterJoinedGraph = flightGraph.outerJoinVertices(airportInformation)((_,name, airportInformation) => (name, airportInformation))
flightOuterJoinedGraph.vertices.foreach(println)
val flightOuterJoinedGraphTwo = flightGraph.outerJoinVertices(airportInformation)((_, name, airportInformation) => (name, airportInformation.getOrElse(AirportInformation("NA","NA"))))
flightOuterJoinedGraphTwo.vertices.foreach(println)

// New returned type for joined vertices
case class Airport(name: String, city: String, code: String)
  val flightOuterJoinedGraphThree = flightGraph.outerJoinVertices(airportInformation)((_, name, b) => b match {
  case Some(airportInformation) => Airport(name, airportInformation.city, airportInformation.code)
  case None => Airport(name, "", "")
})
flightOuterJoinedGraphThree.vertices.foreach(println)


