import org.apache.log4j.Logger
import org.apache.log4j.Level

Logger.getLogger("org").setLevel(Level.ERROR)
Logger.getLogger("akka").setLevel(Level.ERROR)

import org.apache.spark.graphx._
import org.apache.spark.rdd._

import scala.io.Source

class PlaceNode(val name: String) extends Serializable

case class Metro(override val name: String, population: Int) extends PlaceNode(name)

case class Country(override val name: String) extends PlaceNode(name)

val metros: RDD[(VertexId, PlaceNode)] =
  sc.textFile("./EOADATA/metro.csv").
    filter(! _.startsWith("#")).
    map {line =>
      val row = line split ','
      (0L + row(0).toInt, Metro(row(1), row(2).toInt))
    }

val countries: RDD[(VertexId, PlaceNode)] =
  sc.textFile("./EOADATA/country.csv").
    filter(! _.startsWith("#")).
    map {line =>
      val row = line split ','
      (100L + row(0).toInt, Country(row(1)))
    }


val mclinks: RDD[Edge[Int]] =
  sc.textFile("./EOADATA/metro_country.csv").
    filter(! _.startsWith("#")).
    map {line =>
      val row = line split ','
      Edge(0L + row(0).toInt, 100L + row(1).toInt, 1)
    }


val nodes = metros ++ countries

val metrosGraph = Graph(nodes, mclinks)

metrosGraph.vertices.take(5)
metrosGraph.edges.take(5)

metrosGraph.edges.filter(_.srcId == 1).map(_.dstId).collect()
metrosGraph.edges.filter(_.dstId == 103).map(_.srcId).collect()





// Degree Histogram


metrosGraph.numEdges
metrosGraph.numVertices
def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
  if (a._2 > b._2) a else b
}
def min(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
  if (a._2 <= b._2) a else b
}


metrosGraph.outDegrees.reduce(max)
metrosGraph.vertices.filter(_._1 == 5).collect()
metrosGraph.inDegrees.reduce(max)
metrosGraph.vertices.filter(_._1 == 108).collect()

metrosGraph.outDegrees.filter(_._2 <= 1).count
metrosGraph.degrees.reduce(max)
metrosGraph.degrees.reduce(min)
metrosGraph.degrees.
  filter { case (vid, count) => vid >= 100 }. // Apply filter so only VertexId < 100 (countries) are included
  map(t => (t._2,t._1)).
  groupByKey.map(t => (t._1,t._2.size)).
  sortBy(_._1).collect()






// Plot

import breeze.linalg._
import breeze.plot._

def degreeHistogram(net: Graph[PlaceNode, Int]): Array[(Int, Int)] =
  net.degrees.
    filter { case (vid, count) => vid >= 100 }.
    map(t => (t._2,t._1)).
    groupByKey.map(t => (t._1,t._2.size)).
    sortBy(_._1).collect()

val nn = metrosGraph.vertices.filter{ case (vid, count) => vid >= 100 }.count()
val metroDegreeDistribution = degreeHistogram(metrosGraph).map({case(d,n) => (d,n.toDouble/nn)})

val f = Figure()
val p1 = f.subplot(2,1,0)
val x = new DenseVector(metroDegreeDistribution map (_._1.toDouble))
val y = new DenseVector(metroDegreeDistribution map (_._2))

p1.xlabel = "Degrees"
p1.ylabel = "Distribution"
p1 += plot(x, y)
p1.title = "Degree distribution"

val p2 = f.subplot(2,1,1)
val metrosDegrees = metrosGraph.degrees.filter { case (vid, count) => vid >= 100 }.map(_._2).collect()

p2.xlabel = "Degrees"
p2.ylabel = "Histogram of node degrees"
p2 += hist(metrosDegrees, 20)


// Connectedness & Clustering

case class Continent(override val name: String) extends PlaceNode(name)
val continents: RDD[(VertexId, PlaceNode)] =
  sc.textFile("./EOADATA/continent.csv").
    filter(! _.startsWith("#")).
    map {line =>
      val row = line split ','
      (200L + row(0).toInt, Continent(row(1))) // Add 200 to the VertexId to keep the indexes unique
    }

val cclinks: RDD[Edge[Int]] =
  sc.textFile("./EOADATA/country_continent.csv").
    filter(! _.startsWith("#")).
    map {line =>
      val row = line split ','
      Edge(100L + row(0).toInt, 200L + row(1).toInt, 1)
    }

val cnodes = metros ++ countries ++ continents
val clinks = mclinks ++ cclinks
val countriesGraph = Graph(cnodes, clinks)


import org.graphstream.graph.implementations._
val graph: SingleGraph = new SingleGraph("countriesGraph")
graph.addAttribute("ui.stylesheet","url(file:.//style/stylesheet)")
graph.addAttribute("ui.quality")
graph.addAttribute("ui.antialias")

for ((id:VertexId, place:PlaceNode) <- countriesGraph.vertices.collect())
{
  val node = graph.addNode(id.toString).asInstanceOf[SingleNode]
  node.addAttribute("name", place.name)
  node.addAttribute("ui.label", place.name)

  if (place.isInstanceOf[Metro])
    node.addAttribute("ui.class", "metro")
  else if(place.isInstanceOf[Country])
    node.addAttribute("ui.class", "country")
  else if(place.isInstanceOf[Continent])
    node.addAttribute("ui.class", "continent")
}

for (Edge(x,y,_) <- countriesGraph.edges.collect()) {
  graph.addEdge(x.toString ++ y.toString, x.toString, y.toString, true).asInstanceOf[AbstractEdge]
}

graph.display()


// Joining graph datasets



