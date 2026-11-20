import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object CitationPageRank {

  def main(args: Array[String]): Unit = {

    // Step 1: Start Spark
    val spark = SparkSession.builder()
      .appName("GraphX Citation PageRank")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    sc.setLogLevel("ERROR")

    println("Spark Started Successfully")

    // Step 2: Read research papers (vertices)
    val papers: RDD[(VertexId, String)] =
      sc.textFile("data/papers.csv")
        .filter(!_.startsWith("id,"))
        .map { line =>
          val fields = line.split(",")
          (fields(0).toLong, fields(1))
        }

    // Step 3: Read citations (edges): paper src cites paper dst
    val citations: RDD[Edge[Int]] =
      sc.textFile("data/citations.csv")
        .filter(!_.startsWith("src,"))
        .map { line =>
          val fields = line.split(",")
          Edge(fields(0).toLong, fields(1).toLong, 1)
        }

    // Step 4: Create the citation graph
    val graph = Graph(papers, citations)

    println("Citation Graph Created Successfully")

    println("Total Papers: " + graph.numVertices)
    println("Total Citations: " + graph.numEdges)

    // Step 5: Apply PageRank
    val ranks = graph.pageRank(0.0001).vertices

    // Step 6: Combine paper titles and PageRank scores
    val result = papers.join(ranks)
      .map {
        case (id, (title, rank)) =>
          (title, rank)
      }
      .sortBy(_._2, ascending = false)

    // Step 7: Display results
    println("PageRank Results")

    result.collect().foreach {
      case (title, rank) =>
        println(title + " : " + rank)
    }

    // Step 8: Find the most influential paper
    println("Most Influential Paper")

    println(result.first())

    // Step 9: Stop Spark
    spark.stop()
  }
}
