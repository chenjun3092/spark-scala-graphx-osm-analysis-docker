//
// this source code is taken from (based on ) http://raazesh-sainudiin.github.io/scalable-data-science/db/studentProjects/01_DillonGeorge/039_OSMMap2GraphX.html
//


import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map

// map is not available on Java collections inorder to make it available import JavaConversions
import scala.collection.JavaConversions._

import crosby.binary.osmosis.OsmosisReader

import org.apache.hadoop.mapreduce.{TaskAttemptContext, JobContext}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configured

import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer
import org.openstreetmap.osmosis.core.domain.v0_6._
//import org.openstreetmap.osmosis.core.domain.v0_6.Node;
//import org.openstreetmap.osmosis.core.domain.v0_6.Way;
//import org.openstreetmap.osmosis.core.domain.v0_6.Relation;
//import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.openstreetmap.osmosis.core.task.v0_6.Sink

import scala.reflect.ClassTag

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.sql.SQLContext

import com.esri.core.geometry.GeometryEngine.geodesicDistanceOnWGS84
import com.esri.core.geometry.Point

import org.apache.spark.graphx.lib.ShortestPaths
import scala.Tuple2
import shapeless.syntax.std.tuple._

object Analysis {

  	def main(args: Array[String]) {
   
    		val conf = new SparkConf().setAppName("Graphx Analysis").setMaster("local")
    		val sc = new SparkContext(conf)

   		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		

		val allowableWays = Set(
  					"motorway",
  					"motorway_link",
  					"trunk",
  					"trunk_link",
  					"primary",
  					"primary_link",
  					"secondary",
  					"secondary_link",
  					"tertiary",
  					"tertiary_link",
  					"living_street",
  					"residential",
  					"road",
  					"construction",
  					"motorway_junction"
		)

		val fs_conf = new Configuration()
		val fs_default = fs_conf.get("fs.defaultFS")
		println("fs default = " + fs_conf.get("fs.defaultFS"))
		// fs default = file:///		

		val fs = FileSystem.get(new Configuration())
		val path = new Path("///temp/faroe-islands-latest.osm.pbf")
		println("... reading ...")
		val file = fs.open(path)
		println("... done")

		var nodes: ArrayBuffer[Node] = ArrayBuffer()
		var ways: ArrayBuffer[Way] = ArrayBuffer()
		var relations: ArrayBuffer[Relation] = ArrayBuffer()

		val osmosisReader = new OsmosisReader(file)
		
		osmosisReader.setSink(
			new Sink {
  				override def process(entityContainer: EntityContainer): Unit = {

    					if (entityContainer.getEntity.getType != EntityType.Bound) {
      						val entity = entityContainer.getEntity
      						entity match {
      							case node: Node => nodes += node
      							case relation: Relation => relations += relation
      							case way: Way => {
        							val tagSet = way.getTags.map(_.getValue).toSet
        							// way has at least one tag of interest
        							if ( !(tagSet & allowableWays).isEmpty ) {
          								ways += way
        							}
      							}
						}
    					}
				}

				override def initialize(map: java.util.Map[String, AnyRef]): Unit = {
    					// Ensure our entity buffers are empty before processing
    					nodes = ArrayBuffer()
    					ways = ArrayBuffer()
    					relations = ArrayBuffer()
  				}

  				override def complete(): Unit = {}

 		 		override def release(): Unit = {}
			}
		)

		osmosisReader.run()

		println(ways.head)
		println(ways.head.getTags.toList)

		// Way(id=4965671, #tags=2, name='Vagli?')
		// List(Tag('name'='Vagli?'), Tag('highway'='residential'))

		
		println(nodes.head)
		println(nodes.head.getTags.toList)

		// Node(id=29023724, #tags=6, name='Akrar')
		// List(Tag('name'='Akrar'), Tag('is_in'='Sumba (Su?uroy), F?royar'), Tag('place'='village'), Tag('wikidata'='Q420746'), Tag('population'='28'), Tag('postal_code'='927'))

		
		// org.apache.spark.sql.Dataset
		import sqlContext.implicits._
		val nodeDS = nodes.map{node => 
  						NodeEntry(node.getId, 
       							node.getLatitude, 
       							node.getLongitude, 
       							node.getTags.map(_.getValue).toArray
                                                )
                                      }.toDS.cache
		
		nodeDS.show()
		// +--------+------------------+-------------------+--------------------+
		//|  nodeId|          latitude|          longitude|                tags|
		//+--------+------------------+-------------------+--------------------+
		//|29023724|61.455746000000005|         -6.7590335|[Akrar, Sumba (Su...|
		//|29023725|62.254764300000005|         -6.5781653|[?nirnar, Klaksv?...|
		//|29023727|            62.255|             -6.533|[?rnafj?r?ur, Kla...|
		//|29023728|62.086051600000005|         -7.3692636|[B?ur, B?ggjar Ko...|
		//|29023729|61.784322100000004|-6.6785038000000005|[Dalur, H?sav?k (...|
		//|29023730|62.286638800000006|         -6.5299448|[Depil, Hvannasun...|
		//|29023731|        61.6849993|         -6.7558718|[D?mun, Sk?voy (S...|
		//|29023732|62.300426400000006|-7.0894368000000005|[Ei?i, Eysturoy, ...|
		//|29023733|62.281417700000006|          -6.911594|[Elduv?k, Elduv?k...|
		//|29023734|         61.524861| -6.878636800000001|[F?mjin, F?mjin (...|
		//|29023735|61.547000000000004| -6.771000000000001|[Fro?ba, Tv?royri...|
		//|29023736|62.244022900000004|         -6.8141971|[Fuglafj?r?ur, Fu...|
		//|29023737|62.237829100000006|          -6.929513|[Funningsfj?r?ur,...|
		//|29023738|        62.2869145|         -6.9680448|[Funningur, Funni...|
		//|29023739|62.110486800000004|         -7.4370516|[G?sadalur, B?ggj...|
		//|29023740|62.130804700000006|          -6.725463|[Glyvrar, Eysturo...|
		//|29023741| 62.32486170000001| -6.948243700000001|[Gj?gv, Gj?gv (Ey...|
		//|29023742|        62.1918816| -6.748710300000001|[G?tugj?gv, G?tu ...|
		//|29023743|62.175498000000005| -6.768989400000001|[G?tuei?i, G?tu K...|
		//|29023744|62.270046300000004|         -6.6026952|[Haraldssund, Kun...|
		//+--------+------------------+-------------------+--------------------+


		val wayDS = ways.map{way => 
  						WayEntry(way.getId,
      							way.getTags.map(_.getValue).toArray,
      							way.getWayNodes.map(_.getNodeId).toArray
                                                )
				    }.toDS.cache

		wayDS.show()
		
		//+-------+--------------------+--------------------+
		//|  wayId|                tags|               nodes|
		//+-------+--------------------+--------------------+
		//|4965671|[Vagli?, resident...|[564832208, 41719...|
		//|4965672|[Koparg?ta, yes, ...|[564832205, 61261...|
		//|4965675|[Var?ag?ta, no, r...|[32884979, 329474...|
		//|4965713|[Havnarg?ta, resi...|[32885555, 328976...|
		//|4965800|[V?gsbotnur, resi...|[32885827, 328858...|
		//|4965804|[Gr?ms Kambans g?...|[32885937, 328859...|
		//|4967426|[Sk?latr??, resid...|[32897683, 328976...|
		//|4967428|[Havnarg?ta, yes,...|[32897700, 539127...|
		//|4967429|[Yviri vi? Strond...|[32897704, 303582...|
		//|4967430|   [residential, 50]|[32897701, 328977...|
		//|4967432|[Yviri vi? Strond...|[32897703, 328977...|
		//|4967433|[Hv?tanesvegur, s...|[32897795, 328977...|
		//|4967435|[Hv?tanesvegur, y...|[32897941, 566451...|
		//|4967437|[10, Hv?tanesvegu...|[32897939, 369792...|
		//|4967439|[Vegurin Langi, n...|[32897941, 328980...|
		//|4967440|[Sundsvegur, no, ...|[2184401620, 4710...|
		//|4967441|[V.U. Hammershaim...|[32898218, 328982...|
		//|4972589|[B?kjarabrekka, n...|[32946858, 329468...|
		//|4972590|[R.C. Effers?es g...|[32946858, 329468...|
		//|4972595|[Egholmstr??, no,...|[363849135, 36384...|
		//+-------+--------------------+--------------------+

		val nodeCounts = wayDS
				 .flatMap(_.nodes)
				 //.groupBy(node => identity(node))
				 .groupByKey(identity)
				 // or groupBy("value")
				 .count
		
		nodeCounts.collect

		nodeCounts.show()

		//+---------+--------+
		//|    value|count(1)|
		//+---------+--------+
		//| 32898027|       1|
		//| 33253241|       1|
		//| 33253250|       1|
		//|302771931|       3|
		//|302771941|       1|
		//|394860517|       1|
		//|305452274|       1|
		//|305452307|       1|
		//|307696638|       1|
		//|307702309|       1|
		//|307705173|       1|
		//|307705458|       1|
		//|569830699|       2|
		//|310014716|       1|
		//|316536686|       2|
		//|316536768|       1|
		//|793070637|       1|
		//|466326950|       1|
		//|331546657|       1|
		//|331546684|       1|
		//+---------+--------+
		//only showing top 20 rows

		
		// intersection nodes are found by filtering out all ids with a count less than two
		// remove the count field from dataset
		// ._2 selects the second element in a tuple 
		val intersectionNodes = nodeCounts.filter(_._2 >= 2).map(_._1)	
	
		// collection.map(_._2) emits a second component of the tuple

		intersectionNodes.show()
		
		//+----------+
		//|     value|
		//+----------+
		//| 302771931|
		//| 569830699|
		//| 316536686|
		//| 563848141|
		//|3452032799|
		//| 438222804|
		//| 567217965|
		//| 567231100|
		//| 432697135|
		//| 564832208|
		//| 305452188|
		//| 392519975|
		//| 410039023|
		//| 475339609|
		//| 485658054|
		//| 559478134|
		//| 566650007|
		//| 566811424|
		//| 316463628|
		//| 331545676|
		//+----------+
		//only showing top 20 rows


		println("intersectionNodes = " + intersectionNodes)
		//intersectionNodes = [value: bigint]
		
		// Sets are Iterables that contain no duplicate elements
		//
		// scala> val fruit = Set("apple", "orange", "peach", "banana")
    		// fruit: scala.collection.immutable.Set[java.lang.String] = Set(apple, orange, peach, banana)
    		// scala> fruit("peach")
    		// res0: Boolean = true
    		// scala> fruit("potato")
    		// res1: Boolean = false
		

		// keep a read-only variable cached on each machine rather than shipping a copy of it with tasks.
		val intersectionSet = sc.broadcast(intersectionNodes.collect.toSet)

		intersectionSet.value.take(10).foreach(println)

				
		val wayNodeIds = wayDS.flatMap(_.nodes).distinct

		wayNodeIds.show()

		//+---------+
		//|    value|
		//+---------+
		//| 32898027|
		//| 33253241|
		//| 33253250|
		//|302771931|
		//|302771941|
		//|394860517|
		//|305452274|
		//|305452307|
		//|307696638|
		//|307702309|
		//|307705173|
		//|307705458|
		//|569830699|
		//|310014716|
		//|316536686|
		//|316536768|
		//|793070637|
		//|466326950|
		//|331546657|
		//|331546684|
		//+---------+
		//only showing top 20 rows


		// join nodeDS with wayNodeIds
		val wayNodes = nodeDS.as("nodes")
  				.joinWith(wayNodeIds.as("ways"), $"ways.value" === $"nodes.nodeId")
  				.map(_._1).cache

		wayNodes.show()

		//+--------+------------------+-------------------+----+
		//|  nodeId|          latitude|          longitude|tags|
		//+--------+------------------+-------------------+----+
		//|32884410|        62.0101718|-6.7717803000000005|  []|
		//|32884411|62.010621500000006| -6.772577600000001|  []|
		//|32884412| 62.01116880000001| -6.773545800000001|  []|
		//|32884414|         62.012163|         -6.7747314|  []|
		//|32884935| 62.01234530000001|             -6.775|  []|
		//|32884936|         62.012447|         -6.7752521|  []|
		//|32884937|62.012517800000005|         -6.7755269|  []|
		//|32884938| 62.01255510000001|         -6.7757439|  []|
		//|32884939|62.012570200000006|         -6.7758368|  []|
		//|32884940|        62.0126135|         -6.7759818|  []|
		//|32884941|         62.012689|         -6.7761408|  []|
		//|32884943| 62.01018070000001|          -6.771522|  []|
		//|32884944| 62.01014060000001|         -6.7711926|  []|
		//|32884945|        62.0100783| -6.770889800000001|  []|
		//|32884972|62.009958100000006|         -6.7706049|  []|
		//|32884973|        62.0098245|         -6.7702755|  []|
		//|32884974|62.009625500000006|         -6.7699334|  []|
		//|32884975|        62.0094963|         -6.7697532|  []|
		//|32884976| 62.00925110000001| -6.769325500000001|  []|
		//|32884977|        62.0095668|-6.7695525000000005|  []|
		//+--------+------------------+-------------------+----+
	

		println("wayNodes = " + wayNodes)
		
		// case class LabeledWay(wayId: Long, labeledNodes: Array[(Long, Boolean)])

		val labeledWays = wayDS.map{ way => {
  							val nodesWithLabels = way.nodes.map(
									id => (id, intersectionSet.value contains id)
									)
    						
							nodesWithLabels(nodesWithLabels.size - 1) = (nodesWithLabels.last._1, true) 
  							nodesWithLabels(0) = (nodesWithLabels.head._1, true)
  							LabeledWay(way.wayId, nodesWithLabels)
					        }
				  }.cache

		labeledWays.show()

		//+-------+--------------------+
		//|  wayId|        labeledNodes|
		//+-------+--------------------+
		//|4965671|[[564832208,true]...|
		//|4965672|[[564832205,true]...|
		//|4965675|[[32884979,true],...|
		//|4965713|[[32885555,true],...|
		//|4965800|[[32885827,true],...|
		//|4965804|[[32885937,true],...|
		//|4967426|[[32897683,true],...|
		//|4967428|[[32897700,true],...|
		//|4967429|[[32897704,true],...|
		//|4967430|[[32897701,true],...|
		//|4967432|[[32897703,true],...|
		//|4967433|[[32897795,true],...|
		//|4967435|[[32897941,true],...|
		//|4967437|[[32897939,true],...|
		//|4967439|[[32897941,true],...|
		//|4967440|[[2184401620,true...|
		//|4967441|[[32898218,true],...|
		//|4972589|[[32946858,true],...|
		//|4972590|[[32946858,true],...|
		//|4972595|[[363849135,true]...|
		//+-------+--------------------+
		//only showing top 20 rows


		val n_of_intersectionNodes = labeledWays.map{way => 
						(way.wayId, way.labeledNodes.filter{_._2}.length)
					     }

		n_of_intersectionNodes.show()

		
		//+-------+---+
		//|     _1| _2|
		//+-------+---+
		//|4965671|  2|
		//|4965672|  3|
		//|4965675|  6|
		//|4965713|  2|
		//|4965800|  2|
		//|4965804|  2|
		//|4967426|  2|
		//|4967428|  4|
		//|4967429|  5|
		//|4967430|  2|
		//|4967432|  2|
		//|4967433|  3|
		//|4967435|  4|
		//|4967437|  2|
		//|4967439|  3|
		//|4967440|  4|
		//|4967441|  3|
		//|4972589|  2|
		//|4972590|  4|
		//|4972595|  2|
		//+-------+---+
		//only showing top 20 rows
		
		//// case class Intersection(OSMId: Long , inBuf: ArrayBuffer[Long], outBuf: ArrayBuffer[Long])	        

		def segmentWay(way: Array[(Long, Boolean)]): Array[(Long, Array[Long], Array[Long])] = {
  
			// RDD's zipWithIndex() method seems to do this, but it won't preserve the original ordering of the data the RDD was created from. At least you'll get a stable ordering. 
  			val indexedNodes: Array[((Long, Boolean), Int)] = way.zipWithIndex
  
  			val intersections = ArrayBuffer[Intersection]()
  
  			val currentBuffer = ArrayBuffer[Long]()
  
  			// Only one node in the way
  			if (way.length == 1) {
    				val intersect = new Intersection(way(0)._1, ArrayBuffer(-1L), ArrayBuffer(-1L))
    				return Array((intersect.OSMId, intersect.inBuf.toArray, intersect.outBuf.toArray))
  			}
  
  			indexedNodes.foreach{ case ((id, isIntersection), i) =>
    				if (isIntersection) {
      					val newEntry = new Intersection(id, currentBuffer.clone, ArrayBuffer[Long]())
      					intersections += newEntry
      					currentBuffer.clear
    				}
    				else {
        				currentBuffer += id
    				}
    
    				
    				if (i == way.length - 1 && !currentBuffer.isEmpty) {
      					if (intersections.isEmpty) intersections += new Intersection(-1L, ArrayBuffer[Long](), currentBuffer)
      					else intersections.last.outBuf ++= currentBuffer
      					currentBuffer.clear
    				}
  			}
  			intersections.map(i => (i.OSMId, i.inBuf.toArray, i.outBuf.toArray)).toArray
		}

		val segmentedWays = labeledWays.map(way => (way.wayId, segmentWay(way.labeledNodes)))
		////					   (|4965671|      [[564832208,true]...    |)
		segmentedWays.show()

		// +-------+--------------------+
		//|     _1|                  _2|
		//+-------+--------------------+
		//|4965671|[[564832208,Wrapp...|
		//|4965672|[[564832205,Wrapp...|
		//|4965675|[[32884979,Wrappe...|
		//|4965713|[[32885555,Wrappe...|
		//|4965800|[[32885827,Wrappe...|
		//|4965804|[[32885937,Wrappe...|
		//|4967426|[[32897683,Wrappe...|
		//|4967428|[[32897700,Wrappe...|
		//|4967429|[[32897704,Wrappe...|
		//|4967430|[[32897701,Wrappe...|
		//|4967432|[[32897703,Wrappe...|
		//|4967433|[[32897795,Wrappe...|
		//|4967435|[[32897941,Wrappe...|
		//|4967437|[[32897939,Wrappe...|
		//|4967439|[[32897941,Wrappe...|
		//|4967440|[[2184401620,Wrap...|
		//|4967441|[[32898218,Wrappe...|
		//|4972589|[[32946858,Wrappe...|
		//|4972590|[[32946858,Wrappe...|
		//|4972595|[[363849135,Wrapp...|
		//+-------+--------------------+


		println(segmentedWays.printSchema)

		// root
 		// |-- _1: long (nullable = true)
		// |-- _2: array (nullable = true)
		// |    |-- element: struct (containsNull = true)
		// |    |    |-- _1: long (nullable = true)
		// |    |    |-- _2: array (nullable = true)
		// |    |    |    |-- element: long (containsNull = false)
		// |    |    |-- _3: array (nullable = true)
		// |    |    |    |-- element: long (containsNull = false)
		// 
		
		
		val waySegmentDS = segmentedWays
			.flatMap(way => way._2.map(node => (way._1, node)))	
			.map(node => (node._1, IntersectionNode(node._2._1, node._2._2, node._2._3))) 
		// for each (intersection_nodeId, inBuf, outBuf) => (wayId, IntersectionNode(intersection_nodeId, inBuf, outBuf))

		waySegmentDS.show()

		//+-------+--------------------+
		//|     _1|                  _2|
		//+-------+--------------------+
		//|4965671|[564832208,Wrappe...|
		//|4965671|[32884943,Wrapped...|

		//|4965672|[564832205,Wrappe...|
		//|4965672|[612617149,Wrappe...|
		//|4965672|[612617145,Wrappe...|

		//|4965675|[32884979,Wrapped...|
		//|4965675|[331930182,Wrappe...|
		//|4965675|[331930287,Wrappe...|
		//|4965675|[331930301,Wrappe...|
		//|4965675|[331930330,Wrappe...|
		//|4965675|[306613927,Wrappe...|

		//|4965713|[32885555,Wrapped...|
		//|4965713|[32885559,Wrapped...|

		//|4965800|[32885827,Wrapped...|
		//|4965800|[32885831,Wrapped...|

		//|4965804|[32885937,Wrapped...|
		//|4965804|[32885844,Wrapped...|

		//|4967426|[32897683,Wrapped...|
		//|4967426|[32898222,Wrapped...|
		//|4967428|[32897700,Wrapped...|
		//+-------+--------------------+



		// we have a sequence of way segments we extracted the individual intersection (& nodeId) from segments
		// and make tuples in the form of (intersection_nodeId, Map(WayId, (inBuffer, outBuffer)) )

		val intersectionVertices = waySegmentDS
  					.map(way => (way._2.id, Map(way._1 -> (way._2.in, way._2.out))) )
  					.rdd
  					.reduceByKey(_ ++ _)

					//if values are lists then ++ will concatenate them
					// val rdd = sc.parallelize((1, Set("A")) :: (2, Set("B")) :: (2, Set("C")) :: Nil)
					// val reducedRdd = rdd.reduceByKey(_ ++ _)
					// reducedRdd.collect()
					// Array((1,Set(A)), (2,Set(B, C)))
				
		
		intersectionVertices.take(10).foreach(println)
		
		// (324467343,Map(29439275 -> ([J@c4fc610,[J@2c8469fe), 29439280 -> ([J@78626209,[J@7b9b244e)))
		// (3906819976,Map(387380592 -> ([J@996126b,[J@6d2413e)))
		// (444505024,Map(37877995 -> ([J@6d4b09c7,[J@4f50e974)))
		// (394663688,Map(34378120 -> ([J@76ecbfbf,[J@6223dbed), 34378146 -> ([J@5713e35f,[J@1f40bb80)))
		// (563805636,Map(44361935 -> ([J@c42ee90,[J@70c2a046)))
		// (559477985,Map(44000478 -> ([J@3803bc1a,[J@22858c21), 44000481 -> ([J@39a8c08f,[J@5c0d876c)))
		// (559552998,Map(44006155 -> ([J@794b139b,[J@77217c17), 32232186 -> ([J@87276c4,[J@38f4a641)))
		// (563597273,Map(44347871 -> ([J@1902ad0f,[J@653fbbfc)))
		// (559771433,Map(44028102 -> ([J@7e6025c9,[J@63bb52ea)))
		// (559617764,Map(44013684 -> ([J@6f5df147,[J@7725470b)))
		
		// process the data for the edges of the graph

		val edges = segmentedWays
  				.filter(way => way._2.length > 1) 
  				.flatMap{ case (wayId, ways) => { 
             								ways.sliding(2)
               								.flatMap(segment => 
                              							List(
											Edge(segment(0)._1, segment(1)._1, wayId), 
											Edge(segment(1)._1, segment(0)._1, wayId)
										)
               								)
   								}
					}
		edges.show()

		// +---------+---------+-------+
		// |    srcId|    dstId|   attr|
		// +---------+---------+-------+
		// |564832208| 32884943|4965671|
		// | 32884943|564832208|4965671|

		// |564832205|612617149|4965672|
		// |612617149|564832205|4965672|

		// |612617149|612617145|4965672|
		// |612617145|612617149|4965672|

		// | 32884979|331930182|4965675|
		// |331930182| 32884979|4965675|

		// |331930182|331930287|4965675|
		// |331930287|331930182|4965675|

		// |331930287|331930301|4965675|
		// |331930301|331930287|4965675|

		// |331930301|331930330|4965675|
		// |331930330|331930301|4965675|

		// |331930330|306613927|4965675|
		// |306613927|331930330|4965675|

		// | 32885555| 32885559|4965713|
		// | 32885559| 32885555|4965713|

		// | 32885827| 32885831|4965800|
		// | 32885831| 32885827|4965800|
		// +---------+---------+-------+
		
		

		println(edges)
		
		//  Graph(vertexRDD, edgeRDD)
		//  generate a graph
		val roadGraph = Graph(intersectionVertices, edges.rdd).cache

		roadGraph.edges.take(10).foreach(println)
		
		
		// Edge(32884411,564832208,44437140)
		// Edge(32884411,4321355684,433301785)
		// Edge(32884939,32884941,67827112)
		// Edge(32884939,300757075,27397425)
		// Edge(32884939,1685215956,156260610)
		// Edge(32884941,32884939,67827112)
		// Edge(32884941,32912623,67827112)
		// Edge(32884941,1685215956,156260611)
		// Edge(32884943,564832208,4965671)
		// Edge(32884943,612617145,44437142)

		roadGraph.vertices.take(10).foreach(println)

		// (intersection_nodeId, Map(wayId->(in, out), wayId->(in, out),...))
		// (324467343,Map(29439275 -> ([J@304933cb,[J@7a4199e5), 29439280 -> ([J@6fe55fcf,[J@4d1f6e1c)))
		// (3906819976,Map(387380592 -> ([J@3431cb1f,[J@42c9f2cd)))
		// (444505024,Map(37877995 -> ([J@797c67c,[J@31ce271c)))
		// (394663688,Map(34378120 -> ([J@b1d7b09,[J@64faf3d), 34378146 -> ([J@3f523dae,[J@7fe40b9f)))
		// (563805636,Map(44361935 -> ([J@1f41f259,[J@648c80cb)))
		// (559477985,Map(44000478 -> ([J@4a9d6eb9,[J@157a2c86), 44000481 -> ([J@5b9c74a,[J@5b395ee)))
		// (559552998,Map(44006155 -> ([J@24facb47,[J@6020cd42), 32232186 -> ([J@2b68c59b,[J@326d39fd)))
		// (563597273,Map(44347871 -> ([J@403f6c04,[J@2693e39c)))
		// (559771433,Map(44028102 -> ([J@4bbabec8,[J@1043c493)))
		// (559617764,Map(44013684 -> ([J@2b27d5d3,[J@12115c28)))


		val OSMNodes = wayNodes
			 	.map(node => (node.nodeId, (node.latitude, node.longitude)))
			 	.rdd.collectAsMap
		
		def dist(n1: Long, n2: Long): Double = {
  								val n1Coord = OSMNodes(n1) // lat/lon
  								val n2Coord = OSMNodes(n2) // lat/lon
  								val p1 = new Point(n1Coord._1, n1Coord._2)
  								val p2 = new Point(n2Coord._1, n2Coord._2)

  								geodesicDistanceOnWGS84(p1, p2)
							}
			
		// to add the distance as an edge attribute use mapTriplets to modify each of edge attributes.
		
		// The triplet view logically joins the vertex and edge properties yielding an RDD[EdgeTriplet[VD, ED]] containing instances of the EdgeTriplet class. 

		// The EdgeTriplet class extends the Edge class by adding the srcAttr and dstAttr members which contain the source and destination properties respectively.  src.id, dst.id, src.attr, e.attr, dst.attr
		// val facts: RDD[String] = graph.triplets.map(triplet =>  triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)

		// srcId is the ID of the source vertex, while dstId is the ID of the target vertex. Besides, attr is the attribute associated with the edge.
		// srcAttr is the source vertex attribute, dstAttr is the destination vertex attribute. Therefore, EdgeTriplet contains those five (three are inherited from Edge) basic attributes.

		

		val weightedRoadGraph = roadGraph.mapTriplets{triplet =>
  			val wayNodes = triplet.dstAttr(triplet.attr)._1

			// edge(source_id, destination_id, wayId--> attr)
			// vertex(node_id, Map(wayId->(in, out), wayId->(in, out), ...))
			// i.e.  val wayNodes = triplet.dstMap(wayId)._in
  
  			if (wayNodes.isEmpty) {
    				
    				(triplet.attr, dist(triplet.srcId, triplet.dstId)) // straight distance
  			} else {

				// srcId ---- wayNode.head --+--+ --+ ---wayNode.last ----dstId
				
    				var distance: Double = 0.0
				// def head: T , select the first element of this mutable indexed sequence
    				distance += dist(triplet.srcId, wayNodes.head)
				// def last: T, select the last element
    				distance += dist(triplet.dstId, wayNodes.last)
    
    				if (wayNodes.length > 1) {
      					distance += wayNodes.sliding(2).map{
        							buff => dist(buff(0),buff(1))
							}
        						.reduce(_ + _)
    				}
    
    				(triplet.attr, distance) // (wayId, distance)
  			}
		}.cache

		weightedRoadGraph.edges.take(10).foreach(println)
		
		// Edge(32884411,564832208,(44437140,23.827858806630527))
		// Edge(32884411,4321355684,(433301785,64.04273322226669))
		// Edge(32884939,32884941,(67827112,36.19875623601013))
		// Edge(32884939,300757075,(27397425,264.62843332486153))
		// Edge(32884939,1685215956,(156260610,20.75622889630671))
		// Edge(32884941,32884939,(67827112,36.09337132580516))
		// Edge(32884941,32912623,(67827112,11.041126819467808))
		// Edge(32884941,1685215956,(156260611,37.12479491721125))
		// Edge(32884943,564832208,(4965671,103.27972797782714))
		// Edge(32884943,612617145,(44437142,201.6614049977482))
		

		// weightedRoadGraph.triplets.count

		println("numVertices = "  + weightedRoadGraph.numVertices)
		// numVertices = 3304

		println("numEdges = " + weightedRoadGraph.numEdges)
		// numEdges = 7112

		println( "triplets count =  " + weightedRoadGraph.triplets.count)
		// triplets count =  7112


		val output_edgefilter = weightedRoadGraph.edges.filter { case Edge(src, dst, prop) => src > dst }.count
		println("output_edgefiler = " + output_edgefilter)
		// output_edgefiler = 3536


		val temp_vertices = weightedRoadGraph.vertices.take(5)
		temp_vertices.foreach(println)

		//
		// (324467343,Map(29439275 -> ([J@51338b5,[J@f20f100), 29439280 -> ([J@2f3f10a9,[J@659d72ca)))
		// (3906819976,Map(387380592 -> ([J@7957385c,[J@71f76e87)))
		// (444505024,Map(37877995 -> ([J@261a8248,[J@289faf63)))
		// (394663688,Map(34378120 -> ([J@68caf7f4,[J@22832f3c), 34378146 -> ([J@5079f1e6,[J@5c8a83d)))
		// (563805636,Map(44361935 -> ([J@3a828a27,[J@1c3b884)))
		//
		println("vertices")
		temp_vertices.filter{case (id, attr) => id >= 0}.foreach { case (id, attr) => println("id:"+ s"$id has attr: $attr")}

		//
		// vertices
		// id:324467343 has attr: Map(29439275 -> ([J@5e4a068f,[J@24112ec4), 29439280 -> ([J@6a6082b3,[J@1d591f58))
		// id:3906819976 has attr: Map(387380592 -> ([J@13a91c02,[J@3e6367bd))
		// id:444505024 has attr: Map(37877995 -> ([J@50f80fd8,[J@58e6940))
		// id:394663688 has attr: Map(34378120 -> ([J@5e1c2cff,[J@7b67e60e), 34378146 -> ([J@6bf1b075,[J@1d430022))
		// id:563805636 has attr: Map(44361935 -> ([J@2207bca1,[J@66946979))
		//
		println("intersection_nodeId, attr( wayId, memebr_node)")
		temp_vertices.filter{case (id, attr) => id >= 0}.foreach {
			 case (id, attr) => attr foreach {
							case (key, value) => for(x <- value._1) {println(id + " (" + key + " " + x +")")}
						}
		}

		// intersection_nodeId, attr( wayId, memebr_node)
		// 324467343 (29439280 324467351)
		// 324467343 (29439280 324467347)

		// 394663688 (34378120 394663129)
		// 394663688 (34378120 394663130)
		// 394663688 (34378120 394663131)
		// 394663688 (34378120 394663132)
		// 394663688 (34378120 394663133)
		// 394663688 (34378120 394663134)
		// 394663688 (34378120 394663135)
		// 394663688 (34378120 394663136)
		// 394663688 (34378120 394663137)
		// 394663688 (34378120 394663138)

		// 394663688 (34378146 394663678)
		// 394663688 (34378146 394663679)
		// 394663688 (34378146 394663680)
		// 394663688 (34378146 394682596)
		// 394663688 (34378146 394663681)
		// 394663688 (34378146 394663682)
		// 394663688 (34378146 394683886)
		// 394663688 (34378146 394663683)
		// 394663688 (34378146 394663684)
		// 394663688 (34378146 394663685)
		// 394663688 (34378146 394663686)
		// 394663688 (34378146 394663687)
		//



		for (triplet <- weightedRoadGraph.triplets.take(5)) {
  				println(s"${triplet.srcAttr} likes ${triplet.dstAttr}")
		}

		//
		// Map(433301785 -> ([J@417df2ed,[J@6c3bfd66), 44437140 -> ([J@13093694,[J@43a938fc)) likes Map(4965671 -> ([J@6ded40fe,[J@5e8e40e8), 44437140 -> ([J@469603b,[J@e0dd692))
		// Map(433301785 -> ([J@417df2ed,[J@6c3bfd66), 44437140 -> ([J@13093694,[J@43a938fc)) likes Map(433301785 -> ([J@9608d1d,[J@25c515d9))
		// Map(67827112 -> ([J@6ae2d1e2,[J@a3b336a), 27397425 -> ([J@7be4f55,[J@a58f9a6), 156260610 -> ([J@76c944ce,[J@253a891e)) likes Map(67827112 -> ([J@500bae0b,[J@1dd3e865), 156260611 -> ([J@21002025,[J@50833f62))
		// Map(67827112 -> ([J@6ae2d1e2,[J@a3b336a), 27397425 -> ([J@7be4f55,[J@a58f9a6), 156260610 -> ([J@76c944ce,[J@253a891e)) likes Map(27397425 -> ([J@5d75b88f,[J@5fe7f384), 47705833 -> ([J@7f46e155,[J@3677b9f0), 47705832 -> ([J@46b6ce71,[J@262d6ef0))
		


		//
		for (triplet <- weightedRoadGraph.triplets.take(5)) {

  			println(s"${triplet.srcAttr}")
			triplet.srcAttr foreach {case (key, value) => println (key + "-->" + value)}
		}

		//
		//
		// Map(433301785 -> ([J@73a9f793,[J@3b6d0859), 44437140 -> ([J@32140024,[J@3921b006))
		// 433301785-->([J@73a9f793,[J@3b6d0859)
		// 44437140-->([J@32140024,[J@3921b006)
		// Map(433301785 -> ([J@73a9f793,[J@3b6d0859), 44437140 -> ([J@32140024,[J@3921b006))
		// 433301785-->([J@73a9f793,[J@3b6d0859)
		// 44437140-->([J@32140024,[J@3921b006)
		// Map(67827112 -> ([J@44730d2c,[J@70663424), 27397425 -> ([J@3343f1db,[J@44579647), 156260610 -> ([J@1811f98c,[J@599739be))
		// 67827112-->([J@44730d2c,[J@70663424)
		// 27397425-->([J@3343f1db,[J@44579647)
		// 156260610-->([J@1811f98c,[J@599739be)
		// Map(67827112 -> ([J@44730d2c,[J@70663424), 27397425 -> ([J@3343f1db,[J@44579647), 156260610 -> ([J@1811f98c,[J@599739be))
		// 67827112-->([J@44730d2c,[J@70663424)
		// 27397425-->([J@3343f1db,[J@44579647)
		// 156260610-->([J@1811f98c,[J@599739be)
		// Map(67827112 -> ([J@44730d2c,[J@70663424), 27397425 -> ([J@3343f1db,[J@44579647), 156260610 -> ([J@1811f98c,[J@599739be))
		// 67827112-->([J@44730d2c,[J@70663424)
		// 27397425-->([J@3343f1db,[J@44579647)
		// 156260610-->([J@1811f98c,[J@599739be)
		
		weightedRoadGraph.triplets.take(5).filter(_.attr != 0).foreach(t =>
			t.srcAttr foreach {case (key, value) => println (key + "--->" + value)}
		)

		// 433301785--->([J@114fa25b,[J@54595220)
		// 44437140--->([J@40b9696a,[J@393d64d2)
		// 433301785--->([J@114fa25b,[J@54595220)
		// 44437140--->([J@40b9696a,[J@393d64d2)
		// 67827112--->([J@558c44d4,[J@4bdac417)
		// 27397425--->([J@4023063f,[J@12eae11d)
		// 156260610--->([J@25f7bd70,[J@38903fa5)
		// 67827112--->([J@558c44d4,[J@4bdac417)
		// 27397425--->([J@4023063f,[J@12eae11d)
		// 156260610--->([J@25f7bd70,[J@38903fa5)
		// 67827112--->([J@558c44d4,[J@4bdac417)
		// 27397425--->([J@4023063f,[J@12eae11d)
		// 156260610--->([J@25f7bd70,[J@38903fa5)

		val countTriangles = weightedRoadGraph.triangleCount
		println("triangle count")
		countTriangles.vertices.filter(_._2 > 0).take(10).foreach(println)
	
		//(559477985,1)
		(391665674,1)
		(567231045,1)
		(313360160,1)
		(305451969,1)
		(464687933,1)
		(1685216083,1)
		(392811948,1)
		(32897701,1)
		(32884941,1)

		val connectedNodes = weightedRoadGraph.connectedComponents
		connectedNodes.vertices.take(10).foreach(println)

		//(324467343,32884411)
		//(3906819976,3906819960)
		//(444505024,32884411)
		//(394663688,32884411)
		//(563805636,563805341)
		//(559477985,32884411)
		//(559552998,32884411)
		//(563597273,316463374)
		//(559771433,32884411)
		//(559617764,32884411)

		//val graphXscc = weightedRoadGraph.stronglyConnectedComponents(numIter = 2)
		//graphXscc.cache()
		//graphXscc.vertices.take(5).foreach(println)


		//val result = ShortestPaths.run(weightedRoadGraph, Seq(32884939,32884943))
		val result = dijkstra(weightedRoadGraph, Seq(32884939,32884943))
		result.vertices.map(_._2).collect
		
		

    		sc.stop()
  	}

	case class WayEntry(wayId: Long, tags: Array[String], nodes: Array[Long])
	case class NodeEntry(nodeId: Long, latitude: Double, longitude: Double, tags: Array[String])
	case class LabeledWay(wayId: Long, labeledNodes: Array[(Long, Boolean)])
	case class Intersection(OSMId: Long , inBuf: ArrayBuffer[Long], outBuf: ArrayBuffer[Long])
	case class IntersectionNode(id: Long, in: Array[Long], out: Array[Long])
	
	
	

	// with new vertex attributes
	def dijkstra[VD, ED] (graph: Graph[VD, ED], landmarks:Seq[VertexId]) = {

		// mapVertices can transform each vertex "attribute" in graph
		var g2 = graph.mapVertices{
				(vid, attr) => (false, if (landmarks(1) == vid) 0 else Double.MaxValue )
			}
		
		for(i <- 1L to graph.vertices.count-1){

			val currentVertexId =
				g2.vertices.filter(!_._2._1)
					.fold((0L, (false, Double.MaxValue)))((a,b) =>
								if(a._2._2 < b._2._2) a else b)._1

			// This operator applies a user defined sendMsg function to each edge triplet in the graph 
			// then uses the mergeMsg function to aggregate those messages at their destination vertex
			

			val newDistances = g2.aggregateMessages[Double](
				//send Message
				triplet => { 
						if (triplet.srcId == currentVertexId){
							//// Send message to destination vertex 
							//// triplet.attr._2 is the weight
							triplet.sendToDst(triplet.srcAttr._2 + s"${triplet.attr}".replaceAll ("[()]", "").split(",")(1).toDouble)
								
						}
				},
				// To combine messages
				(a,b) => math.min(a,b)		
	
			)

			g2 = g2.outerJoinVertices(newDistances){

				(vid, attr, newSum) =>

					(attr._1 || vid == currentVertexId,
						math.min(attr._2, newSum.getOrElse(Double.MaxValue)))
			}
			
		}//for

		graph.outerJoinVertices(g2.vertices)(
				(vid, attr, dist) => 
					(attr, dist.getOrElse((false, Double.MaxValue))._2)
		)
		
	}
}
