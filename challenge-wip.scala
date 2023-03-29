import scala.math._
import java.time.LocalDateTime
import scala.collection.mutable.ListBuffer

 

object MyFunctions {
    def piCalc (samples:Int, slice:Int = 1 ) : Unit =  {
    val spark = SparkSession.builder().appName("piCalc").enableHiveSupport().getOrCreate

        //sparkContext()
        val  start = LocalDateTime.now()

        val points = new ListBuffer[(Int, Int)]
        val pointsInsideCircle = new ListBuffer[(Int, Int)]

        for (z <- 1 to samples) {
            //val x:Int = rand.nextInt(5)
            //val y:Int = rand.nextInt(5)

            val x:Int = (Math.random()*(-10)+5).toInt
            val y:Int = (Math.random()*(-10)+5).toInt

            if (sqrt(pow(x,2)+pow(y,2))<=2) {
                pointsInsideCircle.append((x,y))
            } 

            points.append((x,y))

        }

        println(points.size)
        println(pointsInsideCircle.size)



        val pi:Double = 4.00 * (pointsInsideCircle.size.toDouble / points.size)

        val end = LocalDateTime.now()

        println(start)
        println(end)

        val error:Double = abs(pi - Pi) 

        println(s"The value of aproximated pi is: $pi")
        println(s"The error of aproximation is: $error")

    }
}
MyFunctions.piCalc(1000)



//supongamos estos son los argumentos
val muestra = 1000
val slices = 5

// creamos funciones lambda que dan un par random
val rand = () => (Math.random()*(-10)+5).toInt
rand()
val randPair = () => (rand() , rand())
randPair()

// creamos un rdd vacio (Int,Int)
val rdd = spark.sparkContext.emptyRDD[(Int,Int)]
println("re-partition count:"+rdd.getNumPartitions)

// Le hacemos tantas particiones como slices pasamos de argumento
val rddPartition = rdd.repartition(5)
println("re-partition count:"+reparRdd.getNumPartitions)

// Ahora habria que ver como poblar el RDD con los numeros random, habria que usar una funcion map

// ver https://sparkbyexamples.com/spark-rdd-tutorial/
val rddRandomized = rddPartition.map(randPair) // o algo así

// declaramos una funcion lambda para chequear que este en circulo
val isInCircle = (x: Int, y: Int) => (sqrt(pow(x,2)+pow(y,2))<=2)

// Ver como aplicar esta funcion, seguro es un map, y obtener una lista de booleanos
val rddCalculated = rddRandomized.map(isInCircle)