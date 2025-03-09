package org.example;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;


public class ArquitecturaSpark{

    public static void main(String[] args) {
        // 1️⃣ DRIVER PROGRAM: Inicializa la aplicación Spark
        SparkSession spark = SparkSession.builder()
                .appName("LecturaArchivoTXT")
                .master("local[*]") // Ejecuta localmente usando todos los núcleos disponibles
                .getOrCreate();

        // 2️⃣ DRIVER: Crea el contexto Spark (necesario para trabajar con RDDs)
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        // 3️⃣ DRIVER: Lee el archivo TXT y lo convierte en un RDD distribuido
        JavaRDD<String> rddTexto = sc.textFile("src/main/input/data.txt");

        // 4️⃣ TRANSFORMACIÓN (EXECUTORS): Convierte cada línea a mayúsculas en paralelo
        JavaRDD<String> rddMayusculas = rddTexto.map(linea -> linea.toUpperCase());

        // 5️⃣ ACCIÓN (EXECUTORS): Recoge los resultados y los envía de vuelta al DRIVER
        rddMayusculas.collect().forEach(System.out::println);

        // Esperar para inspeccionar la interfaz web
        //System.out.println("🖥️ Spark Web UI disponible en http://localhost:4040");
        //System.out.println("Presiona Enter para cerrar Spark...");
        //new java.util.Scanner(System.in).nextLine();

        // 6️⃣ Cerrar Spark
        sc.close();
    }
}

