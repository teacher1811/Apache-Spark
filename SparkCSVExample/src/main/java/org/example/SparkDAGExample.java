package org.example;
import org.apache.spark.sql.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkDAGExample {

    public static void main(String[] args) throws InterruptedException {
        // 1️⃣ Configuración de Spark
        SparkConf conf = new SparkConf().setAppName("DAGExample").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // 2️⃣ Cargar un archivo CSV en un DataFrame
        Dataset<Row> empleados = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/input/employees.csv");

        // 3️⃣ Transformaciones (se agregan al DAG pero no se ejecutan aún)
        Dataset<Row> empleadosFiltrados = empleados.filter("salary > 3000");
        Dataset<Row> empleadosSeleccionados = empleadosFiltrados.select("name", "salary");

        // 4️⃣ Acción: Se ejecuta el DAG en este momento
        empleadosSeleccionados.show();

        // 5️⃣ Mantener el programa abierto para inspeccionar el DAG en Spark UI
        System.out.println("🖥️ Spark UI disponible en http://localhost:4040");
        new java.util.Scanner(System.in).nextLine();

        // 6️⃣ Cerrar Spark
        sc.close();
        spark.close();
    }
}
