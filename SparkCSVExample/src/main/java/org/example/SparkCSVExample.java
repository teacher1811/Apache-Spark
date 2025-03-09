package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;


public class SparkCSVExample {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.INFO);
        Logger.getLogger("akka").setLevel(Level.INFO);

        // 1. Crear una SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Leer CSV en Spark")
                .master("local[*]")  // Usa todos los núcleos disponibles
                .getOrCreate();

        // 2. Leer un archivo CSV
        Dataset<Row> df = spark.read()
                .option("header", "true")  // Indica que el CSV tiene encabezados
                .option("inferSchema", "true")  // Infiera los tipos de datos
                .csv("src/main/input/customers-100.csv");  // Ruta del archivo CSV

        // 3. Mostrar las primeras filas del DataFrame
        df.show();

        // 4. Mostrar el esquema inferido
        df.printSchema();

        // 5. Filtrar datos (Ejemplo: Filtrar por una columna "index" mayor a 30)
        df.filter(df.col("index").gt(30)).show();

        // 6. Finalizar la sesión de Spark
        spark.stop();
    }
}
