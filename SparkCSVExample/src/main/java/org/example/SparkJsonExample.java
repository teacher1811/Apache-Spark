package org.example;
import org.apache.spark.sql.*;


public class SparkJsonExample {
    public static void main(String[] args) {


        // 1️⃣ Crear SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Product Analysis")
                .master("local[*]") // Para ejecución local
                .getOrCreate();

        // 2️⃣ Leer el archivo JSON como DataFrame
        Dataset<Row> df = spark.read()
                .option("multiline", "true") // Soporta JSON con múltiples líneas
                .json("src/main/input/products.json");

        // 3️⃣ Mostrar los datos cargados
        System.out.println("📌 Datos originales:");
        df.show();

        // 4️⃣ Mostrar el esquema del DataFrame
        System.out.println("📌 Esquema de la tabla:");
        df.printSchema();

        // 5️⃣ Seleccionar solo nombre y precio de los productos
        System.out.println("📌 Productos con nombre y precio:");
        df.select("name", "price").show();

        // 6️⃣ Filtrar productos con precio mayor a 100
        System.out.println("📌 Productos con precio mayor a 100:");
        df.filter("price > 100").show();

        // 7️⃣ Ordenar productos por precio de forma descendente
        System.out.println("📌 Productos ordenados por precio (mayor a menor):");
        df.orderBy(df.col("price").desc()).show();

        // 8️⃣ Crear una vista temporal y ejecutar SQL
        df.createOrReplaceTempView("products");

        System.out.println("📌 Consulta SQL: Productos de la categoría 'Electronics'");
        Dataset<Row> electronics = spark.sql("SELECT * FROM products WHERE category = 'Electronics'");
        electronics.show();

        // Cerrar sesión Spark
        spark.stop();
    }
}
