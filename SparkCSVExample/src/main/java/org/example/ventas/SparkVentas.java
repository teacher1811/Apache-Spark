package org.example.ventas;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.example.ventas.Venta;

public class SparkVentas {
    public static void main(String[] args) {
        // 1️⃣ 🔥 Crear SparkSession (Driver Program)
        SparkSession spark = SparkSession.builder()
                .appName("Analisis de Ventas")
                .master("local[*]")  // Usa todos los núcleos disponibles
                .getOrCreate();

        // 2️⃣ 🖥️ Obtener SparkContext
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        // 3️⃣ 📂 Leer archivo CSV como DataFrame
        Dataset<Row> df = spark.read()
                .option("header", "true")  // Usa la primera fila como nombres de columna
                .option("inferSchema", "true")  // Detectar tipos automáticamente
                .csv("src/main/input/ventas.csv");

        System.out.println("📊 Datos Originales:");
        df.show();

        // 4️⃣ 🔄 Transformaciones en DataFrame
        Dataset<Row> ventasFiltradas = df.filter("precio > 50");
        ventasFiltradas.show();

        // 5️⃣ 🔄 Convertir DataFrame a RDD y aplicar map()
        JavaRDD<String> productosRDD = df.select("producto").javaRDD()
                .map(row -> "Producto: " + row.getString(0));

        System.out.println("📦 Productos en RDD:");
        productosRDD.collect().forEach(System.out::println);

        // 6️⃣ 🔄 Convertir DataFrame a Dataset<T>
        Encoder<Venta> encoder = Encoders.bean(Venta.class);
        Dataset<Venta> ventasDataset = df.as(encoder);

        System.out.println("📜 Ventas como Dataset:");
        ventasDataset.show();

        // 7️⃣ 📊 Agrupar por categoría y sumar las cantidades
        Dataset<Row> ventasPorCategoria = df.groupBy("categoria")
                .sum("cantidad")
                .withColumnRenamed("sum(cantidad)", "total_vendido");
        ventasPorCategoria.show();

        // 8️⃣ ⚡ Acción - Contar registros
        long totalVentas = df.count();
        System.out.println("📊 Total de Ventas: " + totalVentas);

        //Mantener el programa abierto para inspeccionar el DAG en Spark UI
        System.out.println("🖥️ Spark UI disponible en http://localhost:4040");
        new java.util.Scanner(System.in).nextLine();


        // 9️⃣ 🛑 Cerrar Spark
        spark.stop();
    }
}
