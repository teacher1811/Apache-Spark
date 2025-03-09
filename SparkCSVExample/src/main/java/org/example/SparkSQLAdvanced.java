package org.example;

import org.apache.spark.sql.*;

public class SparkSQLAdvanced {
    public static void main(String[] args) {
        // Inicializar SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Spark SQL Advanced Problem")
                .master("local[*]") // Usa todos los núcleos disponibles
                .getOrCreate();

        // 📌 Cargar pedidos desde JSON
        Dataset<Row> pedidosDF = spark.read().json("src/main/input/pedidos.json");
        pedidosDF.printSchema();
        pedidosDF.show();

        // 📌 Cargar clientes desde CSV
        Dataset<Row> clientesDF = spark.read()
                .option("header", "true")  // Primera fila como encabezado
                .option("inferSchema", "true") // Inferir tipos de datos
                .csv("src/main/input/clientes.csv");

        clientesDF.printSchema();
        clientesDF.show();

        // 📌 1️⃣ JOIN entre pedidos y clientes (INNER JOIN por id_cliente)
        Dataset<Row> pedidosClientes = pedidosDF
                .join(clientesDF, "id_cliente"); // INNER JOIN

        pedidosClientes.show();

        // 📌 2️⃣ Total de ventas por ciudad
        pedidosClientes.groupBy("ciudad")
                .sum("precio")
                .withColumnRenamed("sum(precio)", "total_ventas")
                .show();

        // 📌 3️⃣ Pedidos con precio mayor a 500€
        pedidosClientes.filter(pedidosClientes.col("precio").gt(500)).show();

        // 📌 4️⃣ Número de compras por cliente (filtrar los que compraron más de 2 veces)
        pedidosClientes.groupBy("id_cliente", "nombre")
                .count()
                .filter("count > 2")
                .withColumnRenamed("count", "total_compras")
                .show();

        // 📌 5️⃣ Producto más vendido
        pedidosClientes.groupBy("producto")
                .count()
                .orderBy(functions.desc("count"))
                .limit(1)
                .show();

        // Detener Spark
        spark.stop();
    }
}
