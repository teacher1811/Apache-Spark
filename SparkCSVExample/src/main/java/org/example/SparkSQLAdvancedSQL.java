package org.example;

import org.apache.spark.sql.*;

public class SparkSQLAdvancedSQL {
    public static void main(String[] args) {
        // 🔹 1️⃣ Inicializar SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Spark SQL Advanced with SQL Queries")
                .master("local[*]") // Usa todos los núcleos disponibles
                .getOrCreate();

        // 🔹 2️⃣ Cargar pedidos desde JSON
        Dataset<Row> pedidosDF = spark.read().json("pedidos.json");
        pedidosDF.createOrReplaceTempView("Pedidos"); // Registrar la tabla temporal

        // 🔹 3️⃣ Cargar clientes desde CSV
        Dataset<Row> clientesDF = spark.read()
                .option("header", "true")  // Primera fila como encabezado
                .option("inferSchema", "true") // Inferir tipos de datos
                .csv("clientes.csv");
        clientesDF.createOrReplaceTempView("Clientes"); // Registrar la tabla temporal

        // 🔹 4️⃣ JOIN entre pedidos y clientes (INNER JOIN por id_cliente)
        Dataset<Row> pedidosClientes = spark.sql(
                "SELECT * FROM Pedidos p " +
                        "JOIN Clientes c ON p.id_cliente = c.id_cliente"
        );
        pedidosClientes.show();

        // 🔹 5️⃣ Total de ventas por ciudad
        Dataset<Row> totalVentasPorCiudad = spark.sql(
                "SELECT c.ciudad, SUM(p.precio) AS total_ventas " +
                        "FROM Pedidos p " +
                        "JOIN Clientes c ON p.id_cliente = c.id_cliente " +
                        "GROUP BY c.ciudad"
        );
        totalVentasPorCiudad.show();

        // 🔹 6️⃣ Pedidos con precio mayor a 500€
        Dataset<Row> pedidosCaros = spark.sql(
                "SELECT * FROM Pedidos WHERE precio > 500"
        );
        pedidosCaros.show();

        // 🔹 7️⃣ Número de compras por cliente (filtrar los que compraron más de 2 veces)
        Dataset<Row> comprasFrecuentes = spark.sql(
                "SELECT c.id_cliente, c.nombre, COUNT(p.id_pedido) AS total_compras " +
                        "FROM Pedidos p " +
                        "JOIN Clientes c ON p.id_cliente = c.id_cliente " +
                        "GROUP BY c.id_cliente, c.nombre " +
                        "HAVING COUNT(p.id_pedido) > 2"
        );
        comprasFrecuentes.show();

        // 🔹 8️⃣ Producto más vendido
        Dataset<Row> productoMasVendido = spark.sql(
                "SELECT p.producto, COUNT(*) AS cantidad_vendida " +
                        "FROM Pedidos p " +
                        "GROUP BY p.producto " +
                        "ORDER BY cantidad_vendida DESC " +
                        "LIMIT 1"
        );
        productoMasVendido.show();

        // Cerrar Spark
        spark.stop();
    }
}
