package org.example;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;


import java.util.Arrays;
import java.util.Iterator;


public class SparkAvanceTranformation {

        public static void main(String[] args) {
            // 1. Configurar Spark
            SparkConf conf = new SparkConf().setAppName("AnalisisVentas").setMaster("local[*]");
            JavaSparkContext sc = new JavaSparkContext(conf);

            // 2. Cargar datos desde un archivo CSV
            JavaRDD<String> data = sc.textFile("src/main/input/compras.csv");

            // 3. Mapear datos a pares clave-valor (id_cliente, monto)
            JavaPairRDD<String, Double> ventas = data
                    .filter(line -> !line.startsWith("id_cliente")) // Omitir cabecera
                    .mapToPair(line -> {
                        String[] parts = line.split(",");
                        return new Tuple2<>(parts[0], Double.parseDouble(parts[2]));
                    });

            // 4. Usar reduceByKey para calcular la venta total por cliente
            JavaPairRDD<String, Double> totalPorCliente = ventas.reduceByKey(Double::sum);

            // 5. Usar combineByKey para calcular el gasto promedio por cliente
            JavaPairRDD<String, Tuple2<Double, Integer>> combinadas = ventas.combineByKey(
                    value -> new Tuple2<>(value, 1), // Crear acumulador inicial
                    (acc, value) -> new Tuple2<>(acc._1 + value, acc._2 + 1), // Agregar valores
                    (acc1, acc2) -> new Tuple2<>(acc1._1 + acc2._1, acc1._2 + acc2._2) // Fusionar acumuladores
            );

            // Calcular promedio dividiendo el total entre la cantidad de compras
            JavaPairRDD<String, Double> promedioPorCliente = combinadas.mapValues(t -> t._1 / t._2);

            // 6. Comparar repartition vs coalesce
            JavaRDD<String> repartitionedData = data.repartition(10); // Aumenta particiones con shuffle
            JavaRDD<String> coalescedData = data.coalesce(2); // Reduce particiones sin shuffle innecesario

            // 7. Imprimir resultados
            System.out.println("Ventas totales por cliente:");
            totalPorCliente.collect().forEach(System.out::println);

            System.out.println("Gasto promedio por cliente:");
            promedioPorCliente.collect().forEach(System.out::println);




        // Finalizar Spark
            sc.close();
        }
}


