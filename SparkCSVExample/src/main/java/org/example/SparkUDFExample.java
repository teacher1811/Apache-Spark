package org.example;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;


public class SparkUDFExample {
    public static void main(String[] args) {
        // 🔹 1️⃣ Inicializar SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("UDF Example in Spark")
                .master("local[*]")
                .getOrCreate();

        // 🔹 2️⃣ Crear DataFrame con empleados
        Dataset<Row> empleadosDF = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/input/employees.csv");

        empleadosDF.show();

        // 🔹 3️⃣ Definir una UDF para categorizar salarios
        UDF1<Integer, String> categorizarSalario = (salary) -> {
            if (salary > 7000) return "Alto";
            else if (salary >= 5000) return "Medio";
            else return "Bajo";
        };

        // 🔹 4️⃣ Registrar la UDF en Spark SQL
        spark.udf().register("categoriaSalario", categorizarSalario, DataTypes.StringType);

        // 🔹 5️⃣ Usar la UDF en una consulta SQL
        empleadosDF.createOrReplaceTempView("Empleados");

        Dataset<Row> resultado = spark.sql(
                "SELECT id, name, salary, categoriaSalario(salary) AS categoria FROM Empleados"
        );

        resultado.explain(true);
        resultado.show();

        // Detener Spark
        spark.stop();
    }
}
