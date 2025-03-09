package org.example.rdddataframedataset;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Encoders;
import org.example.rdddataframedataset.Employee;


public class RddDataFrameDataSet {
    public static void main(String[] args) {
        // Crear una SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("RDD vs DataFrame vs Dataset")
                .master("local[*]") // Ejecutar en local
                .getOrCreate();

        // Ruta del archivo CSV (cambiar según sea necesario)
        String filePath = "src/main/input/employees.csv";

        // === CARGA COMO RDD ===
        JavaRDD<String> rawRDD = spark.read().textFile(filePath).javaRDD();

        JavaRDD<Employee> employeeRDD = rawRDD
                .filter(line -> !line.startsWith("id")) // Omitir cabecera
                .map(line -> {
                    String[] parts = line.split(",");
                    return new Employee(
                            Integer.parseInt(parts[0]),
                            parts[1],
                            parts[2],
                            Double.parseDouble(parts[3])
                    );
                });

        // Filtrar empleados con salario > 6000 en RDD
        JavaRDD<Employee> highSalaryRDD = employeeRDD.filter(emp -> emp.getSalary() > 6000);
        System.out.println("=== RDD ===");
        highSalaryRDD.collect().forEach(System.out::println);


        // === CARGA COMO DATAFRAME ===
        Dataset<Row> employeeDF = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(filePath);

        // Filtrar empleados con salario > 6000 en DataFrame
        System.out.println("=== DataFrame ===");
        Dataset<Row> highSalaryDF = employeeDF.filter("salary > 6000");
        highSalaryDF.show();


        // === CARGA COMO DATASET ===

        Dataset<Employee> employeeDS = employeeDF.as(Encoders.bean(Employee.class));

        // Filtrar empleados con salario > 6000 en Dataset
        System.out.println("=== Dataset ===");
        Dataset<Employee> highSalaryDS = employeeDS.filter((FilterFunction<Employee>) emp -> emp.getSalary() > 6000);
        highSalaryDS.show();

        // Finalizar SparkSession
        spark.stop();
    }
}
