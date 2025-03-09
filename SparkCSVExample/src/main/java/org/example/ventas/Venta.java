package org.example.ventas;

import java.io.Serializable;

public class Venta implements Serializable {


    private int idVenta;
    private String producto;
    private String categoria;
    private double precio;
    private int cantidad;

    // Constructor vacío (necesario para Spark)
    public Venta() {}

    // Constructor con parámetros
    public Venta(int idVenta, String producto, String categoria, double precio, int cantidad) {
        this.idVenta = idVenta;
        this.producto = producto;
        this.categoria = categoria;
        this.precio = precio;
        this.cantidad = cantidad;
    }

    // Getters y Setters
    public int getIdVenta() { return idVenta; }
    public void setIdVenta(int idVenta) { this.idVenta = idVenta; }

    public String getProducto() { return producto; }
    public void setProducto(String producto) { this.producto = producto; }

    public String getCategoria() { return categoria; }
    public void setCategoria(String categoria) { this.categoria = categoria; }

    public double getPrecio() { return precio; }
    public void setPrecio(double precio) { this.precio = precio; }

    public int getCantidad() { return cantidad; }
    public void setCantidad(int cantidad) { this.cantidad = cantidad; }

    @Override
    public String toString() {
        return "Venta{idVenta=" + idVenta + ", producto='" + producto + '\'' +
                ", categoria='" + categoria + '\'' + ", precio=" + precio +
                ", cantidad=" + cantidad + '}';
    }
}


