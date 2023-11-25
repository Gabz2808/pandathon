package com.function;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

import java.sql.Statement;

/**
 * Azure Functions with HTTP Trigger.
 */
public class Function {
  Connection c = null;
  Statement stmt = null;
  String respuesta = "";
  String json = "";

  /**
   * This function listens at endpoint "/api/HttpExample". Two ways to invoke it
   * using "curl" command in bash:
   * 1. curl -d "HTTP Body" {your host}/api/HttpExample
   * 2. curl "{your host}/api/HttpExample?name=HTTP%20Query"
   * 
   * @throws SQLException
   */
  @FunctionName("HttpExample")
  public HttpResponseMessage run(

      @HttpTrigger(name = "req", methods = { HttpMethod.GET,
          HttpMethod.POST }, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
      final ExecutionContext context) throws SQLException {

    String url = "jdbc:postgresql://ptpostgresql.postgres.database.azure.com:5432/PandaSQL";
    String user = "Panda";
    String password = "Tech1234";
    Connection c = DriverManager.getConnection(url, user, password);
    try {

      stmt = c.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT * FROM weather");

      while (rs.next()) {
        int idclima = rs.getInt("idclima");
        String clima = rs.getString("clima");
        String pais = rs.getString("pais");
        String ciudad = rs.getString("ciudad");
        String nombre = rs.getString("nombre");
        String cedula = rs.getString("cedula");
        String registro = rs.getString("registro");

        respuesta = respuesta + "idclima = " + idclima + "\n" + "clima = " + clima + "\n" + "pais = " + pais + "\n"
            + "ciudad = " + ciudad + "\n" + "nombre = " + nombre + "\n" + "cedula = " + cedula + "\n" + "registro = "
            + registro + "\n" + "\n";
        /*
         * System.out.println("idclima = " + idclima);
         * System.out.println("clima = " + clima);
         * System.out.println("pais = " + pais);
         * System.out.println("ciudad = " + ciudad);
         * System.out.println("nombre = " + nombre);
         * System.out.println("cedula = " + cedula);
         * System.out.println("registro = " + registro);
         * System.out.println();
         */
      }
      ResultSet rs2 = stmt.executeQuery("select row_to_json(row) from (select * from weather) row;");
      while (rs2.next()) {
        json += rs2.getString("row_to_json");
      }

      rs.close();
      stmt.close();
      c.close();
    } catch (Exception e) {
      respuesta = e.getMessage();
    }
    context.getLogger().info("Java HTTP trigger processed a request.");

    // Parse query parameter
    final String query = request.getQueryParameters().get("name");
    final String name = request.getBody().orElse(query);

    if (name == null) {
      return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
          .body(respuesta + "Hola Mundo!\n" + json).build();
    } else {
      return request.createResponseBuilder(HttpStatus.OK).body("Hello, " + name).build();
    }

  }
}
