package com.databricks.jdbc.pooling;

import com.databricks.jdbc.core.DatabricksSQLException;
import com.databricks.jdbc.core.IDatabricksConnection;
import com.databricks.jdbc.core.IDatabricksStatement;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.*;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;
import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;
import javax.sql.StatementEventListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DatabricksPooledConnection implements PooledConnection {

  private static final Logger LOGGER = LogManager.getLogger(DatabricksPooledConnection.class);
  private final Set<ConnectionEventListener> listeners = new HashSet<>();
  private Connection physicalConnection;
  private ConnectionHandler connectionHandler;

  public Connection getPhysicalConnection() {
    return this.physicalConnection;
  }

  /**
   * Creates a new PooledConnection representing the specified physical connection.
   *
   * @param physicalConnection connection
   */
  public DatabricksPooledConnection(Connection physicalConnection) {
    this.physicalConnection = physicalConnection;
  }

  /** Fires a connection closed event to all listeners. */
  void fireConnectionClosed() {
    LOGGER.debug("void fireConnectionClosed()");
    for (ConnectionEventListener listener : this.listeners) {
      listener.connectionClosed(new ConnectionEvent(this));
    }
  }

  /**
   * Fires a connection error event to all listeners
   *
   * @param e the SQLException to consider
   */
  private void fireConnectionError(SQLException e) {
    LOGGER.debug("private void fireConnectionError(SQLException e = {})", e.toString());
    for (ConnectionEventListener listener : this.listeners) {
      listener.connectionErrorOccurred(new ConnectionEvent(this, e));
    }
  }

  @Override
  public void addConnectionEventListener(ConnectionEventListener connectionEventListener) {
    listeners.add(connectionEventListener);
  }

  @Override
  public void removeConnectionEventListener(ConnectionEventListener connectionEventListener) {
    listeners.remove(connectionEventListener);
  }

  @Override
  public void removeStatementEventListener(StatementEventListener listener) {
    // Do nothing, not supported
  }

  @Override
  public void addStatementEventListener(StatementEventListener listener) {
    // Do nothing, not supported
  }

  /** Close the physical connection once the pooled connection is closed */
  @Override
  public void close() throws SQLException {
    LOGGER.debug("public void close()");
    if (connectionHandler != null && !connectionHandler.isClosed()) {
      connectionHandler.close();
    }
    if (physicalConnection == null) {
      return;
    }
    try {
      physicalConnection.close();
    } finally {
      physicalConnection = null;
    }
  }

  /**
   * Gets a handle for a client to use. This is a wrapper around the physical connection, so the
   * client can call close, and it will just return the connection to the pool without really
   * closing the physical connection.
   *
   * <p>According to the JDBC 4.3 Optional Package spec (11.4), only one client may have an active
   * handle to the connection at a time, so if there is a previous handle active when this is
   * called, the previous one is forcibly closed.
   */
  @Override
  public Connection getConnection() throws SQLException {
    LOGGER.debug("public Connection getConnection()");
    if (physicalConnection == null) {
      // Before throwing the exception, notify the listeners
      DatabricksSQLException sqlException =
          new DatabricksSQLException("This PooledConnection has already been closed.");
      fireConnectionError(sqlException);
      throw sqlException;
    }
    // Only one connection can be open at a time from this PooledConnection
    if (connectionHandler != null && !connectionHandler.isClosed()) {
      connectionHandler.close();
    }
    connectionHandler = new ConnectionHandler(physicalConnection);
    return connectionHandler.getVirtualConnection();
  }

  /**
   * Instead of declaring a class implementing Connection, use a dynamic proxy to handle all calls
   * through the Connection interface.
   */
  private class ConnectionHandler implements InvocationHandler {
    private final Logger CONNECTION_HANDLER_LOGGER = LogManager.getLogger(ConnectionHandler.class);
    private Connection physicalConnection;
    private Connection
        virtualConnection; // the Connection the client is currently using, which is not a physical

    // connection

    ConnectionHandler(Connection physicalConnection) {
      this.physicalConnection = physicalConnection;
      // Use a proxy connection object as a virtual connection, so that we do not close the physical
      // connection
      this.virtualConnection =
          (Connection)
              Proxy.newProxyInstance(
                  getClass().getClassLoader(),
                  new Class[] {Connection.class, IDatabricksConnection.class},
                  this);
    }

    @Override
    public Object invoke(Object proxy, Method method, @Nullable Object[] args) throws Throwable {
      CONNECTION_HANDLER_LOGGER.debug(
          "public Object invoke(Object proxy, Method method = {}, Object[] args = {})",
          method,
          args);
      final String methodName = method.getName();
      if (method.getDeclaringClass() == Object.class) {
        if (methodName.equals("toString")) {
          return "Pooled connection wrapping physical connection " + physicalConnection;
        }
        if (methodName.equals("equals")) {
          return proxy == args[0];
        }
        if (methodName.equals("hashCode")) {
          return System.identityHashCode(proxy);
        }
        try {
          return method.invoke(physicalConnection, args);
        } catch (InvocationTargetException e) {
          // throwing.nullable
          throw e.getTargetException();
        }
      }

      if (methodName.equals("isClosed")) {
        return physicalConnection == null || physicalConnection.isClosed();
      }
      // Do not close the physical connection, remove reference and fire close event
      if (methodName.equals("close")) {
        if (physicalConnection != null) {
          physicalConnection = null;
          virtualConnection = null;
          connectionHandler = null;
          fireConnectionClosed();
        }
        return null;
      }
      if (physicalConnection == null || physicalConnection.isClosed()) {
        throw new DatabricksSQLException("Connection has been closed.");
      }

      // From here on in, we invoke via reflection and catch exceptions
      try {
        Class statementClass;
        switch (methodName) {
          case "createStatement":
            statementClass = Statement.class;
            break;
          case "prepareCall":
            statementClass = CallableStatement.class;
            break;
          case "prepareStatement":
            statementClass = PreparedStatement.class;
            break;
          default:
            return method.invoke(physicalConnection, args);
        }
        Statement st = (Statement) method.invoke(physicalConnection, args);
        return Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class[] {statementClass, IDatabricksStatement.class},
            new StatementHandler(this, st));
      } catch (final InvocationTargetException ite) {
        final Throwable targetException = ite.getTargetException();
        if (targetException instanceof SQLException) {
          fireConnectionError((SQLException) targetException);
        }
        throw targetException;
      }
    }

    Connection getVirtualConnection() {
      return virtualConnection;
    }

    public void close() {
      CONNECTION_HANDLER_LOGGER.debug("public void close()");
      physicalConnection = null;
      virtualConnection = null;
      // No close event fired here: see JDBC 4.3 Optional Package spec section 11.4
    }

    public boolean isClosed() {
      return physicalConnection == null;
    }
  }

  /**
   * Instead of declaring classes implementing Statement, use a dynamic proxy to handle all calls
   * through the Statement interfaces. The StatementHandler is required in order to return the
   * proper Connection proxy for the getConnection method.
   */
  private class StatementHandler implements InvocationHandler {
    private final Logger STATEMENT_HANDLER_LOGGER = LogManager.getLogger(StatementHandler.class);
    private ConnectionHandler conHandler;
    private Statement physicalStatement;

    StatementHandler(ConnectionHandler conHandler, Statement physicalStatement) {
      this.conHandler = conHandler;
      this.physicalStatement = physicalStatement;
    }

    @Override
    public Object invoke(Object proxy, Method method, @Nullable Object[] args) throws Throwable {
      STATEMENT_HANDLER_LOGGER.debug(
          "public Object invoke(Object proxy = {}, Method method = {}, Object[] args = {})",
          proxy,
          method,
          args);
      final String methodName = method.getName();
      if (method.getDeclaringClass() == Object.class) {
        if (methodName.equals("toString")) {
          return "Pooled statement wrapping physical statement " + physicalStatement;
        }
        if (methodName.equals("hashCode")) {
          return System.identityHashCode(proxy);
        }
        if (methodName.equals("equals")) {
          return proxy == args[0];
        }
        return method.invoke(physicalStatement, args);
      }

      if (methodName.equals("isClosed")) {
        return physicalStatement == null || physicalStatement.isClosed();
      }
      if (methodName.equals("close")) {
        if (physicalStatement == null || physicalStatement.isClosed()) {
          return null;
        }
        conHandler = null;
        physicalStatement.close();
        physicalStatement = null;
        return null;
      }
      if (physicalStatement == null || physicalStatement.isClosed()) {
        throw new DatabricksSQLException("Statement has been closed.");
      }
      if (methodName.equals("getConnection")) {
        return conHandler
            .getVirtualConnection(); // the virtual connection from the connection handler
      }

      // Delegate the call to the physical Statement.
      try {
        return method.invoke(physicalStatement, args);
      } catch (final InvocationTargetException ite) {
        final Throwable targetException = ite.getTargetException();
        if (targetException instanceof SQLException) {
          fireConnectionError((SQLException) targetException);
        }
        throw targetException;
      }
    }
  }
}
