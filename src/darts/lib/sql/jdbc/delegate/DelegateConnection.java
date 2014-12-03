package darts.lib.sql.jdbc.delegate;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class DelegateConnection
implements Connection {
	
	protected final Connection connection;
	
	public DelegateConnection(Connection c) {
		this.connection = c;
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		if (iface.isInstance(this)) return iface.cast(this);
		return connection.unwrap(iface);
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		if (iface.isInstance(this)) return true;
		return connection.isWrapperFor(iface);
	}
	
	protected Statement wrapStatement(Statement stmt) {
		return new DelegateStatement<Statement,DelegateConnection>(this, stmt);
	}
	
	protected PreparedStatement wrapPreparedStatement(PreparedStatement stmt) {
		return new DelegatePreparedStatement<PreparedStatement,DelegateConnection>(this, stmt);
	}
	
	protected CallableStatement wrapCallableStatement(CallableStatement stmt) {
		return new DelegateCallableStatement<DelegateConnection>(this, stmt);
	}

	public Statement createStatement() throws SQLException {
		return wrapStatement(connection.createStatement());
	}

	public PreparedStatement prepareStatement(String sql) throws SQLException {
		return wrapPreparedStatement(connection.prepareStatement(sql));
	}

	public CallableStatement prepareCall(String sql) throws SQLException {
		return wrapCallableStatement(connection.prepareCall(sql));
	}

	public String nativeSQL(String sql) throws SQLException {
		return connection.nativeSQL(sql);
	}

	public void setAutoCommit(boolean autoCommit) throws SQLException {
		connection.setAutoCommit(autoCommit);
	}

	public boolean getAutoCommit() throws SQLException {
		return connection.getAutoCommit();
	}

	public void commit() throws SQLException {
		connection.commit();
	}

	public void rollback() throws SQLException {
		connection.rollback();
	}

	public void close() throws SQLException {
		connection.close();
	}

	public boolean isClosed() throws SQLException {
		return connection.isClosed();
	}

	public DatabaseMetaData getMetaData() throws SQLException {
		return new DelegateDatabaseMetaData<Connection>(this, connection.getMetaData());
	}

	public void setReadOnly(boolean readOnly) throws SQLException {
		connection.setReadOnly(readOnly);
	}

	public boolean isReadOnly() throws SQLException {
		return connection.isReadOnly();
	}

	public void setCatalog(String catalog) throws SQLException {
		connection.setCatalog(catalog);
	}

	public String getCatalog() throws SQLException {
		return connection.getCatalog();
	}

	public void setTransactionIsolation(int level) throws SQLException {
		connection.setTransactionIsolation(level);
	}

	public int getTransactionIsolation() throws SQLException {
		return connection.getTransactionIsolation();
	}

	public SQLWarning getWarnings() throws SQLException {
		return connection.getWarnings();
	}

	public void clearWarnings() throws SQLException {
		connection.clearWarnings();
	}

	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		return wrapStatement(connection.createStatement(resultSetType, resultSetConcurrency));
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		return wrapPreparedStatement(connection.prepareStatement(sql, resultSetType, resultSetConcurrency));
	}

	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		return wrapCallableStatement(connection.prepareCall(sql, resultSetType, resultSetConcurrency));
	}

	public Map<String, Class<?>> getTypeMap() throws SQLException {
		return connection.getTypeMap();
	}

	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		connection.setTypeMap(map);
	}

	public void setHoldability(int holdability) throws SQLException {
		connection.setHoldability(holdability);
	}

	public int getHoldability() throws SQLException {
		return connection.getHoldability();
	}

	public Savepoint setSavepoint() throws SQLException {
		return connection.setSavepoint();
	}

	public Savepoint setSavepoint(String name) throws SQLException {
		return connection.setSavepoint(name);
	}

	public void rollback(Savepoint savepoint) throws SQLException {
		connection.rollback(savepoint);
	}

	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		connection.releaseSavepoint(savepoint);
	}

	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		return wrapStatement(connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability));
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		return wrapPreparedStatement(connection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability));
	}

	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		return wrapCallableStatement(connection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability));
	}

	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		return wrapPreparedStatement(connection.prepareStatement(sql, autoGeneratedKeys));
	}

	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		return wrapPreparedStatement(connection.prepareStatement(sql, columnIndexes));
	}

	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		return wrapPreparedStatement(connection.prepareStatement(sql, columnNames));
	}

	public Clob createClob() throws SQLException {
		return connection.createClob();
	}

	public Blob createBlob() throws SQLException {
		return connection.createBlob();
	}

	public NClob createNClob() throws SQLException {
		return connection.createNClob();
	}

	public SQLXML createSQLXML() throws SQLException {
		return connection.createSQLXML();
	}

	public boolean isValid(int timeout) throws SQLException {
		return connection.isValid(timeout);
	}

	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		connection.setClientInfo(name, value);
	}

	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		connection.setClientInfo(properties);
	}

	public String getClientInfo(String name) throws SQLException {
		return connection.getClientInfo(name);
	}

	public Properties getClientInfo() throws SQLException {
		return connection.getClientInfo();
	}

	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		return connection.createArrayOf(typeName, elements);
	}

	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		return connection.createStruct(typeName, attributes);
	}

    public void setSchema(String schema) throws SQLException {
        connection.setSchema(schema);
    }

    public String getSchema() throws SQLException {
        return connection.getSchema();
    }

    public void abort(Executor executor) throws SQLException {
        connection.abort(executor);
    }

    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        connection.setNetworkTimeout(executor, milliseconds);
    }

    public int getNetworkTimeout() throws SQLException {
        return connection.getNetworkTimeout();
    }
}
