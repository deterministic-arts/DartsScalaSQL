package darts.lib.sql.jdbc.delegate;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

public class DelegateStatement<S extends Statement, C extends Connection>
implements Statement {
	
	protected final C connection;
	protected final S statement;
	
	public DelegateStatement(C connection, S statement) {
		this.connection = connection;
		this.statement = statement;
	}
	
	protected ResultSet wrapResultSet(ResultSet rs) {
		return new DelegateResultSet(this, rs);
	}

	public ResultSet executeQuery(String sql) throws SQLException {
		return wrapResultSet(statement.executeQuery(sql));
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		if (iface.isInstance(this)) return iface.cast(this);
		return statement.unwrap(iface);
	}

	public int executeUpdate(String sql) throws SQLException {
		return statement.executeUpdate(sql);
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		if (iface.isInstance(this)) return true;
		return statement.isWrapperFor(iface);
	}

	public void close() throws SQLException {
		statement.close();
	}

	public int getMaxFieldSize() throws SQLException {
		return statement.getMaxFieldSize();
	}

	public void setMaxFieldSize(int max) throws SQLException {
		statement.setMaxFieldSize(max);
	}

	public int getMaxRows() throws SQLException {
		return statement.getMaxRows();
	}

	public void setMaxRows(int max) throws SQLException {
		statement.setMaxRows(max);
	}

	public void setEscapeProcessing(boolean enable) throws SQLException {
		statement.setEscapeProcessing(enable);
	}

	public int getQueryTimeout() throws SQLException {
		return statement.getQueryTimeout();
	}

	public void setQueryTimeout(int seconds) throws SQLException {
		statement.setQueryTimeout(seconds);
	}

	public void cancel() throws SQLException {
		statement.cancel();
	}

	public SQLWarning getWarnings() throws SQLException {
		return statement.getWarnings();
	}

	public void clearWarnings() throws SQLException {
		statement.clearWarnings();
	}

	public void setCursorName(String name) throws SQLException {
		statement.setCursorName(name);
	}

	public boolean execute(String sql) throws SQLException {
		return statement.execute(sql);
	}

	public ResultSet getResultSet() throws SQLException {
		return wrapResultSet(statement.getResultSet());
	}

	public int getUpdateCount() throws SQLException {
		return statement.getUpdateCount();
	}

	public boolean getMoreResults() throws SQLException {
		return statement.getMoreResults();
	}

	public void setFetchDirection(int direction) throws SQLException {
		statement.setFetchDirection(direction);
	}

	public int getFetchDirection() throws SQLException {
		return statement.getFetchDirection();
	}

	public void setFetchSize(int rows) throws SQLException {
		statement.setFetchSize(rows);
	}

	public int getFetchSize() throws SQLException {
		return statement.getFetchSize();
	}

	public int getResultSetConcurrency() throws SQLException {
		return statement.getResultSetConcurrency();
	}

	public int getResultSetType() throws SQLException {
		return statement.getResultSetType();
	}

	public void addBatch(String sql) throws SQLException {
		statement.addBatch(sql);
	}

	public void clearBatch() throws SQLException {
		statement.clearBatch();
	}

	public int[] executeBatch() throws SQLException {
		return statement.executeBatch();
	}

	public Connection getConnection() throws SQLException {
		return connection;
	}

	public boolean getMoreResults(int current) throws SQLException {
		return statement.getMoreResults(current);
	}

	public ResultSet getGeneratedKeys() throws SQLException {
		return wrapResultSet(statement.getGeneratedKeys());
	}

	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
		return statement.executeUpdate(sql, autoGeneratedKeys);
	}

	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
		return statement.executeUpdate(sql, columnIndexes);
	}

	public int executeUpdate(String sql, String[] columnNames) throws SQLException {
		return statement.executeUpdate(sql, columnNames);
	}

	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
		return statement.execute(sql, autoGeneratedKeys);
	}

	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		return statement.execute(sql, columnIndexes);
	}

	public boolean execute(String sql, String[] columnNames) throws SQLException {
		return statement.execute(sql, columnNames);
	}

	public int getResultSetHoldability() throws SQLException {
		return statement.getResultSetHoldability();
	}

	public boolean isClosed() throws SQLException {
		return statement.isClosed();
	}

	public void setPoolable(boolean poolable) throws SQLException {
		statement.setPoolable(poolable);
	}

	public boolean isPoolable() throws SQLException {
		return statement.isPoolable();
	}

    public void closeOnCompletion() throws SQLException {
        statement.closeOnCompletion();
    }

    public boolean isCloseOnCompletion() throws SQLException {
        return statement.isCloseOnCompletion();
    }
}
