package com.cloudbeaver.client.common;

import java.sql.SQLException;

public interface MySqlConsumer<T> {
	void accept(T t) throws SQLException;
}
