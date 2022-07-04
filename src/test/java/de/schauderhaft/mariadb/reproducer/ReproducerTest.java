/*
 * Copyright 2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.schauderhaft.mariadb.reproducer;

import org.assertj.core.api.Assertions;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.MariaDbDataSource;
import org.testcontainers.containers.MariaDBContainer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

class ReproducerTest {


	private static MariaDBContainer<?> MARIADB_CONTAINER;

	static DataSource createDataSource() {

		if (MARIADB_CONTAINER == null) {

			MariaDBContainer<?> container = new MariaDBContainer<>("mariadb:10.5").withUsername("root").withPassword("")
					.withConfigurationOverride("");
			container.start();

			MARIADB_CONTAINER = container;
		}

		try {

			MariaDbDataSource dataSource = new MariaDbDataSource();
			dataSource.setUrl(MARIADB_CONTAINER.getJdbcUrl());
			dataSource.setUser(MARIADB_CONTAINER.getUsername());
			dataSource.setPassword(MARIADB_CONTAINER.getPassword());
			return dataSource;
		} catch (SQLException sqlex) {
			throw new RuntimeException(sqlex);
		}
	}


	@Test
	void reproduce() throws SQLException {

		final Connection conn = createDataSource().getConnection();
		createTable(conn);
		createData(conn);

		final Object binaryData = readBinaryData(conn);
		Assertions.assertThat(((String) binaryData).getBytes()).isEqualTo(new byte[]{1, 23, 42});
	}

	private Object readBinaryData(Connection conn) throws SQLException {
		final PreparedStatement statement = conn.prepareStatement("SELECT ID, BINARY_DATA FROM BYTE_ARRAY_OWNER");
		final ResultSet resultSet = statement.executeQuery();

		resultSet.next();

		final Object binaryData = resultSet.getObject(2);
		return binaryData;
	}

	private void createData(Connection conn) throws SQLException {

		System.out.println("insert data");
		final PreparedStatement statement = conn.prepareStatement("""
				INSERT INTO BYTE_ARRAY_OWNER(ID, BINARY_DATA)
				VALUES (?,?);
				""");
		statement.setInt(1, 3);
		statement.setObject(2, new byte[]{1, 23, 42});
		statement.execute();
	}

	@NotNull
	private Statement createTable(Connection conn) throws SQLException {
		final Statement statement = conn.createStatement();
		System.out.println("creating table");
		statement.execute("""
				CREATE TABLE BYTE_ARRAY_OWNER
				(
				  ID          BIGINT PRIMARY KEY,
				  BINARY_DATA VARBINARY(20) NOT NULL
				);
				""");
		return statement;
	}

}
