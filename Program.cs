using Dapper;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace WikiJsMigration
{
	class Program
	{
		private static readonly StreamWriter CONSOLE_LOG = new StreamWriter(new FileStream("console.log", FileMode.Create, FileAccess.Write, FileShare.Read));

		private static readonly List<string> TABLE_PRIORITIES = new List<string>(new[]
		{
			"users",
			"pageHistory",
			"pages",
			"pageTree",
			"comments",
			"assets",
			"userGroups",
			"pageTags"
		});

		static async Task Main()
		{
			const string sourceConnectionString = "";
			const string targetConnectionString = "";
			const string targetTableSchema = "wiki";

			Log("Opening Source Database");
			using var sourceDatabase = new SqlConnection(sourceConnectionString);
			await sourceDatabase.OpenAsync();

			Log("Opening Target Database");
			using var targetDatabase = new MySqlConnection(targetConnectionString);
			await targetDatabase.OpenAsync();

			Log("Retrieving Source Table Names");
			var sourceTableNames = (await sourceDatabase.QueryAsync<dynamic>("SELECT TABLE_NAME AS TableName FROM INFORMATION_SCHEMA.TABLES"))
				.Select(dyn => dyn.TableName as string)
				.ToArray();

			Log("Retrieving Target Table Names");
			var targetTableNames = (await targetDatabase.QueryAsync<dynamic>($"SELECT TABLE_NAME AS TableName FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{targetTableSchema}'"))
				.Select(dyn => dyn.TableName as string)
				.ToArray();

			if (sourceTableNames.Length != targetTableNames.Length)
			{
				Log($"Source Database has {sourceTableNames.Length} tables while Target Database have {targetTableNames.Length}, aborting...");
				return;
			}

			Log("Clearing Target Table Data");
			foreach (var tableName in targetTableNames.OrderByDescending(tableName => TABLE_PRIORITIES.IndexOf(tableName)))
			{
				var affectedRows = await targetDatabase.ExecuteAsync($"DELETE FROM `{tableName}`");
				Log($"Deleted {affectedRows} rows from {tableName}");
			}

			Log("Migrating Source Table Data");
			foreach (var tableName in sourceTableNames.OrderByDescending(tableName => TABLE_PRIORITIES.IndexOf(tableName)).Reverse())
			{
				var rows = (await sourceDatabase.QueryAsync<dynamic>($"SELECT * FROM {tableName}"))
					.Select(row => row as IDictionary<string, object>)
					.ToArray();

				foreach (var row in rows)
				{
					var columns = row.Select(kvp => $"`{kvp.Key}`");
					var columnParameters = row.Select(kvp => $"@{kvp.Key}");

					var query = $"INSERT INTO `{tableName}` ({string.Join(',', columns)}) VALUES ({string.Join(',', columnParameters)})";

					var dynamicParameters = new DynamicParameters();
					foreach (var kvp in row) { dynamicParameters.Add($"@{kvp.Key}", kvp.Value); }

					var affectedRows = await targetDatabase.ExecuteAsync(query, dynamicParameters);
				}

				Log($"Migrated {rows.Length} rows for {tableName}");
			}

			Log("Migration Successful");
		}

		private static void Log(string message)
		{
			var formattedMessage = $"{DateTimeOffset.Now.TimeOfDay} : {message}";
			CONSOLE_LOG.WriteLine(formattedMessage);
			CONSOLE_LOG.Flush();
			Console.WriteLine(formattedMessage);
		}
	}
}
