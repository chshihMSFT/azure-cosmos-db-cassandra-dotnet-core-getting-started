using Cassandra;
using Cassandra.Mapping;
using System;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using System.Threading;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Json;

namespace CassandraQuickStart
{
	public class Program
	{
		// Cassandra Cluster Configs      
		private static string UserName;
		private static string Password;
		private static string CassandraContactPoint;
		private static int CassandraPort;

		private static string keyspaceName;
		private static string tableName;

		private static int InsertTotalItems;
		private static int InsertItemsPerBatch;
		private static int InsertDelayMSPerBatch;

		static void Main(string[] args)
		{
			//Reading application settings
			try
			{
				IConfigurationRoot configuration = new ConfigurationBuilder()
							.AddJsonFile("appSettings.json")
							.Build();

				UserName = configuration["UserName"];
				Password = configuration["Password"];
				CassandraContactPoint = configuration["CassandraContactPoint"];
				CassandraPort = Int32.Parse(configuration["CassandraPort"]);

				keyspaceName = configuration["keyspaceName"];
				tableName = configuration["tableName"];

				InsertTotalItems = Int32.Parse(configuration["InsertTotalItems"]);
				InsertItemsPerBatch = Int32.Parse(configuration["InsertItemsPerBatch"]);
				InsertDelayMSPerBatch = Int32.Parse(configuration["InsertDelayMSPerBatch"]);
			}
			catch (Exception ex)
			{
				Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, reading application settings, failed: {ex.Message}");
			}
			finally
			{
				Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, demo start");
				ProcessAsync().Wait();
				Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, demo end");
			}

			Console.WriteLine("Done! You can go look at your data in Cosmos DB. Don't forget to clean up Azure resources!");
			Console.WriteLine("Press any key to exit.");

			Console.ReadKey();
		}

		public static async Task ProcessAsync()
		{
			// Connect to cassandra cluster  (Cassandra API on Azure Cosmos DB supports only TLSv1.2)
			var options = new Cassandra.SSLOptions(SslProtocols.Tls12, true, ValidateServerCertificate);

			options.SetHostNameResolver((ipAddress) => CassandraContactPoint);
			Cluster cluster = Cluster
				.Builder()
				.WithCredentials(UserName, Password)
				.WithPort(CassandraPort)
				.AddContactPoint(CassandraContactPoint)
				.WithSSL(options)
				.Build()
			;

			ISession session = await cluster.ConnectAsync();
			Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, connected to Cassandra cluster: {CassandraContactPoint}");

			// Creating KeySpace and table
			String cqlCreateKeySpace = "CREATE KEYSPACE IF NOT EXISTS " + keyspaceName + " WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 1 }; ";
			await session.ExecuteAsync(new SimpleStatement(cqlCreateKeySpace));
			Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, {cqlCreateKeySpace}");
			String cqlCreatTable = "CREATE TABLE IF NOT EXISTS " + keyspaceName + "." + tableName + " (item_category varchar, item_id varchar, item_name varchar, item_createtime varchar, PRIMARY KEY(item_category, item_id)); ";
			await session.ExecuteAsync(new SimpleStatement(cqlCreatTable));
			Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, {cqlCreatTable}");

			session = await cluster.ConnectAsync(keyspaceName);
			IMapper mapper = new Mapper(session);

			//Set the Mapping Configuration
			MappingConfiguration.Global.Define(
				new Map<Item>()
					.TableName(tableName)
					.PartitionKey(u => u.item_category, u => u.item_id)
			);

			// Inserting Data into table
			var DemoPK = "demo" + DateTime.UtcNow.ToString("yyyyMMddHHmmss");
			int AccumulatedItems = 0;
			int BatchCount = 1;
			int SerialNo = 1;
			while (AccumulatedItems < InsertTotalItems)
			{
				InsertItemsPerBatch = ((AccumulatedItems + InsertItemsPerBatch) > InsertTotalItems) ? (InsertTotalItems - AccumulatedItems) : InsertItemsPerBatch;
				for (int i = 1; i <= InsertItemsPerBatch; i++)
				{
					try
					{
						var newitem = new Item(
								DemoPK																	//item_category
								, String.Format($"{BatchCount.ToString().PadLeft(4, '0')}-{i.ToString().PadLeft(6, '0')}-{SerialNo++.ToString().PadLeft(10, '0')}")	//item_id
								, Guid.NewGuid().ToString().Substring(0, 8)								//item_name
								, DateTime.UtcNow.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'ffffff'Z'")	//item_createtime
							);

						await mapper.InsertAsync<Item>(newitem);
						Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, Insert:{{{newitem.ToString()}}}");
						AccumulatedItems++;
					}
					catch (Exception ce)
					{
						Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, Insert failed:{ce.Message}");
					}
					finally
					{
						Thread.Sleep(InsertDelayMSPerBatch);
					}
				}
				BatchCount++;
			}

			/*
			Console.WriteLine("Select ALL");
			Console.WriteLine("-------------------------------");
			foreach (User user in await mapper.FetchAsync<User>("Select * from user"))
			{
				Console.WriteLine(user);
			}

			Console.WriteLine("Getting by id 3");
			Console.WriteLine("-------------------------------");
			User userId3 = await mapper.FirstOrDefaultAsync<User>("Select * from user where user_id = ?", 3);
			Console.WriteLine(userId3);
			*/

			// Clean up of Table and KeySpace - commented out since other QuickStarts do not immediately delete what was done,
			// and so QuickStart user (that's you) has chance to go look at this data in Cosmos DB
			//session.Execute("DROP table user");
			//session.Execute("DROP KEYSPACE uprofile");
		}

		public static bool ValidateServerCertificate
		(
			object sender,
			X509Certificate certificate,
			X509Chain chain,
			SslPolicyErrors sslPolicyErrors
		)
		{
			if (sslPolicyErrors == SslPolicyErrors.None)
				return true;

			Console.WriteLine("Certificate error: {0}", sslPolicyErrors);
			// Do not allow this client to communicate with unauthenticated servers.
			return false;
		}
	}
}
