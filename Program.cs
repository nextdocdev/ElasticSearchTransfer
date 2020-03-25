using Dapper;
using Elasticsearch.Net;
using Microsoft.Extensions.Configuration;
using Nest;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;

namespace IndexAndTranfer
{
	internal static class Program
	{
		private static IConfigurationRoot configuration;
		private static StaticConnectionPool pool;
		private static ConnectionSettings settings;
		private static IElasticClient client;
		private static string IdxName = null;

		private static void Main(string[] args)
		{
			Console.WriteLine("-- Using ElasticSearch 7.5.0 --");

			var builder = new ConfigurationBuilder()
					   .SetBasePath(Directory.GetCurrentDirectory())
					   .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);

			configuration = builder.Build();

			if (args.Length == 0)
			{
				Console.WriteLine("Invalid parameters.");
				return;
			}

			string assinante = Array.Find(args, p => p.Contains("/assinante"));
			string index = Array.Find(args, p => p.Contains("/index"));
			string search = Array.Find(args, p => p.Contains("/search"));

			if (string.IsNullOrEmpty(assinante))
			{
				Console.WriteLine("Invalid parameter. Assinante not informed.");
				return;
			}

			if (string.IsNullOrEmpty(index) && string.IsNullOrEmpty(search))
			{
				Console.WriteLine("Invalid parameter. Wrong command.");
				return;
			}

			string assinanteId = assinante.Split(new string[] { ":" }, StringSplitOptions.RemoveEmptyEntries)[1];
			IdxName = $"idx_{assinanteId}";

			bool checkConn = BuildConn(!string.IsNullOrEmpty(index));
			if (!checkConn)
			{
				throw new Exception("The ElasticSearch Cluster is not responding at this time. Try later.");
			}

			if (!string.IsNullOrEmpty(index))
			{
				BuildIndex(int.Parse(assinanteId));
			}

			if (!string.IsNullOrEmpty(search))
			{
				string term = search.Split(new string[] { ":" }, StringSplitOptions.RemoveEmptyEntries)[1];
				//Search(term);

				DateTime dt = DateTime.Now;

				//MultiFieldSearch(term);
				PastaFieldSearch(3216, 17465);

				Console.WriteLine($"Start: {dt} => Finish: {DateTime.Now}");
			}
		}

		private static void Search(string term)
		{
			var searchResponse = client.Search<IndiceDocumento>(s => s
				.Index(Indices.Index("docindex"))
				.From(0)
				.Size(200)
				.Query(q => q
					 .Match(m => m
						.Field(f => f.NomeDocumento)
						.Query(term)
					 )
				)
			);

			Console.WriteLine($"Result: {searchResponse.Documents.Count} records.");
			foreach (var item in searchResponse.Documents)
			{
				Console.WriteLine($"{item.NomeDocumento} - {item.NomeRepositorio} - {item.ConteudoPropriedade}");
			}
		}

		private static void PastaFieldSearch(int codigoRepositorio, int codigoPasta)
		{
			Console.WriteLine("PastaFieldSearch Search...\r\n");

			var searchResponse = client.Search<IndiceDocumento>(s => s
				.Index(Indices.Index("docindex"))
				.From(0)
				.Size(2000)
				.Query(q => q
					.Bool(b => b
						.Must(
							bs => bs.Term(p => p.CodigoRepositorio, codigoRepositorio),
							bs => bs.Term(p => p.CodigoPasta, codigoPasta)
						)
					)
				));

			Console.WriteLine($"\t\tResultado: {searchResponse.Documents.Count} registros.\r");
			foreach (var item in searchResponse.Documents)
			{
				Console.WriteLine($"\t\t\t({item.CodigoRepositorio}|{item.CodigoPasta})={item.NomeRepositorio}\\{item.NomePasta}\\{item.NomeDocumento}{item.NomeArquivoOriginal}\r");
			}
		}

		private static void MultiFieldSearch(string term)
		{
			Console.WriteLine("MultiFields Search...\r\n");
			var searchResponse = client.Search<IndiceDocumento>(s => s
				.Index(Indices.Index("docindex"))
				.From(0)
				.Size(2000)
				.Query(q => q
					.MultiMatch(m => m
						.Fuzziness(Fuzziness.Auto)
						.PrefixLength(2)
						.MaxExpansions(2)
						.Operator(Operator.Or)
						.Fields(f => f
							.Field(fd => fd.NomeRepositorio)
							.Field(fd => fd.NomeDocumento, boost: 15.0)
							.Field(fd => fd.ConteudoPropriedade, boost: 14.0)
							.Field(fd => fd.NomePasta)
							.Field(fd => fd.NomeTipoDocumento)
							.Field(fd => fd.NomeArquivoOriginal, boost: 10.0)
							.Field(fd => fd.Extensao)
							.Field(fd => fd.Comentario, boost: 13.0)
							.Field(fd => fd.ContentType)
						)
						.Query(term)
					)
				)
			);

			Console.WriteLine($"Result: {searchResponse.Documents.Count} records.");
			foreach (var item in searchResponse.Documents)
			{
				Console.WriteLine($"{item.CodigoRepositorio}\\{item.CodigoPasta} >> {item.NomeDocumento} - {item.NomeRepositorio} - {item.ConteudoPropriedade} - {item.NomePasta} - {item.NomeTipoDocumento} - {item.NomeArquivoOriginal} - {item.Comentario}");
			}
		}

		private static bool BuildConn(bool create)
		{
			var nodes = new Uri[]
			{
				new Uri("http://localhost:9200")
			};

			pool = new StaticConnectionPool(nodes);
			settings = new ConnectionSettings(pool);
			client = new ElasticClient(settings);

			if (create) return true;

			var checkClusterHealth = client.Cluster.Health(Indices.Index(IdxName));
			//var checkClusterHealth2 = client.Cluster.Health(Indices.Index("docindex"));

			return checkClusterHealth.ApiCall.Success && checkClusterHealth.IsValid;
			//return (checkClusterHealth2.ApiCall.Success && checkClusterHealth2.IsValid);
		}

		private static void BuildIndex(int assinanteId)
		{
			var checkIndexOld = client.Indices.Exists(new IndexExistsRequest(Indices.Index(IdxName)));
			if (checkIndexOld.Exists)
				client.Indices.Delete(new DeleteIndexRequest(Indices.Index(IdxName)));

			var checkIndex = client.Indices.Exists(new IndexExistsRequest(Indices.Index(IdxName)));

			if (!checkIndex.Exists)
			{
				var createIndexResponse = client.Indices.Create(IdxName, i => i.Map<IndiceDocumento>(m => m.AutoMap()));
				if (!createIndexResponse.ApiCall.Success || !createIndexResponse.IsValid)
				{
					throw new Exception($"Not possible create index.\r\n{createIndexResponse.DebugInformation}.", createIndexResponse.OriginalException);
				}

				int PageSize = assinanteId == 23 ? 2000 : 200;
				int TotalRecords = 0;
				int TotalPages = 0;

				using (var connection = new System.Data.SqlClient.SqlConnection(configuration.GetConnectionString("DefaultConnection")))
				{
					if(connection.State != ConnectionState.Open)
						connection.Open();

					TotalRecords = connection.ExecuteScalar<int>(sql_count, new { assinanteId }, commandTimeout: 360);
					TotalPages = (TotalRecords / (assinanteId == 23 ? 2000 : 200)) + 1;

					if (connection.State != ConnectionState.Closed)
						connection.Close();

					Console.WriteLine("Total Pages: " + TotalPages.ToString());
					for (int i = 1; i <= TotalPages; i++)
					{
						Console.WriteLine("Página: " + i.ToString());
						IEnumerable<IndiceDocumento> listIndex = BuilQueryFromDatabase(assinanteId, PageSize, i);
						var responseBulk = client.Bulk(b => b
							.Index(IdxName)
							.IndexMany(listIndex));

						if (!responseBulk.ApiCall.Success || !responseBulk.IsValid)
						{
							Console.WriteLine("round: " + i.ToString());
							Console.WriteLine(responseBulk.DebugInformation);
							Console.WriteLine(responseBulk.OriginalException);

							throw new Exception($"Error populating index.\r\n{responseBulk.DebugInformation}.", responseBulk.OriginalException);
						}
					}
				}

				//foreach (var itemIndex in listIndex)
				//{
				//	var response = client.Index(itemIndex, idx => idx.Index(IdxName));
				//}
			}

			//foreach (var itemIndex in listIndex)
			//{
			//	var response = client.Index(itemIndex, idx => idx.Index("docindex"));
			//}
		}

		// count structure index
		private const string sql_count = @"
					  SELECT count(Id)
						FROM [dbo].[Indexacao];";

		// structure index
		private const string sqlv2 = @"SELECT
									DocumentoId,
									RegisterKey,
									CodigoRepositorio,
									NomeRepositorio,
									NomeDocumento,
									CodigoPasta,
									NomePasta,
									ConteudoPropriedade,
									NomeTipoDocumento,
                                    ArquivoKey,
                                    NomeArquivoOriginal,
                                    Extensao,
									Comentario,
                                    ContentType,
									UltimaAlteracao,
									Inclusao,
									FileUltimaAlteracao
						FROM [dbo].[Indexacao]
					 ORDER BY Id
					OFFSET @PageSize * (@PageNumber - 1) ROWS
					FETCH NEXT @PageSize ROWS ONLY;";

		private static IEnumerable<IndiceDocumento> BuilQueryFromDatabase(int assinanteId, int pageSize, int pageNumber)
		{
			IEnumerable<IndiceDocumento> list;

			using (var connection = new System.Data.SqlClient.SqlConnection(configuration.GetConnectionString("DefaultConnection")))
			{
				if (connection.State != ConnectionState.Open)
					connection.Open();

				list = connection.Query<IndiceDocumento>(sqlv2,
					new
					{
						assinanteId,
						PageSize = pageSize,
						PageNumber = pageNumber
					});

				if (connection.State != ConnectionState.Closed)
					connection.Close();
			}

			return list;
		}
	}
}