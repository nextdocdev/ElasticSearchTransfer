using System;

namespace IndexAndTranfer
{
	internal class IndiceDocumento
	{
		public int DocumentoId { get; set; }
		public int CodigoPasta { get; set; }
		public Guid RegisterKey { get; set; }
		public int CodigoRepositorio { get; set; }
		public string NomeRepositorio { get; set; }
		public string NomeDocumento { get; set; }
		public string ConteudoPropriedade { get; set; }
		public string NomePasta { get; set; }
		public int CodigoTipoDocumento { get; set; }
		public string NomeTipoDocumento { get; set; }
		public Guid ArquivoKey { get; set; }
		public string NomeArquivoOriginal { get; set; }
		public string Extensao { get; set; }
		public string ContentType { get; set; }
		public string Comentario { get; set; }

		public DateTime? Inclusao { get; set; }
		public DateTime? UltimaAlteracao { get; set; }
		public DateTime? FileUltimaAlteracao { get; set; }

		public IndiceDocumento()
		{
		}

		public IndiceDocumento(
			int docId,
			int codigoPasta,
			Guid registerKey,
			int codigoRepositorio,
			string nomeRepositorio,
			string nomeDocumento,
			string conteudoDocumento,
			string nomePasta,
			int codigoTipoDocumento,
			string nomeTipoDocumento,
			Guid arquivoKey,
			string nomeArquivoOriginal,
			string extensao,
			string contentType,
			string comentario)
		{
			DocumentoId = docId;
			CodigoPasta = codigoPasta;
			RegisterKey = registerKey;
			CodigoRepositorio = codigoRepositorio;
			NomeRepositorio = nomeRepositorio;
			NomeDocumento = nomeDocumento;
			ConteudoPropriedade = conteudoDocumento;
			NomePasta = nomePasta;
			CodigoTipoDocumento = codigoTipoDocumento;
			NomeTipoDocumento = nomeTipoDocumento;
			ArquivoKey = arquivoKey;
			NomeArquivoOriginal = nomeArquivoOriginal;
			Extensao = extensao;
			ContentType = contentType;
			Comentario = comentario;
		}
	}
}