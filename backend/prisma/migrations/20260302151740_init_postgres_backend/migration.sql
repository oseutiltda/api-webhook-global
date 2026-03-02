-- CreateTable
CREATE TABLE "WebhookEvent" (
    "id" TEXT NOT NULL,
    "source" TEXT NOT NULL,
    "receivedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "status" TEXT DEFAULT 'pending',
    "processedAt" TIMESTAMP(3),
    "errorMessage" VARCHAR(1000),
    "retryCount" INTEGER NOT NULL DEFAULT 0,
    "integrationStatus" VARCHAR(50),
    "processingTimeMs" INTEGER,
    "integrationTimeMs" INTEGER,
    "seniorId" VARCHAR(255),
    "metadata" VARCHAR(2000),
    "tipoIntegracao" VARCHAR(50),

    CONSTRAINT "WebhookEvent_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "CteAutorizado" (
    "id" TEXT NOT NULL,
    "authorizationNumber" INTEGER NOT NULL,
    "status" TEXT NOT NULL,
    "xml" VARCHAR(4000) NOT NULL,
    "eventXml" VARCHAR(4000),
    "chCTe" VARCHAR(60),
    "nProt" VARCHAR(60),
    "dhEmi" VARCHAR(40),
    "dhRecbto" VARCHAR(40),
    "serie" VARCHAR(10),
    "nCT" VARCHAR(20),
    "emitCnpj" VARCHAR(20),
    "destCnpj" VARCHAR(20),
    "vTPrest" DOUBLE PRECISION,
    "vRec" DOUBLE PRECISION,

    CONSTRAINT "CteAutorizado_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "CteCancelado" (
    "id" TEXT NOT NULL,
    "authorizationNumber" INTEGER NOT NULL,
    "status" TEXT NOT NULL,
    "xml" VARCHAR(4000) NOT NULL,
    "eventXml" VARCHAR(4000),
    "chCTe" VARCHAR(60),
    "nProt" VARCHAR(60),
    "dhRegEvento" VARCHAR(40),
    "tpEvento" VARCHAR(20),
    "xEvento" VARCHAR(60),
    "emitCnpj" VARCHAR(20),

    CONSTRAINT "CteCancelado_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "CiotBase" (
    "id" TEXT NOT NULL,
    "ciot" TEXT NOT NULL,
    "transportadoraCnpj" TEXT NOT NULL,
    "criadoEm" TEXT NOT NULL,

    CONSTRAINT "CiotBase_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "CiotParcela" (
    "id" TEXT NOT NULL,
    "nrciot" TEXT NOT NULL,
    "cdempresa" TEXT NOT NULL,
    "cdcartafrete" TEXT NOT NULL,
    "nrcgccpfprop" TEXT NOT NULL,
    "tpformapagamentocontratado" TEXT NOT NULL,
    "nrformapagamentocontratado" TEXT NOT NULL,
    "nrcgccpfmot" TEXT NOT NULL,
    "tpformapagamentomotorista" TEXT NOT NULL,
    "nrformapagamentomotorista" TEXT NOT NULL,
    "tpformapagamentosubcontratado" TEXT NOT NULL,
    "nrformapagamentosubcontratado" TEXT NOT NULL,
    "dtemissao" TEXT NOT NULL,
    "vlcarga" DOUBLE PRECISION,
    "qtpesocarga" DOUBLE PRECISION,
    "nrmanifesto" TEXT NOT NULL,
    "nrplaca" TEXT NOT NULL,
    "nrceporigem" TEXT NOT NULL,
    "nrcepdestino" TEXT NOT NULL,
    "fgemitida" INTEGER,
    "dtliberacaopagto" TEXT,
    "cdcentrocusto" TEXT,
    "insituacao" INTEGER NOT NULL,
    "cdcondicaovencto" INTEGER NOT NULL,
    "dsobservacao" TEXT,
    "cdtipotransporte" TEXT,
    "cdremetente" TEXT NOT NULL,
    "cddestinatario" TEXT NOT NULL,
    "cdnaturezacarga" TEXT NOT NULL,
    "cdespeciecarga" TEXT NOT NULL,
    "clmercadoria" TEXT,
    "qtpeso" DOUBLE PRECISION,
    "cdempresaconhec" TEXT,
    "nrseqcontrole" TEXT,
    "nrnotafiscal" TEXT NOT NULL,
    "cdhistorico" TEXT,
    "dsusuarioinc" TEXT NOT NULL,
    "dsusuariocanc" TEXT,
    "dtinclusao" TEXT NOT NULL,
    "dtcancelamento" TEXT,
    "intipoorigem" TEXT NOT NULL,
    "nrplacareboque1" TEXT NOT NULL,
    "nrplacareboque2" TEXT,
    "nrplacareboque3" TEXT,
    "cdtarifa" INTEGER NOT NULL,
    "dsusuarioacerto" TEXT,
    "dtacerto" TEXT,
    "cdinscricaocomp" TEXT,
    "nrseriecomp" TEXT,
    "nrcomprovante" TEXT,
    "vlfrete" DOUBLE PRECISION NOT NULL,
    "insestsenat" INTEGER,
    "cdmotivocancelamento" TEXT,
    "dsobscancelamento" TEXT,
    "inveiculoproprio" INTEGER NOT NULL,
    "dsusuarioimpressao" TEXT,
    "dtimpressao" TEXT,
    "dtprazomaxentrega" TEXT NOT NULL,
    "nrseloautenticidade" TEXT,
    "hrmaxentrega" TEXT,
    "cdvinculacaoiss" TEXT,
    "dthrretornociot" TEXT,
    "cdciot" TEXT,
    "serie" INTEGER NOT NULL,
    "cdmsgretornociot" TEXT,
    "dsmsgretornociot" TEXT,
    "inenvioarquivociot" INTEGER,
    "dsavisotransportador" TEXT,
    "nrprotocolocancciot" TEXT,
    "cdndot" TEXT,
    "nrprotocoloautndot" TEXT,
    "inoperacaoperiodo" INTEGER,
    "vlfreteestimado" DOUBLE PRECISION,
    "inoperacaodistribuicao" INTEGER NOT NULL,
    "nrprotocoloenctociot" TEXT,
    "indotimpresso" TEXT,
    "inveiculo" INTEGER NOT NULL,
    "cdrota" TEXT NOT NULL,
    "inoperadorapagtoctrb" TEXT NOT NULL,
    "inrespostaquesttacagreg" INTEGER NOT NULL,
    "cdmoeda" TEXT,
    "nrprotocolointerroociot" TEXT,
    "inretimposto" TEXT,
    "cdintersenior" TEXT,
    "nrcodigooperpagtociot" TEXT,
    "cdseqhcm" TEXT,
    "insitcalcpedagio" TEXT,
    "nrrepom" TEXT,
    "vlmanifesto" DOUBLE PRECISION NOT NULL,
    "vlcombustivel" DOUBLE PRECISION NOT NULL,
    "vlpedagio" DOUBLE PRECISION NOT NULL,
    "vlnotacreditodebito" DOUBLE PRECISION NOT NULL,
    "vldesconto" DOUBLE PRECISION NOT NULL,
    "vlcsll" DOUBLE PRECISION NOT NULL,
    "vlpis" DOUBLE PRECISION NOT NULL,
    "vlirff" DOUBLE PRECISION NOT NULL,
    "vlinss" DOUBLE PRECISION NOT NULL,
    "vltotalmanifesto" DOUBLE PRECISION NOT NULL,
    "vlabastecimento" DOUBLE PRECISION NOT NULL,
    "vladiantamento" DOUBLE PRECISION NOT NULL,
    "vlir" DOUBLE PRECISION NOT NULL,
    "vlsaldoapagar" DOUBLE PRECISION NOT NULL,
    "vlsaldofrete" DOUBLE PRECISION NOT NULL,
    "vlcofins" DOUBLE PRECISION NOT NULL,
    "vlsestsenat" DOUBLE PRECISION NOT NULL,
    "vliss" DOUBLE PRECISION NOT NULL,
    "cdtributacao" TEXT,
    "vlcsl" DOUBLE PRECISION NOT NULL,

    CONSTRAINT "CiotParcela_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "CiotParcelaItem" (
    "id" TEXT NOT NULL,
    "manifestoId" TEXT NOT NULL,
    "idparcela" TEXT NOT NULL,
    "nrciotsistema" TEXT NOT NULL,
    "nrciot" TEXT NOT NULL,
    "dstipo" TEXT NOT NULL,
    "dsstatus" TEXT NOT NULL,
    "cdfavorecido" TEXT NOT NULL,
    "cdcartafrete" TEXT NOT NULL,
    "cdevento" TEXT NOT NULL,
    "dtpagto" TEXT NOT NULL,
    "indesconto" DOUBLE PRECISION,
    "vlbasecalculo" DOUBLE PRECISION,
    "dtrecebimento" TEXT,
    "vloriginal" DOUBLE PRECISION,
    "dtinclusao" TEXT NOT NULL,
    "hrinclusao" TEXT NOT NULL,
    "dsusuarioinc" TEXT NOT NULL,
    "dtreferenciacalculo" TEXT NOT NULL,
    "dsobservacao" TEXT,
    "vlprovisionado" DOUBLE PRECISION,

    CONSTRAINT "CiotParcelaItem_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "CiotFaturamento" (
    "id" TEXT NOT NULL,
    "manifestoId" TEXT NOT NULL,
    "cdempresa" TEXT NOT NULL,
    "cdcartafrete" TEXT NOT NULL,
    "cdempresaFV" TEXT NOT NULL,
    "nrficha" TEXT,

    CONSTRAINT "CiotFaturamento_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "FaturaPagar" (
    "id" TEXT NOT NULL,
    "fornecedorCnpj" TEXT NOT NULL,
    "numero" TEXT NOT NULL,
    "emissao" TEXT NOT NULL,
    "valor" DOUBLE PRECISION NOT NULL,

    CONSTRAINT "FaturaPagar_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "FaturaPagarCancelamento" (
    "id" TEXT NOT NULL,
    "numero" TEXT NOT NULL,
    "motivo" TEXT NOT NULL,

    CONSTRAINT "FaturaPagarCancelamento_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "FaturaPagarParcela" (
    "id" TEXT NOT NULL,
    "faturaId" TEXT NOT NULL,
    "posicao" INTEGER NOT NULL,
    "dueDate" TEXT NOT NULL,
    "valor" DOUBLE PRECISION NOT NULL,
    "interestValue" DOUBLE PRECISION,
    "discountValue" DOUBLE PRECISION,
    "paymentMethod" TEXT,
    "comments" TEXT,
    "installmentId" INTEGER,

    CONSTRAINT "FaturaPagarParcela_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "FaturaPagarBaixa" (
    "id" TEXT NOT NULL,
    "parcelaId" TEXT NOT NULL,
    "pagamentoEm" TEXT NOT NULL,
    "valorPago" DOUBLE PRECISION NOT NULL,
    "desconto" DOUBLE PRECISION,
    "juros" DOUBLE PRECISION,
    "formaPagamento" TEXT,
    "contaBancaria" TEXT,
    "banco" TEXT,
    "numeroConta" TEXT,
    "digitoConta" TEXT,
    "observacao" TEXT,

    CONSTRAINT "FaturaPagarBaixa_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "FaturaReceber" (
    "id" TEXT NOT NULL,
    "clienteCnpj" TEXT NOT NULL,
    "numero" TEXT NOT NULL,
    "emissao" TEXT NOT NULL,
    "valor" DOUBLE PRECISION NOT NULL,

    CONSTRAINT "FaturaReceber_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "FaturaReceberBaixa" (
    "id" TEXT NOT NULL,
    "parcelaId" TEXT NOT NULL,
    "recebimentoEm" TEXT NOT NULL,
    "valorRecebido" DOUBLE PRECISION NOT NULL,
    "desconto" DOUBLE PRECISION,
    "juros" DOUBLE PRECISION,
    "formaPagamento" TEXT,
    "contaBancaria" TEXT,
    "banco" TEXT,
    "numeroConta" TEXT,
    "digitoConta" TEXT,
    "observacao" TEXT,

    CONSTRAINT "FaturaReceberBaixa_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "FaturaReceberParcela" (
    "id" TEXT NOT NULL,
    "faturaId" TEXT NOT NULL,
    "posicao" INTEGER NOT NULL,
    "dueDate" TEXT NOT NULL,
    "valor" DOUBLE PRECISION NOT NULL,
    "interestValue" DOUBLE PRECISION,
    "discountValue" DOUBLE PRECISION,
    "paymentMethod" TEXT,
    "comments" TEXT,
    "installmentId" INTEGER,

    CONSTRAINT "FaturaReceberParcela_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "FaturaReceberItem" (
    "id" TEXT NOT NULL,
    "faturaId" TEXT NOT NULL,
    "cteKey" TEXT,
    "cteNumber" TEXT,
    "cteSeries" TEXT,
    "payerName" TEXT,
    "draftNumber" TEXT,
    "nfseNumber" TEXT,
    "nfseSeries" TEXT,
    "total" DOUBLE PRECISION,
    "type" TEXT,

    CONSTRAINT "FaturaReceberItem_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "NfseAutorizado" (
    "id" TEXT NOT NULL,
    "numero" INTEGER NOT NULL,
    "codigoVerificacao" TEXT,
    "dataEmissao" TEXT NOT NULL,
    "rpsNumero" INTEGER NOT NULL,
    "rpsSerie" TEXT NOT NULL,
    "rpsTipo" INTEGER NOT NULL,
    "dataEmissaoRps" TEXT NOT NULL,
    "naturezaOperacao" INTEGER NOT NULL,
    "optanteSimples" INTEGER NOT NULL,
    "incentivadorCultural" INTEGER NOT NULL,
    "competencia" TEXT NOT NULL,
    "itemListaServico" INTEGER NOT NULL,
    "codTribMunicipio" TEXT NOT NULL,
    "discriminacao" TEXT NOT NULL,
    "codMunicipioServ" INTEGER NOT NULL,
    "valorServicos" DOUBLE PRECISION NOT NULL,
    "valorDeducoes" DOUBLE PRECISION NOT NULL,
    "issRetido" DOUBLE PRECISION NOT NULL,
    "valorIss" DOUBLE PRECISION NOT NULL,
    "valorIssRetido" DOUBLE PRECISION NOT NULL,
    "baseCalculo" DOUBLE PRECISION NOT NULL,
    "aliquota" DOUBLE PRECISION NOT NULL,
    "valorLiquidoNfse" DOUBLE PRECISION NOT NULL,
    "valorCredito" DOUBLE PRECISION NOT NULL,
    "prestadorCnpj" TEXT NOT NULL,
    "prestadorIM" TEXT NOT NULL,
    "prestadorRazao" TEXT NOT NULL,
    "prestadorFantasia" TEXT NOT NULL,
    "prestadorLogradouro" TEXT,
    "prestadorNumero" INTEGER,
    "prestadorComplemento" TEXT,
    "prestadorBairro" TEXT,
    "prestadorCodMunicipio" INTEGER,
    "prestadorUf" TEXT,
    "prestadorCep" TEXT,
    "prestadorTelefone" TEXT,
    "prestadorEmail" TEXT,
    "tomadorCnpj" TEXT NOT NULL,
    "tomadorIM" TEXT NOT NULL,
    "tomadorRazao" TEXT NOT NULL,
    "tomadorLogradouro" TEXT,
    "tomadorNumero" INTEGER,
    "tomadorComplemento" TEXT,
    "tomadorBairro" TEXT,
    "tomadorCodMunicipio" INTEGER,
    "tomadorUf" TEXT,
    "tomadorCep" TEXT,
    "tomadorTelefone" TEXT,
    "tomadorEmail" TEXT,
    "orgaoCodMunicipio" INTEGER NOT NULL,
    "orgaoUf" TEXT NOT NULL,
    "pesoReal" DOUBLE PRECISION,
    "pesoCubado" DOUBLE PRECISION,
    "quantidadeVolumes" INTEGER,
    "valorProduto" DOUBLE PRECISION,
    "valorNota" DOUBLE PRECISION,
    "valorFretePeso" DOUBLE PRECISION,
    "valorAdv" DOUBLE PRECISION,
    "valorOutros" DOUBLE PRECISION,
    "comentarioFrete" VARCHAR(4000),
    "usuarioAlteracao" TEXT,
    "dataAlteracao" TEXT,
    "dataCancelamento" TEXT,
    "motivoCancelamento" TEXT,
    "filialCancelamento" TEXT,
    "cnpjConsignatario" TEXT,
    "cnpjRedespacho" TEXT,
    "cnpjExpedidor" TEXT,
    "valorBaseCalculoPis" DOUBLE PRECISION,
    "aliqPis" DOUBLE PRECISION,
    "valorPis" DOUBLE PRECISION,
    "aliqCofins" DOUBLE PRECISION,
    "valorCofins" DOUBLE PRECISION,
    "dataPrazo" TEXT,
    "diasEntrega" INTEGER,
    "cnpjRemetente" TEXT,
    "cepRemetente" TEXT,
    "cnpjDestinatario" TEXT,
    "cepDestinatario" TEXT,
    "logradouroDestinatario" TEXT,

    CONSTRAINT "NfseAutorizado_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Pessoa" (
    "id" TEXT NOT NULL,
    "nomeRazaoSocial" TEXT,
    "nomeFantasia" TEXT,
    "cpf" TEXT,
    "cnpj" TEXT,
    "inscricaoMunicipal" TEXT,
    "inscricaoEstadual" TEXT,
    "ativo" BOOLEAN,
    "dataCadastro" TEXT,
    "usuarioCadastro" TEXT,
    "payload" VARCHAR(4000),

    CONSTRAINT "Pessoa_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "PessoaEndereco" (
    "id" TEXT NOT NULL,
    "pessoaId" TEXT NOT NULL,
    "codTipoEndereco" INTEGER NOT NULL,
    "descricaoTipoEndereco" TEXT,
    "cep" TEXT,
    "logradouro" TEXT,
    "numero" TEXT,
    "complemento" TEXT,
    "bairro" TEXT,
    "cidade" TEXT,
    "estado" TEXT,

    CONSTRAINT "PessoaEndereco_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Nfse" (
    "id" SERIAL NOT NULL,
    "external_id" INTEGER,
    "NumeroNfse" INTEGER NOT NULL,
    "CodigoVerificacao" VARCHAR(100),
    "DataEmissao" TIMESTAMP(3),
    "NumeroIdentificacaoRps" INTEGER,
    "SerieIdentificacaoRps" VARCHAR(100),
    "TipoIdentificacaoRps" INTEGER,
    "DataEmissaoRps" TIMESTAMP(3),
    "NaturezaOperacao" INTEGER,
    "OptanteSimplesNacional" INTEGER,
    "IncentivadorCultural" INTEGER,
    "DtCompetencia" TIMESTAMP(3),
    "ValorServicos" DECIMAL(18,2),
    "ValorDeducoes" DECIMAL(18,2),
    "IssRetido" INTEGER,
    "ValorIss" DECIMAL(18,2),
    "ValorIssRetido" DECIMAL(18,2),
    "BaseCalculo" DECIMAL(18,2),
    "Aliquota" DECIMAL(18,2),
    "ValorLiquidoNfse" DECIMAL(18,2),
    "ItemListaServico" DECIMAL(18,2),
    "CdTributacaoMunicipio" VARCHAR(50),
    "Discriminacao" VARCHAR(255),
    "CodigoMunicipio" INTEGER,
    "ValorCredito" DECIMAL(18,2),
    "corporation_id" INTEGER NOT NULL,
    "external_idPrestador" INTEGER NOT NULL,
    "person_idPrestador" INTEGER NOT NULL,
    "CnpjIdentPrestador" VARCHAR(14) NOT NULL,
    "InscMunicipalPrestador" VARCHAR(100),
    "RazaoSocialPrestador" VARCHAR(255) NOT NULL,
    "NomeFantasiaPrestador" VARCHAR(255),
    "CodEnderecoPrestador" INTEGER,
    "CodContatoPrestador" INTEGER,
    "customer_id" INTEGER NOT NULL,
    "external_idTomador" INTEGER NOT NULL,
    "CnpjIdentTomador" VARCHAR(14),
    "CpfIdentTomador" VARCHAR(11),
    "InscMunicipalTomador" VARCHAR(100),
    "RazaoSocialTomador" VARCHAR(255),
    "CodEnderecoTomador" INTEGER,
    "CodContatoTomador" INTEGER,
    "CdMunicipioOrgaoGerador" INTEGER,
    "UFOrgaoGerador" VARCHAR(50),
    "created_at" TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3),
    "status" VARCHAR(20) NOT NULL,
    "error_message" TEXT,
    "processed" BOOLEAN NOT NULL DEFAULT false,
    "cancelado" BOOLEAN DEFAULT false,
    "Obscancelado" VARCHAR(255),
    "Prestador_Logradouro" VARCHAR(255),
    "Prestador_Numero" VARCHAR(50),
    "Prestador_Complemento" VARCHAR(255),
    "Prestador_Bairro" VARCHAR(255),
    "Prestador_CodigoMunicipio" INTEGER,
    "Prestador_UF" VARCHAR(10),
    "Prestador_CEP" VARCHAR(20),
    "Prestador_Telefone" VARCHAR(50),
    "Prestador_Email" VARCHAR(255),
    "Tomador_Logradouro" VARCHAR(255),
    "Tomador_Numero" VARCHAR(50),
    "Tomador_Complemento" VARCHAR(255),
    "Tomador_Bairro" VARCHAR(255),
    "Tomador_CodigoMunicipio" INTEGER,
    "Tomador_UF" VARCHAR(10),
    "Tomador_CEP" VARCHAR(20),
    "Tomador_Telefone" VARCHAR(50),
    "Tomador_Email" VARCHAR(255),
    "PesoReal" DECIMAL(18,2),
    "PesoCubado" DECIMAL(18,2),
    "QuantidadeVolumes" INTEGER,
    "ValorProduto" DECIMAL(18,2),
    "ValorNota" DECIMAL(18,2),
    "ValorFretePeso" DECIMAL(18,2),
    "ValorAdv" DECIMAL(18,2),
    "ValorOutros" DECIMAL(18,2),
    "ComentarioFrete" TEXT,
    "UsuarioAlteracao" VARCHAR(255),
    "DataDeAlteracao" TIMESTAMP(3),
    "DataCancelamento" TIMESTAMP(3),
    "MotivoCancelamento" VARCHAR(255),
    "FilialCancelamento" VARCHAR(100),
    "CnpjConsignatario" VARCHAR(20),
    "CnpjRedespacho" VARCHAR(255),
    "CnpjExpedidor" VARCHAR(20),
    "ValorBaseCalculoPIS" DECIMAL(18,2),
    "AliqPIS" DECIMAL(18,2),
    "ValorPIS" DECIMAL(18,2),
    "AliqCOFINS" DECIMAL(18,2),
    "ValorCOFINS" DECIMAL(18,2),
    "DataPrazo" TIMESTAMP(3),
    "DiasEntrega" INTEGER,
    "CnpjRemetente" VARCHAR(20),
    "CepRemetente" VARCHAR(20),
    "CnpjDestinatario" VARCHAR(20),
    "CepDestinatario" VARCHAR(20),
    "LogradouroDestinatario" VARCHAR(255),
    "NumeroJsonOriginal" INTEGER,

    CONSTRAINT "Nfse_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ctes" (
    "id" SERIAL NOT NULL,
    "external_id" INTEGER NOT NULL,
    "authorization_number" INTEGER NOT NULL,
    "status" VARCHAR(50) NOT NULL,
    "xml" TEXT NOT NULL,
    "event_xml" TEXT,
    "processed" BOOLEAN NOT NULL DEFAULT false,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3),

    CONSTRAINT "ctes_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "WebhookEvent_status_idx" ON "WebhookEvent"("status");

-- CreateIndex
CREATE INDEX "WebhookEvent_receivedAt_idx" ON "WebhookEvent"("receivedAt");

-- CreateIndex
CREATE INDEX "WebhookEvent_integrationStatus_idx" ON "WebhookEvent"("integrationStatus");

-- CreateIndex
CREATE INDEX "WebhookEvent_source_idx" ON "WebhookEvent"("source");

-- CreateIndex
CREATE INDEX "WebhookEvent_tipoIntegracao_idx" ON "WebhookEvent"("tipoIntegracao");

-- CreateIndex
CREATE INDEX "CteAutorizado_chCTe_idx" ON "CteAutorizado"("chCTe");

-- CreateIndex
CREATE INDEX "CteAutorizado_nProt_idx" ON "CteAutorizado"("nProt");

-- CreateIndex
CREATE INDEX "CteAutorizado_emitCnpj_idx" ON "CteAutorizado"("emitCnpj");

-- CreateIndex
CREATE INDEX "CteAutorizado_destCnpj_idx" ON "CteAutorizado"("destCnpj");

-- CreateIndex
CREATE INDEX "CteCancelado_chCTe_idx" ON "CteCancelado"("chCTe");

-- CreateIndex
CREATE INDEX "CteCancelado_nProt_idx" ON "CteCancelado"("nProt");

-- CreateIndex
CREATE INDEX "CiotParcelaItem_manifestoId_idx" ON "CiotParcelaItem"("manifestoId");

-- CreateIndex
CREATE UNIQUE INDEX "CiotFaturamento_manifestoId_key" ON "CiotFaturamento"("manifestoId");

-- CreateIndex
CREATE INDEX "FaturaPagarParcela_faturaId_idx" ON "FaturaPagarParcela"("faturaId");

-- CreateIndex
CREATE INDEX "FaturaPagarBaixa_parcelaId_idx" ON "FaturaPagarBaixa"("parcelaId");

-- CreateIndex
CREATE INDEX "FaturaReceberBaixa_parcelaId_idx" ON "FaturaReceberBaixa"("parcelaId");

-- CreateIndex
CREATE INDEX "FaturaReceberParcela_faturaId_idx" ON "FaturaReceberParcela"("faturaId");

-- CreateIndex
CREATE INDEX "FaturaReceberItem_faturaId_idx" ON "FaturaReceberItem"("faturaId");

-- CreateIndex
CREATE INDEX "NfseAutorizado_prestadorCnpj_idx" ON "NfseAutorizado"("prestadorCnpj");

-- CreateIndex
CREATE INDEX "NfseAutorizado_tomadorCnpj_idx" ON "NfseAutorizado"("tomadorCnpj");

-- CreateIndex
CREATE INDEX "PessoaEndereco_pessoaId_idx" ON "PessoaEndereco"("pessoaId");

-- CreateIndex
CREATE INDEX "Nfse_NumeroNfse_idx" ON "Nfse"("NumeroNfse");

-- CreateIndex
CREATE INDEX "Nfse_CnpjIdentPrestador_idx" ON "Nfse"("CnpjIdentPrestador");

-- CreateIndex
CREATE INDEX "Nfse_customer_id_idx" ON "Nfse"("customer_id");

-- CreateIndex
CREATE INDEX "Nfse_corporation_id_idx" ON "Nfse"("corporation_id");

-- CreateIndex
CREATE INDEX "ctes_external_id_idx" ON "ctes"("external_id");

-- CreateIndex
CREATE INDEX "ctes_authorization_number_idx" ON "ctes"("authorization_number");

-- CreateIndex
CREATE INDEX "ctes_status_idx" ON "ctes"("status");

-- CreateIndex
CREATE INDEX "ctes_processed_idx" ON "ctes"("processed");

-- AddForeignKey
ALTER TABLE "CiotParcelaItem" ADD CONSTRAINT "CiotParcelaItem_manifestoId_fkey" FOREIGN KEY ("manifestoId") REFERENCES "CiotParcela"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "CiotFaturamento" ADD CONSTRAINT "CiotFaturamento_manifestoId_fkey" FOREIGN KEY ("manifestoId") REFERENCES "CiotParcela"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "FaturaPagarParcela" ADD CONSTRAINT "FaturaPagarParcela_faturaId_fkey" FOREIGN KEY ("faturaId") REFERENCES "FaturaPagar"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "FaturaPagarBaixa" ADD CONSTRAINT "FaturaPagarBaixa_parcelaId_fkey" FOREIGN KEY ("parcelaId") REFERENCES "FaturaPagarParcela"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "FaturaReceberBaixa" ADD CONSTRAINT "FaturaReceberBaixa_parcelaId_fkey" FOREIGN KEY ("parcelaId") REFERENCES "FaturaReceberParcela"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "FaturaReceberParcela" ADD CONSTRAINT "FaturaReceberParcela_faturaId_fkey" FOREIGN KEY ("faturaId") REFERENCES "FaturaReceber"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "FaturaReceberItem" ADD CONSTRAINT "FaturaReceberItem_faturaId_fkey" FOREIGN KEY ("faturaId") REFERENCES "FaturaReceber"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "PessoaEndereco" ADD CONSTRAINT "PessoaEndereco_pessoaId_fkey" FOREIGN KEY ("pessoaId") REFERENCES "Pessoa"("id") ON DELETE CASCADE ON UPDATE CASCADE;
