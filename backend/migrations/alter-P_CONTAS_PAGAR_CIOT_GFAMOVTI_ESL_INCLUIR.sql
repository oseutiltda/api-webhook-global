-- ============================================
-- Migration: Modificar P_CONTAS_PAGAR_CIOT_GFAMOVTI_ESL_INCLUIR
-- Descrição: Adiciona parâmetro @DtBaixa para permitir definir DtBaixa = DtDigitacao
-- Banco: AFS_INTEGRADOR
-- ============================================

USE [AFS_INTEGRADOR]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[P_CONTAS_PAGAR_CIOT_GFAMOVTI_ESL_INCLUIR]  
(
	 @InPagarReceber	INT
	,@CdInscricao		VARCHAR(14)
	,@CdTitulo			VARCHAR(15)
	,@CdSequencia		INT
	,@CdTipoLancamento	INT
	,@VlMovimento		numeric(14,4)	
	,@DsObservacao		varchar(max) = null
	,@DsUsuario			VARCHAR(10) = null
	,@CdEmpresaCF		int
	,@CdCartaFrete		int
	,@DtBaixa			DATETIME = null
)
AS
BEGIN
	delete
	from SOFTRAN_BRASILMAXI_HML..GFAMOVTI	
	where InPagarReceber = @InPagarReceber
		and CdInscricao = @CdInscricao	
		and CdTitulo = @CdTitulo	
		and CdSequencia = @CdSequencia	

	IF NOT EXISTS (
					select * 
					from SOFTRAN_BRASILMAXI_HML..GFAMOVTI	
					where InPagarReceber =@InPagarReceber
						and CdInscricao = @CdInscricao	
						and CdTitulo = @CdTitulo	
						and CdSequencia = @CdSequencia	
				)
	BEGIN
		declare @CodIDENTITY int   
		
		-- DtDigitacao sempre usa getdate() (já está correto)
		DECLARE @DtDigitacaoFinal DATETIME = GETDATE()
		-- Se @DtBaixa não for informado, usar o mesmo valor de DtDigitacao
		DECLARE @DtBaixaFinal DATETIME = ISNULL(@DtBaixa, @DtDigitacaoFinal)

		INSERT INTO SOFTRAN_BRASILMAXI_HML..GFAMOVTI
		(
			 [InPagarReceber]
			,[CdInscricao]
			,[CdTitulo]
			,[CdSequencia]
			,[CdTipoLancamento]
			,[VlMovimento]
			,[DtDigitacao]
			,[DtBaixa]
			,[DsObservacao]
			,[CdContaCorrente]
			,[CdSequenciaCC]
			,[CdSequenciaLancto]
			,[CdPortador]
			,[CdEmpresaLote]
			,[CdLote]
			,[CdCCustoContabil]
			,[DsUsuario]
			,[HrDigitacao]
			,[InSituacao]
			,[DsUsuarioCan]
			,[DtCancelamento]
			,[CdMotivoCancelamento]
			,[DsMotivoCancelamento]
			,[FgPagamentoEscritural]
			,[CdLotePagto]
			,[CdEmpresaLotePagto]
			,[InGeradoFatura]
			,[InOrigemEmissao]
			,[CdEmpresaPagto]
			,[CdEmpresaCF]
			,[CdCartaFrete]
			,[InOrigemDigitacao]
			,[CdEmpSolicAntPagto]
			,[CdSolicAntPagto]
			,[CdParcelaSolicAntPag]
			,[CdSequenciaLanctoBKP]
			,[NrSeqArqRemRetIBC]
			,[VlMovimentoAux]
			,[CdMotivoDescto]
			,[fgconversao]
			,[CdSolicCob]
		)
		VALUES
		(
			 @InPagarReceber
			,@CdInscricao
			,@CdTitulo
			,@CdSequencia
			,@CdTipoLancamento
			,@VlMovimento
			,@DtDigitacaoFinal
			,@DtBaixaFinal
			,@DsObservacao
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,@DsUsuario
			,getdate()
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,1
			,NULL
			,@CdEmpresaCF
			,@CdCartaFrete
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
		)

		set @CodIDENTITY = @@IDENTITY   

		select @CodIDENTITY AS 'CodManifest' 
	END
	ELSE
	BEGIN
		select cast(CdInscricao as varchar(100)) + '-' + cast(CdTitulo as varchar(100)) as Manifesto		
		from SOFTRAN_BRASILMAXI_HML..GFAMOVTI	
		where InPagarReceber =@InPagarReceber
			and CdInscricao = @CdInscricao	
			and CdTitulo = @CdTitulo	
			and CdSequencia = @CdSequencia	
	END
END
GO

