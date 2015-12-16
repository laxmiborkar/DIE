USE [esp_stage]
GO

/****** Object:  StoredProcedure [dbo].[dieEspPiping]    Script Date: 12/16/2015 8:43:17 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		Laxmi
-- Create date: 12.10.2015
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[dieEspPiping]
	-- Add the parameters for the stored procedure here

AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    -- Insert statements for procedure here

	if object_id('tempdb..#PipingPull') is not null drop table #PipingPull;
	with  pipingPull as (
		Select *
		,case
				when CasingBottom > CasingTop then CasingBottom-CasingTop
				else null end as [CasingLength] 
		From (
		Select distinct
				ch.API10 as [API] ,ch.[CompletionID],ch.PurposeOfFiling,ch.state_name as [State] ,ch.CompletionDate as [CompletionDate]
				--,p.*
				,case
				when CasingRecordType = 'CASING' and StringType in ('CONDUCTOR','INTERMEDIATE','LINER','PRODUCTION','SURFACE','TUBING') then UPPER(StringType)
				when CasingRecordType IS null OR CasingRecordType = '' OR CasingRecordType = 'UNKNOWN' then 'CASING'
				else UPPER(CasingRecordType) end as [CasingRecordType]
				,case
				when CasingSize <= 0.99 then null
				when CasingSize >= 100 then null
				else CasingSize end as [CasingSize]
				,case
				when HoleSize <= 0 then null
				when HoleSize >= 100 then null
				else HoleSize end as [HoleSize]
				,case
				when CasingBottom <= 10 then null
				else CasingBottom end as [CasingBottom]
				,case
				when CasingBottom <= 10  AND CasingTop = 0 then null
				else CasingTop end as [CasingTop]
				,case
				when CasingWeight <= 0 OR CasingWeight >500  then null
				else CasingWeight end as [CasingWeight]
				,case
				when CementAmount <= 0  AND CementClass = 'UNKNOWN' then null
				else CementAmount end as [CementAmount]
				,case
				when CementAmount <= 0  AND CementClass = 'UNKNOWN' then null
				else CementClass end as [CementClass]
				,case
				when CementAmount <= 0  AND CementClass = 'UNKNOWN' and TopOfCement=0 then null
				else TopOfCement end as [TopOfCement]
				,case
				when CementAmount <= 0  AND CementClass = 'UNKNOWN' and TopOfCement=0 and TopCementDeterminedBy='UNKNOWN' then null
				else TopCementDeterminedBy end as [TopCementDeterminedBy]
				,case
				when SlurryVolume = 0 then null
				else SlurryVolume end as [SlurryVolume]
				-- add measured depth?
		From  [esp_data].[dbo].[cmpHeader] ch
		Join [esp_data].[dbo].[cmpPipe] p   ON p.CompletionID=ch.CompletionID
		Where state_name in  ('LOUISIANA','MONTANA','NEW MEXICO','NORTH DAKOTA','OKLAHOMA','TEXAS','WYOMING') and p.CompletionID<>'-1'
		--and ch.API10='4250536539'
		)a
		WHERE (CasingSize is not null or HoleSize is not null or CasingBottom is not null or	CasingTop is not null or CasingWeight is not null or CementAmount is not null or CementClass is not null or TopOfCement is not null or TopCementDeterminedBy is not null or SlurryVolume is not null)
				
	)

		SELECT * INTO #pipingPull FROM pipingPull
	raiserror('#pipingPull created', 10,1) with nowait;

	if object_id('tempdb..#espHeaderPull') is not null drop table #espHeaderPull;
		With espHeaderPull as (
			SELECT DISTINCT 
			 api
			,play
			,latitude27
			,longitude27
			,operator
			,DENSE_RANK() OVER (PARTITION BY api ORDER BY firstProductionDate DESC,completionDate DESC) as dateRank
			,MIN(h.firstProductionDate) OVER (PARTITION BY api) as minDate
			FROM [esp_live].[dbo].[espHeader] h
			WHERE api>'0'
		)

		SELECT * INTO #espHeaderPull FROM espHeaderPull
	raiserror('#espHeaderPull created', 10,1) with nowait;


	if object_id('esp_stage.dbo.espPiping') is not null drop table esp_stage.dbo.espPiping;
		select DISTINCT 
		p.API
		, p.CompletionID as completionId
		, UPPER(h.play) as play
		,h.latitude27 as latitude
		,h.longitude27 as longitude
		,h.minDate as firstProductionDate
		,h.operator as currentOperator
		, UPPER(p.PurposeOfFiling) as filingPrupose
		, p.CasingRecordType as casingType
		, p.CasingSize as casingSize
		, p.HoleSize as holeSize
		, p.CasingBottom as casingBottom
		, p.CasingTop as casingTop
		, p.CasingWeight as casingWeight
		, p.CementAmount as cementAmount
		, p.CementClass as cementClass
		, p.TopOfCement as cementTop
		, UPPER(p.TopCementDeterminedBy) as cementTopSource
		, p.SlurryVolume slurryVolume
		, p.CasingLength as casingLength
		
		Into esp_stage.dbo.espPiping
		from #espHeaderPull h
		Inner join #pipingPull p 
		on p.API=h.api AND h.dateRank=1
	raiserror('esp_stage.dbo.espPiping', 10,1) with nowait;
END

GO

