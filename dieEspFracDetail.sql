USE [esp_stage]
GO

/****** Object:  StoredProcedure [dbo].[dieEspFracDetail]    Script Date: 12/16/2015 8:42:43 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		Laxmi	
-- Create date: 12.10.2015
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[dieEspFracDetail]
	-- Add the parameters for the stored procedure here

AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    -- Insert statements for procedure here


	if object_id('tempdb..#additiveSummaryPull') is not null drop table #additiveSummaryPull;
		With additiveSummaryPull as (
			SELECT DISTINCT 
				  DRWellID as fracDetailId
				  ,[API] as api10
				  ,CONVERT(varchar(14),[FFAPI14]) as api14
				  ,DRWellID as completionId 
				  ,[FirstSpuddate] as spudDate
				  ,Case when min(h.FirstProducedDate) over (partition by h.API10) >='1951-01-01'  then min(h.FirstProducedDate) over (partition by h.API10) else null end as firstProducedDate
				  ,JobEndDate as completionDate
				  ,'FRACFOCUS' as filingPurpose
				  ,CONVERT(varchar(32),null) as completionType
				  ,[JobStartDate] as jobStartDate
				  ,[JobEndDate] as jobEndDate
				  ,null as depthIntervalFrom
				  ,null as depthIntervalTo
				  ,CONVERT(varchar(10),null) as isSummary
				  ,null as stageNumber
				  ,null as numberOfStages
				  ,ISNULL(MD,h.TotalDepthMD) as measuredDepth
				  ,h.TotalDepthTVD as trueVerticalDepth 
				  ,h.Reservoir as formation
				  ,[IngredientID] as ingredientId
				  ,[CasNumber] as CASNumber
				  ,UPPER([IngredientName]) as materialName
				  ,[CalcIngredientMass] as materialAmount
				  ,Case when IngredientMassUOM='lb' then 'LBS' 
					else UPPER(IngredientMassUOM) end as materialUnit
				  ,UPPER([PurposeName]) as materialType
				  ,[PurposeID] as purposeId
				  ,Case when PurposeDescription='NULL' then null
					when PurposeDescription='UNKNOWN' then null
					when PurposeDescription='' then null
					else PurposeDescription end as purposeDescription
				  ,[SupplierID] as supplierId
				  ,UPPER([SupplierName]) as supplierName
				  ,Case  when [IngredientIsResinCoated]='Y' OR [TradeIsResinCoated]='Y'  then 'RESIN COATED'
					when [IngredientIsArtificial]='Y' OR [TradeIsArtificial]='Y'  then 'ARTIFICIAL'
					when [IngredientIsSand]='Y' or [TradeIsSand]='Y' then 'SAND'
					when PurposeName='PROPPANT' then 'UNKNOWN'
					else null end as proppantType
				  ,Case  when ([TradeSandMeshSize] is not null and [TradeSandMeshSize]<>'') then [TradeSandMeshSize]
					when ([IngredientSandMeshSize] is not null and [IngredientSandMeshSize]<>'') then [IngredientSandMeshSize]
					when PurposeName='PROPPANT' then 'UNKNOWN' 
					else null end as proppantMeshSize
				  ,Case  when ([TradeSandQuality] is not null and [TradeSandQuality]<>'') then [TradeSandQuality]
					when ([IngredientSandQuality] is not null and [IngredientSandQuality]<>'') then [IngredientSandQuality] 
					else null end as sandQuality
				  ,[TradeID] as tradeId
				  ,[TradeName] as tradeName
				  ,UPPER([DisclosureStatus]) as materialDisclosueStatus
				  ,[DisclosureID] as disclosureId
				  ,[PercentHighAdditive] as percentHighAdditive
				  ,[IngredientHFJobPercent] as materialAmountPercent
				  ,UPPER([MatchingStrategy]) as materialMatchingStrategy
				  ,[PercentofConfidence] as materialMatchingPercentConfidence
				  ,h.DateDrillingCommenced as drillingCommencedDate
				  ,h.DateDrillingCompleted as drillingCompletedDate
				  ,h.DrillingContractor as drillingContractor
				  ,h.PipelineConnection as pipelineConnection
				  ,CONVERT(varchar(max),null) as  remarks
				  ,Case when [FFVersion]=1 then 'FF 1'
					when [FFVersion]=2 then 'FF 2'
					when [FFVersion]=3 then 'FF 3'
					else 'FF' end as sourceColumn
			  FROM [esp_data].[dbo].[cmpASvFactJobDetailDenormalized] ff
			  INNER JOIN [esp_data].[dbo].[cmpHeader] h on ff.API=h.API10 --and h.FirstProducedDate is not null
		  )
		  
		SELECT *
		INTO #additiveSummaryPull
		FROM additiveSummaryPull
	raiserror('#additiveSummaryPull created', 10,1) with nowait;
	  
	if object_id('tempdb..#dataWarehousePull') is not null drop table #dataWarehousePull;
		With dataWarehousePull as (
		  SELECT DISTINCT 
			  [cmpFracDetailId] as fracDetailId
			  ,h.[API10] as api10
			  ,CONVERT(varchar(14),null) as api14
			  ,h.[CompletionID] as completionId
			  ,h.[CompletionSpudDate] as spudDate
			  ,h.FirstProducedDate as firstProducedDate
			  ,h.[CompletionDate] as completionDate
			  ,UPPER(h.PurposeOfFiling) as filingPurpose
			  ,UPPER(h.TypeOfCompletion) as completionType
			  ,[BeginDateTime] as jobStartDate
			  ,[EndDateTime] as jobEndDate
			  ,[DepthIntervalFrom] as depthIntervalFrom
			  ,[DepthIntervalTo] as depthIntervalTo
			  ,case when [isSummary]='1' then 'YES'
				when [isSummary]='0' then 'NO'
				else UPPER(isSummary) end as isSummary 
			  ,case when [StageNumber]='-1' then null
				else [StageNumber] end as stageNumber	
			  ,case when numberOfStages is null AND [StageNumber]<>'-1' AND [StageNumber] is not null then max(StageNumber) over (partition by fd.API10)
			  else numberOfStages end as numberOfStages
			  ,h.TotalDepthMD as measuredDepth
			  ,h.TotalDepthTVD as trueVerticalDepth
			  ,[stimulatedFormation] as formation
			  ,null as ingredientId
			  ,CONVERT(nvarchar(100),null) as CASNumber
			  ,[materialDescription] as materialName
			  ,materialAmount
			  ,Case when materialUnit='NULL' then null
				when materialUnit='UNKNOWN' then null
				when materialUnit='' then null
				else materialUnit end as materialUnit
			  ,UPPER(materialType) as materialType
			  ,null as purposeId
			  ,CONVERT(nvarchar(max),null) as purposeDescription
			  ,null as supplierId
			  ,Case when materialType='PROPPANT' AND (
					materialDescription LIKE '%PRIME PLUS%'  OR materialDescription LIKE '%PRIMELUS%' OR materialDescription LIKE '%OILPLUS%' OR materialDescription LIKE '% PP %' 
					OR materialDescription LIKE '%SB Prime%' OR materialDescription LIKE '%AquaBond%' OR materialDescription LIKE '%Black Pro%' OR materialDescription LIKE '%MicroBond%'
					OR materialDescription LIKE '%SiberProp%' OR materialDescription LIKE '%Black Ultra%' OR materialDescription LIKE '%PR6000%' OR materialDescription LIKE '%PR%000%'
					OR materialDescription LIKE '%SB EXCEL%' OR materialDescription LIKE '%SB$EXCEL%' OR materialDescription LIKE '%CERAMAX%'
					OR materialDescription LIKE '%HEXION%'
				) then 'HEXION'
				when materialType='PROPPANT' AND (
					materialDescription LIKE '%Propel SSP%' OR materialDescription LIKE '%SuperLC%' 
					OR materialDescription LIKE '%SLC%' OR materialDescription LIKE '%OptiProp G2%' OR materialDescription LIKE '%CoolSet%' OR materialDescription LIKE '%SuperDC%' OR materialDescription LIKE '%SDC%'
					OR materialDescription LIKE '%PowerProp%' OR materialDescription LIKE '%THS%' OR materialDescription LIKE '%TLC%'
					OR materialDescription LIKE '%FAIRMONT%SANTROL%'
				) then 'FAIRMOUNT SANTROL'
				when materialType='PROPPANT' AND (
					materialDescription LIKE '%ATLAS%' OR materialDescription LIKE '%BADGER%'
					OR materialDescription LIKE '%CRC-E%'  OR materialDescription LIKE '%CRC-C%'  OR materialDescription LIKE '%CRC-LT%'  OR materialDescription LIKE '%PRC-P%'
				) then 'BADGER'
				when materialType='PROPPANT' AND (
					materialDescription LIKE '%Garnet%' OR materialDescription LIKE '%PEARL%' OR materialDescription LIKE '%RUBY%' 
					OR materialDescription LIKE '%ICE%' OR materialDescription LIKE '%CHROME%' OR materialDescription LIKE '%HEAT%'
					OR materialDescription LIKE '%HEAT%' OR materialDescription LIKE '%FLOPRO%PPT%' OR materialDescription LIKE '%DUSTPRO%'
					OR materialDescription LIKE '%PREFERRED%'
				) then 'PREFERRED SANDS'
				when materialType='PROPPANT' AND (
					materialDescription LIKE '%PROPSTAR%' OR materialDescription LIKE '%UNIMIN%'
				) then 'UNIMIN'
				when materialType='PROPPANT' AND (
					materialDescription LIKE '%INNOPROP%' OR materialDescription LIKE '%PYTHON%' OR materialDescription LIKE '%US SILICA%'
				) then 'US SILICA'
				when materialType='PROPPANT' AND (
					materialDescription LIKE '%CARBO%' OR materialDescription LIKE '%KRYPTOSPHERE%' OR materialDescription LIKE '%ECONOPROP%' OR materialDescription LIKE '%HYDROPROP%'
				) then 'CARBO'
				when materialType='PROPPANT' AND (
					materialDescription LIKE '%ST%GOBAIN%' OR materialDescription LIKE '%SAINT%GOBAIN%' OR materialDescription LIKE '%BauxLite%' OR materialDescription LIKE '%VersaLite%' OR materialDescription LIKE '%InterProp%' OR materialDescription LIKE '%VersaProp%'
					OR materialDescription LIKE '%UltraProp%' OR materialDescription LIKE '%Titan%' OR materialDescription LIKE '%SINTERED%Bauxite%' 
				) then 'SAINT GOBAIN'
				when materialType='PROPPANT' AND (
					materialDescription LIKE '%COORSTEK%' OR materialDescription LIKE '%CERAPROP%'
				) then 'COORSTEK'
				when contractorName is not null then UPPER(contractorName)
				when materialType='PROPPANT' then 'UNKNOWN'
				else null end as supplierName
			  ,Case when materialType='PROPPANT' AND 
					(materialDescription LIKE '%RESIN%' OR materialDescription LIKE '%Formaldehyde%' OR materialDescription LIKE '%SB EXCEL%' OR materialDescription LIKE '%PRIME PLUS%' OR materialDescription LIKE '%PRIMELUS%' OR materialDescription LIKE '%OILPLUS%'
					OR materialDescription LIKE '%RCP%' OR materialDescription LIKE '%RCS%' OR materialDescription LIKE '%CRCS%' OR materialDescription LIKE '%PRCS%' OR materialDescription LIKE '%CRS%' OR materialDescription LIKE '%CRC%'  OR materialDescription LIKE '% RC%' 
					OR materialDescription LIKE '% PP %' OR materialDescription LIKE '%SB Prime%' OR materialDescription LIKE '%AquaBond%' OR materialDescription LIKE '%Black Pro%' OR materialDescription LIKE '%MicroBond%'
					OR materialDescription LIKE '%SiberProp%' OR materialDescription LIKE '%Black Ultra%' OR materialDescription LIKE '%PR6000%' OR materialDescription LIKE '%PR%000%' OR materialDescription LIKE '%Propel SSP%' OR materialDescription LIKE '%SuperLC%' 
					OR materialDescription LIKE '%SLC%' OR materialDescription LIKE '%OptiProp G2%' OR materialDescription LIKE '%CoolSet%' OR materialDescription LIKE '%SuperDC%' OR materialDescription LIKE '%SDC%'
					OR materialDescription LIKE '%PowerProp%' OR materialDescription LIKE '%THS%' OR materialDescription LIKE '%TLC%' OR materialDescription LIKE '%Garnet%' OR materialDescription LIKE '%PRC%'
					OR materialDescription LIKE '%PEARL%' OR materialDescription LIKE '%RUBY%' OR materialDescription LIKE '%ICE%' OR materialDescription LIKE '%CHROME%' OR materialDescription LIKE '%HEAT%'
					OR materialDescription LIKE '%HEAT%' OR materialDescription LIKE '%FLOPRO%PPT%' OR materialDescription LIKE '%DUSTPRO%' OR materialDescription LIKE '%PROPSTAR%' OR materialDescription LIKE '%PYTHON%PR%'
					OR materialDescription LIKE '%PYTHON%CR%' OR materialDescription LIKE '% CR%' OR materialDescription LIKE '%PYTHON PR%' OR materialDescription LIKE '%PYTHON LC%' OR materialDescription LIKE '% DC%'  OR materialDescription LIKE '%NORCOTE%'
					OR materialDescription LIKE '%OPTIPROP%' OR materialDescription LIKE '%MAGNAPROP%' OR materialDescription LIKE '%DYNAPROP%' OR materialDescription LIKE '%HYPERPROP%'
					) then 'RESIN COATED'
				when materialType='PROPPANT' AND (
					materialDescription LIKE '%CERAMIC%' OR materialDescription LIKE '%CERAMAX%' OR materialDescription LIKE '%CORBOBOND LITE%' OR materialDescription LIKE '%CERAPROP%'
					OR materialDescription LIKE '%Kryptosphere%' OR materialDescription LIKE '%Econoprop%' OR materialDescription LIKE '%Econo%prop%' OR materialDescription LIKE '%Hydroprop%' OR materialDescription LIKE '%Hydro%prop%' OR materialDescription LIKE '%Carbolite%' OR materialDescription LIKE '%CarboProp%'
					OR materialDescription LIKE '%BauxLite%' OR materialDescription LIKE '%VersaLite%' OR materialDescription LIKE '%InterProp%' OR materialDescription LIKE '%VersaProp%'
					OR materialDescription LIKE '%Bauxite%' OR materialDescription LIKE '%UltraProp%' OR materialDescription LIKE '%Titan%' OR materialDescription LIKE '%Corundum%'
					OR materialDescription LIKE '%Aluminum Oxide%' OR materialDescription LIKE '%Mullite%' OR materialDescription LIKE '%Alumuminum Silicate%'
					OR materialDescription LIKE '%VALUEPROP%' OR materialDescription LIKE '%NAPLITE%'  OR materialDescription LIKE '%BOROPROP%'  OR materialDescription LIKE '%FOROPROP%'  OR materialDescription LIKE '%SINTERBALL%'
				) then 'ARTIFICIAL'
				when materialType='PROPPANT' AND (
					materialDescription LIKE '%SAND%' OR materialDescription LIKE '%Northern White%' OR materialDescription LIKE '%Ottawa%' OR materialDescription LIKE '%Ott%wa%' OR materialDescription LIKE '%WHITE%'  OR materialDescription LIKE '%JORDAN%'
					OR materialDescription LIKE '%HICKORY%' OR materialDescription LIKE '%BRADY%' OR materialDescription LIKE '%QUARTZ%' OR materialDescription LIKE '%SILICA%' OR materialDescription LIKE '% SD%' OR materialDescription LIKE 'SD' 
					OR materialDescription LIKE '%SBEXCEL%' OR materialDescription LIKE '%AC%BLACK%' OR materialDescription LIKE '%ARIZONA%' OR materialDescription LIKE '% AZ%'  OR materialDescription LIKE '% SAN'  OR materialDescription LIKE 'SND' OR materialDescription LIKE 'SD.' OR materialDescription LIKE '%SND' 
					OR materialDescription LIKE '%TEXAS%' OR materialDescription LIKE '%DANIELS%' 
				) then 'SAND'
				when materialType='PROPPANT' then 'UNKNOWN'
				else null end as proppantType
			  ,Case when materialType='PROPPANT' AND materialDescription LIKE '%012%' then '012'
				when materialType='PROPPANT' AND materialDescription LIKE '%018%' then '018'
				when materialType='PROPPANT' AND materialDescription LIKE '%02/30%' then '02/30'
				when materialType='PROPPANT' AND materialDescription LIKE '%100%' then '100'
				when materialType='PROPPANT' AND materialDescription LIKE '%10/20%' OR materialDescription LIKE '%10-20%' then '10/20'
				when materialType='PROPPANT' AND materialDescription LIKE '%10/30%' OR materialDescription LIKE '%10-30%' then '10/30'
				when materialType='PROPPANT' AND materialDescription LIKE '%10/40%' OR materialDescription LIKE '%10-40%' OR materialDescription ='10/40'  OR materialDescription ='10/40%'then '10/40'
				when materialType='PROPPANT' AND materialDescription LIKE '%10/70%' OR materialDescription LIKE '%10-70%' then '10/70'
				when materialType='PROPPANT' AND materialDescription LIKE '%12/18%' OR materialDescription LIKE '%12-18%' OR materialDescription LIKE '%12.18%' then '12/18'
				when materialType='PROPPANT' AND materialDescription LIKE '%12/19%' OR materialDescription LIKE '%12-19%' OR materialDescription LIKE '%12.19%' then '12/19'
				when materialType='PROPPANT' AND materialDescription LIKE '%12/20%' OR materialDescription LIKE '%12-20%' OR materialDescription LIKE '%12.20%' then '12/20'
				when materialType='PROPPANT' AND materialDescription LIKE '%12/30%' OR materialDescription LIKE '%12-30%' OR materialDescription LIKE '%12.30%' then '12/30'
				when materialType='PROPPANT' AND materialDescription LIKE '%12/40%' then '12/40'
				when materialType='PROPPANT' AND materialDescription LIKE '%13/30%' OR materialDescription LIKE '%13-30%' OR materialDescription LIKE '%13.30%' then '13/30'
				when materialType='PROPPANT' AND materialDescription LIKE '%13/40%' OR materialDescription LIKE '%13-40%' OR materialDescription LIKE '%13.40%' then '13/40'
				when materialType='PROPPANT' AND materialDescription LIKE '%13/60%' OR materialDescription LIKE '%13-60%' OR materialDescription LIKE '%13.60%' then '13/60'
				when materialType='PROPPANT' AND materialDescription LIKE '%14/10%' then '14/10'
				when materialType='PROPPANT' AND materialDescription LIKE '%14/30%'OR materialDescription LIKE '%14-30%'  then '14/30'
				when materialType='PROPPANT' AND materialDescription LIKE '%14/40%' then '14/40'
				when materialType='PROPPANT' AND materialDescription LIKE '%16/20%' then '16/20'
				when materialType='PROPPANT' AND materialDescription LIKE '%15/30%' then '15/30'
				when materialType='PROPPANT' AND materialDescription LIKE '%16/30%' OR materialDescription LIKE '%16-30%' OR materialDescription LIKE '%16 30%' OR materialDescription LIKE '%16*30%' then '16/30'
				when materialType='PROPPANT' AND materialDescription LIKE '%16/40%' OR materialDescription LIKE '%16-40%' OR materialDescription LIKE '%16.40%' then '16/40'
				when materialType='PROPPANT' AND materialDescription LIKE '%18/40%' then '18/40'
				when materialType='PROPPANT' AND materialDescription LIKE '%20/27%' then '20/27'
				when materialType='PROPPANT' AND (materialDescription LIKE '%20/40%' OR materialDescription LIKE '%20-40%' OR materialDescription LIKE '%02/40%' OR materialDescription LIKE '%SBEXCEL%') then '20/40'
				when materialType='PROPPANT' AND materialDescription LIKE '%20/41%' then '20/41'
				when materialType='PROPPANT' AND materialDescription LIKE '%20/50%' then '20/50'
				when materialType='PROPPANT' AND materialDescription LIKE '%20/70%' then '20/70'
				when materialType='PROPPANT' AND materialDescription LIKE '%25/81%' then '25/81'
				when materialType='PROPPANT' AND materialDescription LIKE '%30/40%' then '30/40'
				when materialType='PROPPANT' AND materialDescription LIKE '%30/50%' OR materialDescription LIKE '%30-50%' OR materialDescription LIKE '%03/50%' then '30/50'
				when materialType='PROPPANT' AND materialDescription LIKE '%30/60%' then '30/60'
				when materialType='PROPPANT' AND materialDescription LIKE '%30/70%' then '30/70'
				when materialType='PROPPANT' AND materialDescription LIKE '%30/80%' then '30/80'
				when materialType='PROPPANT' AND materialDescription LIKE '%4 / 1%' then '4/1'
				when materialType='PROPPANT' AND materialDescription LIKE '%40/12%' then '40/120'
				when materialType='PROPPANT' AND materialDescription LIKE '%40/40%' then '40/40'
				when materialType='PROPPANT' AND materialDescription LIKE '%40/50%' then '40/50'
				when materialType='PROPPANT' AND materialDescription LIKE '%40/7 %' then '40/70'
				when materialType='PROPPANT' AND (materialDescription LIKE '%40/70%' OR materialDescription LIKE '%04/70%' OR materialDescription LIKE '%PRIME PLUS%') then '40/70'
				when materialType='PROPPANT' AND materialDescription LIKE '%40/71%' then '40/71'
				when materialType='PROPPANT' AND materialDescription LIKE '%40/80%' then '40/80'
				when materialType='PROPPANT' AND materialDescription LIKE '%50/12%' then '50/120'
				when materialType='PROPPANT' AND materialDescription LIKE '%58/2.%' then '58/20'
				when materialType='PROPPANT' AND materialDescription LIKE '%60/12%' then '60/12'
				when materialType='PROPPANT' AND materialDescription LIKE '%621%' then '621'
				when materialType='PROPPANT' AND materialDescription LIKE '%70/14%' then '70/14'
				when materialType='PROPPANT' AND materialDescription LIKE '%7/8%' then '7/8'
				when materialType='PROPPANT' AND materialDescription LIKE '%8/12%' then '8/12'
				when materialType='PROPPANT' AND materialDescription LIKE '%80/10%' then '80/10'
				when materialType='PROPPANT' then 'UNKNOWN'
				else null end as proppantMeshSize
			  ,CONVERT(varchar(20),null) as sandQuality
			  ,null as tradeId
			  ,CONVERT(nvarchar(250),null) as tradeName
			  ,CONVERT(varchar(42),null) as materialDisclosureStatus
			  ,null as disclosureId
			  ,null as percentHighAdditive
			  ,null as materialAmountPercent
			  ,CONVERT(varchar(21),null) as materialMatchingStrategy
			  ,null as materialMatchingPercentConfidence
			  ,h.[DateDrillingCommenced] as drillingCommencedDate
			  ,h.[DateDrillingCompleted] as drillingCompletedDate
			  ,h.[DrillingContractor] as drillingContractor
			  ,h.[PipelineConnection] as pipelineConnection
			  ,h.Remarks as  remarks
			  ,'STATE ANGENCY' as sourceColumn
		  FROM [esp_data].[dbo].[cmpFracDetail_New] fd
		  LEFT JOIN [esp_data].[dbo].[cmpHeader] h on h.CompletionID=fd.CompletionID
		  )
		  
		SELECT *
		INTO #dataWarehousePull
		FROM dataWarehousePull
	raiserror('#dataWarehousePull created', 10,1) with nowait;
	  
	if object_id('tempdb..#unionTable') is not null drop table #unionTable;
		With unionTable as(
		  SELECT * FROM #additiveSummaryPull
		  UNION ALL 
		  SELECT * FROM #dataWarehousePull
		  )
		  
		SELECT * INTO #unionTable
		FROM unionTable
		order by api10
	raiserror('#unionTable created', 10,1) with nowait;

	if object_id('tempdb..#materialStuff') is not null drop table #materialStuff;
	with materialStuff as(
		SELECT DISTINCT u1.api10, u1.completionId,   
				STUFF((  
				SELECT DISTINCT ISNULL(', ' + u2.materialType,'') + ISNULL(', ' + u2.materialName,'')-- + ISNULL(', ' + u2.tradeName,'')
				FROM #unionTable u2  
				WHERE u1.api10 = u2.api10 AND u1.completionId=u2.completionId
				AND materialType NOT IN ('Antifreeze','Anti-Sludge Additive','Biocide','Clay Control','Corrosion Inhibitor','Defoamer','Flow Enhancer','Flow-Back Additive','Fluid Diverter','Fluid Loss Additive','IRON CONTROL','Parafin Inhibitor','Resin Activator','Scale Inhibitor')  
				FOR XML PATH ('') 
				),1,1,'') as materialStuff
				FROM #unionTable u1  
		GROUP BY u1.api10, u1.completionId
	)

	SELECT * INTO #materialStuff FROM materialStuff
	raiserror('#materialStuff created', 10,1) with nowait;

	if object_id('tempdb..#treatmentType') is not null drop table #treatmentType;
		with treatmentTypeLogic as (
			SELECT DISTINCT u.api10, u.completionId
				,case when materialStuff LIKE '%FRICTION REDUCER%' AND (materialStuff LIKE '%GELLING AGENT%'  OR materialStuff LIKE '% GEL%'  OR materialStuff LIKE '%GELLED%' OR materialStuff LIKE '%MGEL%'  OR materialStuff LIKE '%GELW%') AND (materialStuff LIKE '%CROSSLINKER%' OR materialStuff LIKE '%X-LINK%'   OR materialStuff LIKE '%X LINK%') AND materialStuff LIKE '%ACID%' then 'HYBRID'
					when materialStuff NOT LIKE '%BASE CARRIER, WATER%' AND (materialStuff LIKE '%PROPANE %' OR materialStuff LIKE '%PROPANE, %' OR materialStuff LIKE '%GAS FRAC%') then 'GAS FRAC'
					when ((materialStuff LIKE '%BASE CARRIER, ACID%' OR (materialStuff LIKE '%BASE CARRIER, 15%ACID%')) OR (materialStuff LIKE '%BASE CARRIER, GEL%ACID%') OR (materialStuff LIKE '%ACID FRAC%')) AND NOT (materialStuff LIKE 'PROPPANT') then 'ACID FRAC'
					when (materialStuff LIKE '%BASE CARRIER, NITROGEN%' OR (materialStuff LIKE '%BASE CARRIER, CO2%') OR (materialStuff LIKE '%BASE CARRIER, CARBON%')) OR (materialStuff LIKE '%FOAM%' AND(materialStuff LIKE '%CO2%' OR materialStuff LIKE '%CARBON%DIOXIDE' OR materialStuff LIKE '%NITROGEN%')) OR (materialStuff LIKE '%ENERGIZED%' OR materialStuff LIKE '%ENERGIZER%') then 'ENERGIZED'
					when (materialStuff LIKE '%GELLING AGENT%' AND (materialStuff LIKE '%CROSSLINKER%')) AND NOT (materialStuff LIKE '%FRICTION REDUCER%') then 'CONVENTIONAL'
					when (materialStuff LIKE '%BASE CARRIER, WATER%' AND materialStuff LIKE '%FRICTION REDUCER%' AND materialStuff NOT LIKE '%CROSSLINKER%') OR (materialStuff LIKE '%SLICKWATER%')then 'WATER FRAC'
					else 'UNKNOWN' end as treatmentType
			FROM #unionTable u
			LEFT JOIN #materialStuff m ON u.api10 = m.api10 AND u.completionId=m.completionId
		)

		SELECT * INTO #treatmentType FROM treatmentTypeLogic
	raiserror('#treatmentType created', 10,1) with nowait;

	if object_id('tempdb..#espHeaderPull') is not null drop table #espHeaderPull;
		With espHeaderPull as (
			SELECT api,play
			,upperPerforation,lowerPerforation,tvd,stages,reservoirAlias
			,Operator,firstProductionDate,ROW_NUMBER() OVER (PARTITION BY api ORDER BY firstProductionDate DESC) as dateRank
			,MIN(h.firstProductionDate) OVER (PARTITION BY api) as minDate
			FROM [esp_live].[dbo].[espHeader] h
			WHERE api>'0'
		)

		SELECT * INTO #espHeaderPull FROM espHeaderPull
	raiserror('#espHeaderPull created', 10,1) with nowait;

	if object_id('esp_stage.dbo.espFracDetail') is not null drop table esp_stage.dbo.espFracDetail;
		With espFracDetail as (
			SELECT 
			u.api10
			,h.play--, h.firstProductionDate, u.firstProducedDate as uProducedDate, DATEDIFF(MM,h.firstProductionDate,u.firstProducedDate) as diff, h.minDate
			,u.completionId
			,u.fracDetailId
			,h.operator as currentOperator
			,u.spudDate
			,u.completionDate
			,case when DATEDIFF(MM,h.minDate,u.firstProducedDate)>=-3 AND DATEDIFF(MM,h.minDate,u.firstProducedDate)<=0 then u.firstProducedDate
				else h.minDate end as [firstProducedDate]
			,u.filingPurpose
			,u.completionType
			,u.jobStartDate
			,u.jobEndDate
			,ISNULL(u.depthIntervalFrom,h.upperPerforation) as depthIntervalFrom
			,ISNULL(u.depthIntervalTo,h.lowerPerforation) as depthIntervalTo
			,u.isSummary
			,u.stageNumber
			,ISNULL(u.numberOfStages,h.stages) as numberOfStages
			,u.measuredDepth
			,ISNULL(u.trueVerticalDepth,h.tvd) as trueVerticalDepth
			,ISNULL(u.formation,h.reservoirAlias) as formation
			,u.ingredientId
			,u.CASNumber
			,u.materialName
			,u.materialAmount
			,u.materialUnit
			,u.materialType
			,u.purposeId
			,u.purposeDescription
			,u.supplierId
			,Case when [SupplierName]='LOOKUP OPERATOR NAME IN HEADER' then h.operator
			when [SupplierName]='OIPERATOR' then h.operator
			 when [SupplierName] is null then 'UNKNOWN'
			 when [SupplierName]='N/A'  then 'UNKNOWN'
			else supplierName end as supplierName
			,u.proppantType
			,u.proppantMeshSize
			,u.sandQuality
			,t.treatmentType
			,case when materialName like '%WATER%' and materialUnit='LB' then SUM(materialAmount) OVER (PARTITION BY u.api10 ,u.completionId)/(8.33*42)
					when materialName like '%WATER%' and materialUnit='GAL' then SUM(materialAmount) OVER (PARTITION BY u.api10 ,u.completionId)/42
					when materialName like '%WATER%' and materialUnit='Bbl' then SUM(materialAmount) OVER (PARTITION BY u.api10 ,u.completionId)
				else null end as cumWaterBbl 
			,u.tradeId
			,u.tradeName
			,u.materialDisclosueStatus
			,u.disclosureId
			,u.percentHighAdditive
			,u.materialAmountPercent
			,u.materialMatchingPercentConfidence
			,u.drillingCommencedDate
			,u.drillingCompletedDate
			,u.drillingContractor
			,u.pipelineConnection
			,u.remarks
			,u.sourceColumn
			FROM #unionTable u
			Left JOIN #espHeaderPull h
			ON h.api=u.api10 AND h.dateRank=1
			LEFT JOIN #treatmentType t
			ON u.api10=t.api10 and u.completionId=t.completionId
		)

		SELECT * 
		INTO esp_stage.dbo.espFracDetail  
		FROM espFracDetail
	raiserror('esp_stage.dbo.espFracDetail created', 10,1) with nowait;


END

GO

