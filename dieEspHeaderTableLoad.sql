USE [esp_stage]
GO

/****** Object:  StoredProcedure [dbo].[dieEspHeaderTableLoad]    Script Date: 12/16/2015 8:43:02 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[dieEspHeaderTableLoad] 
	-- Add the parameters for the stored procedure here

AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    -- Insert statements for procedure here
			if object_id('tempdb..#materialStuff') is not null drop table #materialStuff;
				with materialStuff as(
					SELECT DISTINCT f1.api10  
							,STUFF((  
							SELECT DISTINCT ISNULL(', ' + f2.proppantType,'')
							FROM [esp_live].[dbo].[vEspFracDetail] f2  
							WHERE f1.api10 = f2.api10 and materialType='PROPPANT'
							FOR XML PATH ('') 
							),1,2,'') as proppantTypeStuff
							,STUFF((  
							SELECT DISTINCT ISNULL(', ' + f2.treatmentType,'')
							FROM [esp_live].[dbo].[vEspFracDetail] f2  
							WHERE f1.api10 = f2.api10 and materialType<>'PROPPANT'
							FOR XML PATH ('') 
							),1,2,'') as treatmentTypeStuff
							,STUFF((  
							SELECT DISTINCT ISNULL(', ' + f2.proppantMeshSize,'')
							FROM [esp_live].[dbo].[vEspFracDetail] f2  
							WHERE f1.api10 = f2.api10 and materialType='PROPPANT'
							FOR XML PATH ('') 
							),1,2,'') as proppantMeshSizeStuff
							,STUFF((  
							SELECT DISTINCT ISNULL(', ' + f2.materialName,'')
							FROM [esp_live].[dbo].[vEspFracDetail] f2  
							WHERE f1.api10 = f2.api10 and (materialType='BASE CARRIER' OR materialName LIKE '%WATER%' OR materialName LIKE '%SILVERSTIM%' OR materialName LIKE '%FRAC FLUID%')
							FOR XML PATH ('') 
							),1,1,'') as baseCarrierTypeStuff
							,MAX(cumWaterBbl) as cumWaterBbl
					FROM [esp_live].[dbo].[vEspFracDetail] f1  
					GROUP BY f1.api10
				)

				SELECT * INTO #materialStuff FROM materialStuff
			raiserror('#materialStuff created', 10,1) with nowait;

			if object_id('tempdb..#unionTable') is not null drop table #unionTable;
				With unionTable as(
				  SELECT api10 as api FROM [esp_live].[dbo].[vEspFracDetail]
				  UNION ALL 
				  SELECT api FROM [esp_live].[dbo].[vEspWellTest]
				  UNION ALL 
				  SELECT api FROM [esp_live].[dbo].[vEspPiping]
				  )
	  
				SELECT DISTINCT * INTO #unionTable
				FROM unionTable
				order by api
			raiserror('#unionTable created', 10,1) with nowait;


			if object_id('tempdb..#espHeaderPull') is not null drop table #espHeaderPull;
				With espHeaderPull as (
					SELECT DISTINCT 
					 h.api
					,hpdiEntityId
					,UPPER(h.play) as play
					,operator
					,DENSE_RANK() OVER (PARTITION BY h.api ORDER BY h.firstProductionDate DESC,h.completionDate DESC) as dateRank
					--,MIN(h.firstProductionDate) OVER (PARTITION BY h.api) as minDate
					,h.[state]
					,h.county
					,h.field
					,h.reservoir
					,h.reservoirAlias
					,h.primaryProduct
					,h.wellbore
					--,h.wellboreSource
					,h.perfInterval
					,h.perfIntervalSource
					,h.section
					,h.township
					,h.[range]
					,h.quarterQuarter
					,h.abstract
					,h.survey
					,h.upperPerforation
					,h.lowerPerforation
					,h.totalDepth
					,h.tvd
					,h.latitude27
					,h.longitude27
					,h.basin
					,UPPER(h.playArea) as playArea
					,h.spudDate
					,h.completionDate
					,h.firstProductionDate
					,h.lastProductionDate
					,h.maxIpOil
					,h.maxIpGas 
					,h.maxIpOilPending
					,h.maxIpGasPending
					,h.cum3MonthsOil
					,h.cum3MonthsGas
					,h.cum3MonthsWater
					,h.cum6MonthsOil
					,h.cum6MonthsGas
					,h.cum6MonthsWater
					,h.cum12MonthsOil
					,h.cum12MonthsGas
					,h.cum12MonthsWater
					,h.cum24MonthsOil
					,h.cum24MonthsGas
					,h.cum24MonthsWater
					,h.cum60MonthsOil
					,h.cum60MonthsGas
					,h.cum60MonthsWater
					,h.cumTotalOil
					,h.cumTotalGas
					,h.cumTotalWater
					,h.[vnUltimateOil_Bbl] as EUROil
					,h.[vnUltimateGas_Mcf] as EURgas
					--,h.declineOil3Months
					--,h.declineGas3Months
					--,h.declineWater3Months
					--,h.declineOil6Months
					--,h.declineGas6Months
					--,h.declineWater6Months
					--,h.declineOil12Months
					--,h.declineGas12Months
					--,h.declineWater12Months
					--,h.declineOil24Months
					--,h.declineGas24Months
					--,h.declineWater24Months
					--,h.declineOil60Months
					--,h.declineGas60Months
					--,h.declineWater60Months
					,h.azimuth
					,h.lateralLength
					,h.cumulativeGasOilRatio
					--,case when cumTotalWater>0 and cumTotalOil>0 then cumTotalWater/cumTotalOil
					--	else null end as cumTotalWaterOilRatio --newHeader
					,h.firstGasOilRatio
					,h.lastGasOilRatio
					,h.pracIPGasOilRatio
					,h.first3MonthsGasOilRatio
					,h.wellType
					,h.oilGravity
					,h.gasGravity
					,h.lastProductionOil
					,h.lastProductionGas
					,h.lastProductionWater
					,h.allocFlag
					,h.intermediateCasing
					,h.simulFrac
					,h.padDrill
					,h.linerSize
					,h.tubingSize
					,h.stages
					,proppantLbs as totalProppantLbs
					,treatmentBbls as totalTreatmentBbls
					FROM [esp_live].[dbo].[espHeader] h
					WHERE EXISTS (select api from #unionTable u where u.api=h.api)
				)

			SELECT * INTO #espHeaderPull FROM espHeaderPull
			raiserror('#espHeaderPull created', 10,1) with nowait;




			if object_id('esp_stage.dbo.dieEspHeader') is not null drop table esp_stage.dbo.dieEspHeader;




			SELECT DISTINCT 
			h.*
			,cumWaterBbl  as totalWaterBbl--newHeader
			,MIN(p.casingBottom) OVER (PARTITION BY p.api) as surfaceCasingDepth
			,case when proppantTypeStuff is not null AND totalProppantLbs is not null  then proppantTypeStuff 
				when totalProppantLbs is not null then 'UNKNOWN'
				else null end as proppantType --newHeader
			,case when proppantMeshSizeStuff is not null AND totalProppantLbs is not null  then proppantMeshSizeStuff 
				when totalProppantLbs is not null then 'UNKNOWN'
				else null end as proppantMeshSize
			,case when treatmentTypeStuff is not null AND totalTreatmentBbls is not null then treatmentTypeStuff 
				when totalTreatmentBbls is not null then 'UNKNOWN'
				else null end as treatmentType --newHeader
			,w.[testFormation]
			,w.[24HourGas]
			,w.[24HourOil]
			,w.[24HourWater]
			,w.[24HourGasOilRatio]
			,w.[24HourWaterOilRatio]
			,w.[oilGravity] as testOilGravity
			,w.[casingPressure]
			,w.[flowingTubePressue]
			,w.[producingMethod]
			,w.[pumpType]
			,w.[bottomHoleTemp]
			,w.[bottomHoleTempDepth]
			,w.[chokeSize] 
			,w.[dryGasGravity] as testDryGasGravity

			Into esp_stage.dbo.dieEspHeader
			FROM #espHeaderPull h
			LEFT JOIN #materialStuff f ON f.api10=h.api
			LEFT JOIN [esp_live].[dbo].[vEspPiping] p ON p.API=h.api and casingType='SURFACE'
			LEFT JOIN [esp_live].[dbo].[vEspWellTest] w ON w.api=h.api and h.dateRank=1 and w.dateRank=1 and ((DATEADD(MM,1,w.completionDate)>=h.completionDate AND DATEADD(MM,-1,w.completionDate)<=h.completionDate) OR (DATEADD(MM,1,w.completionDate)>=h.firstProductionDate AND DATEADD(MM,-1,w.completionDate)<=h.firstProductionDate))

END

GO

