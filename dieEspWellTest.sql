USE [esp_stage]
GO

/****** Object:  StoredProcedure [dbo].[dieEspWellTest]    Script Date: 12/16/2015 8:43:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		Laxmi	
-- Create date: 12.10.2015
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[dieEspWellTest]
	-- Add the parameters for the stored procedure here

AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

if object_id('tempdb..#espHeaderPull') is not null drop table #espHeaderPull;
	With espHeaderPull as (
		SELECT api,play
		,latitude27
		,longitude27
		,upperPerforation,lowerPerforation,tvd,stages,reservoirAlias
		,Operator
		,firstProductionDate,ROW_NUMBER() OVER (PARTITION BY api ORDER BY firstProductionDate DESC) as dateRank
		,MIN(h.firstProductionDate) OVER (PARTITION BY api) as minDate
		FROM [esp_live].[dbo].[espHeader] h
		WHERE api>'0'
	)

	SELECT * INTO #espHeaderPull FROM espHeaderPull
raiserror('#espHeaderPull created', 10,1) with nowait;

		-- initial Potential test data for all the states, I have to seperate the query for OK state to get the formation into data set.

			if object_id('esp_stage.[dbo].[espInitialProductionTest]') is not null drop table esp_stage.[dbo].[espInitialProductionTest];


			Select * Into esp_stage.[dbo].[espInitialProductionTest]
			From (
			Select distinct
					ch.API10 as [API] ,ch.[CompletionID],ch.PurposeOfFiling,ch.state_name as [State] ,ch.CompletionDate as [CompletionDate],
					ip.testDate [TestDate],ip.Duration [Duration],ip.OilGravity,ip.TestOil,ip.TestGas,ip.TestWater,--ip.[24HourOil],ip.[24HourGas],ip.[24HourWater],
					case
						when (ch.state_name='Louisiana') and ip.[24HourOil] is null then ip.TestOil
						when (ch.state_name='Montana' OR ch.state_name='Texas') and Duration LIKE '24%'and ip.[24HourOil] is null then ip.TestOil
						when ip.[24HourOil] IS null and ip.[24HourGas] IS not null then 0
						else ip.[24HourOil] end as [24HourOil],
					case
						when (ch.state_name='Louisiana') and ip.[24HourGas] is null then ip.TestGas
						when (ch.state_name='Montana' OR ch.state_name='Texas') and Duration LIKE '24%'and ip.[24HourGas] is null then ip.TestGas
						when ip.[24HourOil] IS not null and ip.[24HourGas] IS null then 0
						else ip.[24HourGas] end as [24HourGas],
					case
						when (ch.state_name='Louisiana') and ip.[24HourWater] is null then ip.TestWater
						when (ch.state_name='Montana' OR ch.state_name='Texas') and Duration LIKE '24%'and ip.[24HourWater] is null then ip.TestWater
						when ip.[24HourWater] IS null and ip.[24HourOil] IS not null and ip.[24HourGas] IS not null then 0
						else ip.[24HourWater] end as [24HourWater],
					case
						when ip.ChokeSize LIKE '0.0%' then null
						else ChokeSize end as ChokeSize
					, Null as TestFormation,CasingPressure,TestFlowingTubePressure
					,Case when ProducingMethod='unknown' then null else UPPER(ProducingMethod) end as [ProducingMethod]
					,Case when SizeAndTypeOfPump='unknown' then null else UPPER(SizeAndTypeOfPump) end as [SizeAndTypeOfPump]
			From  [esp_data].[dbo].[cmpHeader] ch
			Join [esp_data].[dbo].[cmpInitialPotentialTest] ip   ON ip.completion_dr_id=ch.CompletionID
			Where state_name in  ('LOUISIANA','MONTANA','NEW MEXICO','NORTH DAKOTA','TEXAS','WYOMING') and completion_dr_id<>'-1'
			Union ALL

			Select distinct 
					ch.API10 as [API] ,ch.[CompletionID],ch.PurposeOfFiling,ch.state_name as [State] ,ch.CompletionDate as [CompletionDate],
					ip.testDate [TestDate],ip.Duration [Duration],ip.OilGravity,ip.TestOil,ip.TestGas,ip.TestWater,ip.[24HourOil],ip.[24HourGas],ip.[24HourWater],
					ip.ChokeSize,pt.TestFormation,ip.CasingPressure,ip.TestFlowingTubePressure
					,Case when ip.ProducingMethod='unknown' then null else UPPER(ip.ProducingMethod) end as [ProducingMethod]
					,Case when ip.SizeAndTypeOfPump='unknown' then null else UPPER(ip.SizeAndTypeOfPump) end as [SizeAndTypeOfPump]
			From   [esp_data].[dbo].[cmpHeader] ch
			Join [esp_data].[dbo].[cmpInitialPotentialTest] ip ON ip.completion_dr_id=ch.CompletionID
			Join [esp_data].[dbo].[cmpOKProductionTest] pt ON ip.completion_dr_id= pt.completionID
				and ISNULL(pt.[24HourGas],0)=ISNULL(ip.[24HourGas],0) and ISNULL(pt.[24HourOil],0)=ISNULL(ip.[24HourOil],0) and ISNULL(pt.[24HourWater],0)=ISNULL(ip.[24HourWater] ,0)
				and ISNULL(pt.ChokeSize,0)=ISNULL(ip.ChokeSize,0)  
				and ISNULL(pt.OilGravity,0)=ISNULL(ip.OilGravity,0)
				and ISNULL(pt.CasingPressure,0)=ISNULL(ip.CasingPressure,0) and ISNULL(pt.TestFlowingTubePressure,0)=ISNULL(ip.TestFlowingTubePressure,0)
				and pt.TestDate=ip.testDate
			Where state_name in  ('Oklahoma') 

			)a


	-- GM & FT Child table load with cartisian product , It Consist of data only with RunNumber =1 from both GasMEasuremnt and FieldTest Tables

			if object_id('tempdb..#GM_FT_Child') is not null drop table #GM_FT_Child;

			Select  ch.API10 as [API] ,ch.CompletionID,ch.FormType,ch.CompletionDate,ch.PurposeOfFiling,ft.TimeOfRun,gm.volumePerDay,ft.BottomHoleTemp,ft.BottomHoleTempDepth,CAST(ft.RunChokeSize as varchar(34)) as ChokeSize,ft.GravityDryGas,ft.GravityLiquidHydrocarbon,
					gm.RunNumber as GM_RunNumber,ft.RunNumber as FT_RunNumber
			Into #GM_FT_Child
			From [esp_data].[dbo].[cmpHeader] ch
			Join [esp_data].[dbo].[cmpGasMeasurementTest] gm on ch.CompletionID=gm.CompletionID 
			JOin [esp_data].[dbo].[cmpFieldTest] ft on ch.CompletionID=ft.CompletionID
			Where ch.state_name='Texas' and ch.CompletionID <>'-1' and gm.RunNumber=1 and ft.RunNumber=1
			

	-- API exists in GasMeasurement but not in Field Test Table

			if object_id('tempdb..#GMT_APIs') is not null drop table #GMT_APIs;

			Select ch.API10 as API , ch.CompletionID, ch.FormType,  ch.CompletionDate, ch.PurposeOfFiling
			Into #GMT_APIs
			From
			(
			Select API,CompletionID
			From [esp_data].[dbo].[cmpGasMeasurementTest]
			Except
			Select API,CompletionID
			From [esp_data].[dbo].[cmpFieldTest]
			)a
			Inner join [esp_data].[dbo].[cmpHeader] ch on a.API=ch.API10 and a.CompletionID =ch.CompletionID


  -- API exists in FieldTest Table

			if object_id('tempdb..#FT_APIs') is not null drop table #FT_APIs;

			Select ch.API10 as API , ch.CompletionID, ch.CompletionDate, ch.PurposeOfFiling, ch.FormType
			Into #FT_APIs
			From
			(
			Select API,CompletionID
			From [esp_data].[dbo].[cmpFieldTest]
			Except
			Select API,CompletionID
			From [esp_data].[dbo].[cmpGasMeasurementTest]
			)a
			Inner Join [esp_data].[dbo].[cmpHeader] ch on a.API=ch.API10 and a.CompletionID =ch.CompletionID
;
 -- Parent table Load

			if object_id('esp_stage.[dbo].[espWellTest]') is not null drop table esp_stage.[dbo].[espWellTest];
			with a as
			(
					Select x.API,x.CompletionID,x.CompletionDate,x.PurposeOfFiling,x.FormType,x.BottomHoleTemp,x.BottomHoleTempDepth,x.ChokeSize,x.GravityDryGas,x.GravityLiquidHydrocarbon,x.volumePerDay,x.TimeOfRun,x.FT_RunNumber,x.GM_RunNumber					   
					from #GM_FT_Child x
					--Where x.FT_RunNumber=1 and x.GM_RunNumber=1 
			)
			,b as
			(
				Select x.API,x.CompletionID,x.CompletionDate,x.PurposeOfFiling,x.FormType, null as BottomHoleTemp, null as BottomHoleTempDepth, null as ChokeSize, null as GravityDryGas, null as GravityLiquidHydrocarbon,
					   y.volumePerDay, null as TimeOfRun, null as FT_RunNumber, y.RunNumber as GM_RunNumber
				From #GMT_APIs x
				Inner join [esp_data].[dbo].[cmpGasMeasurementTest] y on x.API=y.API and x.CompletionID=y.CompletionID
				Where y.RunNumber=1
			)
			,c as

			(
				Select x.API,x.CompletionID,x.CompletionDate,x.PurposeOfFiling,x.FormType,y.BottomHoleTemp,y.BottomHoleTempDepth,CAST(y.RunChokeSize as varchar(34)) as ChokeSize,y.GravityDryGas,y.GravityLiquidHydrocarbon, null as volumePerDay,y.TimeOfRun,y.RunNumber as FT_RunNumber,
					   null as GM_RunNumber
				From #FT_APIs x
				Inner Join [esp_data].[dbo].[cmpFieldTest] y on x.API=y.API and x.CompletionID=y.CompletionID
				Where y.RunNumber=1
			)
			,d as
			(
			Select * from a
			Union all
			Select * from b
			Union all
			Select * from c
			)
			Select ip.API as api
			,ip.CompletionID as completionId
			,h.play
			,h.latitude27 as latitude
			,h.longitude27 as longitude
			,h.minDate as firstProductionDate
			,h.operator as currentOperator
			,ip.CompletionDate as completionDate
			,ip.PurposeOfFiling as filingPurpose
			,case when ip.TestDate <= '01-01-1951' then null
			else ip.TestDate end as testDate
			,ip.[State] as [state]
			,ip.TestFormation as testFormation
			,ISNULL(ip.[24HourGas],volumePerDay) as [24HourGas]
			,ip.[24HourOil]
			,ip.[24HourWater]
			,case when ip.[24HourGas]>0 and ip.[24HourOil]>0 then ip.[24HourGas]/ip.[24HourOil]
				else null end as [24HourGasOilRatio]
			,case when ip.[24HourWater]>0 and ip.[24HourOil]>0 then ip.[24HourWater]/ip.[24HourOil]
				else null end as [24HourWaterOilRatio]
			,ISNULL(CAST(ip.OilGravity as bigint),CAST(d.GravityLiquidHydrocarbon as bigint)) as oilGravity
			,ip.CasingPressure as casingPressure
			,ip.TestFlowingTubePressure as flowingTubePressue
			,ip.ProducingMethod as producingMethod
			,case when ip.SizeAndTypeOfPump IN ('N/A') then null
			else ip.SizeAndTypeOfPump end as pumpType
			,d.BottomHoleTemp as bottomHoleTemp
			,d.BottomHoleTempDepth as bottomHoleTempDepth
			,ISNULL(ip.ChokeSize,d.ChokeSize) as chokeSize
			,d.GravityDryGas as dryGasGravity
			,d.TimeOfRun as timeOfRun
			,d.FT_RunNumber as runNumberFT
			,d.GM_RunNumber as runNUmberGM
			,DENSE_RANK() OVER (PARTITION BY h.api ORDER BY h.firstProductionDate DESC, ip.completionDate DESC, TestDate DESC) as dateRank
			
			Into esp_stage.[dbo].[espWellTest] --TEST
			From  esp_data.[dbo].[espInitialProductionTest] ip
			left join d on ip.API=d.api and ip.CompletionID=d.CompletionID
			left join #espHeaderPull h on h.api=ip.API AND h.dateRank=1
			WHERE (d.FormType is not null or ip.[24HourGas] is not null or ip.[24HourOil] is not null or ip.[24HourWater] is not null or ip.OilGravity is not null or ip.TestFormation is not null or d.BottomHoleTemp is not null or d.BottomHoleTempDepth is not null or d.ChokeSize is not null or d.GravityDryGas is not null or d.GravityLiquidHydrocarbon is not null or d.TimeOfRun is not null or d.FT_RunNumber is not null or d.GM_RunNumber is not null)
				


END

GO

