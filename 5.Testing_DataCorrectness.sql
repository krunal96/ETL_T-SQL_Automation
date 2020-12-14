-- Data Correctness Test : Check if Source and Destination tables have same amount for given year and given CRA_Code
-- User can dynamically select source file, CRA_Code('5010', '5050' etc.) and Year(2015, 2016, etc.)
-- The approach is to compare aggregation of selected attribute values from Source and Destination. If aggregation is equal then granular data should be equal as well.

ALTER  PROCEDURE Compare_Data_correctness
(
 @year int,
 @CRA_Code nvarchar(50),
 @filepath nvarchar(50)
) 
AS
BEGIN

	IF Object_id('tempdb..#temp', 'U') IS NOT NULL
		DROP TABLE #temp;

	-- Temporary table of the Source Filepath specified.
	CREATE TABLE #temp (
		["bn registration number"] [VARCHAR](50) NULL,
		["fiscal period end"] [DATETIME] NULL,
		["form id"] [VARCHAR](50) NULL,
		["4020"] [VARCHAR](50) NULL,
		["4050"] [VARCHAR](50) NULL,
		["4100"] [VARCHAR](50) NULL,
		["4110"] [VARCHAR](50) NULL,
		["4120"] [VARCHAR](50) NULL,
		["4130"] [VARCHAR](50) NULL,
		["4140"] [VARCHAR](50) NULL,
		["4150"] [VARCHAR](50) NULL,
		["4155"] [VARCHAR](50) NULL,
		["4160"] [VARCHAR](50) NULL,
		["4165"] [VARCHAR](50) NULL,
		["4166"] [VARCHAR](50) NULL,
		["4170"] [VARCHAR](50) NULL,
		["4180"] [VARCHAR](50) NULL,
		["4200"] [VARCHAR](50) NULL,
		["4250"] [VARCHAR](50) NULL,
		["4300"] [VARCHAR](50) NULL,
		["4310"] [VARCHAR](50) NULL,
		["4320"] [VARCHAR](50) NULL,
		["4330"] [VARCHAR](50) NULL,
		["4350"] [VARCHAR](50) NULL,
		["4400"] [VARCHAR](50) NULL,
		["4490"] [VARCHAR](50) NULL,
		["4500"] [VARCHAR](50) NULL,
		["4505"] [VARCHAR](50) NULL,
		["4510"] [VARCHAR](50) NULL,
		["4520"] [VARCHAR](50) NULL,
		["4525"] [VARCHAR](50) NULL,
		["4530"] [VARCHAR](50) NULL,
		["4540"] [VARCHAR](50) NULL,
		["4550"] [VARCHAR](50) NULL,
		["4560"] [VARCHAR](50) NULL,
		["4565"] [VARCHAR](50) NULL,
		["4570"] [VARCHAR](50) NULL,
		["4571"] [VARCHAR](50) NULL,
		["4575"] [VARCHAR](50) NULL,
		["4580"] [VARCHAR](50) NULL,
		["4590"] [VARCHAR](50) NULL,
		["4600"] [VARCHAR](50) NULL,
		["4610"] [VARCHAR](50) NULL,
		["4620"] [VARCHAR](50) NULL,
		["4630"] [VARCHAR](50) NULL,
		["4640"] [VARCHAR](50) NULL,
		["4650"] [VARCHAR](50) NULL,
		["4655"] [VARCHAR](max) NULL,
		["4700"] [VARCHAR](50) NULL,
		["4800"] [VARCHAR](50) NULL,
		["4810"] [VARCHAR](50) NULL,
		["4820"] [VARCHAR](50) NULL,
		["4830"] [VARCHAR](50) NULL,
		["4840"] [VARCHAR](50) NULL,
		["4850"] [VARCHAR](50) NULL,
		["4860"] [VARCHAR](50) NULL,
		["4870"] [VARCHAR](50) NULL,
		["4880"] [VARCHAR](50) NULL,
		["4891"] [VARCHAR](50) NULL,
		["4890"] [VARCHAR](50) NULL,
		["4900"] [VARCHAR](50) NULL,
		["4910"] [VARCHAR](50) NULL,
		["4920"] [VARCHAR](50) NULL,
		["4930"] [VARCHAR](max) NULL,
		["4950"] [VARCHAR](50) NULL,
		["5000"] [VARCHAR](50) NULL,
		["5010"] [VARCHAR](50) NULL,
		["5020"] [VARCHAR](50) NULL,
		["5030"] [VARCHAR](50) NULL,
		["5040"] [VARCHAR](50) NULL,
		["5050"] [VARCHAR](50) NULL,
		["5060"] [VARCHAR](50) NULL,
		["5070"] [VARCHAR](50) NULL,
		["5100"] [VARCHAR](50) NULL,
		["5500"] [VARCHAR](50) NULL,
		["5510"] [VARCHAR](50) NULL,
		["5520"] [VARCHAR](50) NULL,
		["5610"] [VARCHAR](50) NULL,
		["5640"] [VARCHAR](50) NULL,
		["5710"] [VARCHAR](50) NULL,
		["5720"] [VARCHAR](50) NULL,
		["5730"] [VARCHAR](50) NULL,
		["5740"] [VARCHAR](50) NULL,
		["5750"] [VARCHAR](50) NULL,
		["5900"] [VARCHAR](50) NULL,
		["5910"] [VARCHAR](50) NULL
		) ON [PRIMARY] textimage_on [PRIMARY];

	DECLARE @sql NVARCHAR(4000) = 'BULK INSERT #temp FROM ''' + @filepath + ''' WITH ( FIELDTERMINATOR ='','', ROWTERMINATOR =''\n'', FORMAT = ''CSV'', FIELDQUOTE = ''"'',  FIRSTROW = 2)';
	EXEC (@sql);

	-- Source Data processing. This block of code will store Source value for given parameters into @Source_Value variable
    DECLARE @Source_Value money;
    DECLARE @Destination_Value money;
	DECLARE @sql2 nvarchar(Max);	
	DECLARE @sql3 nvarchar(Max);
	DECLARE @QueryResult1 table(CRA_RESULT nvarchar(50));
	DECLARE @YEAR2 int;
	SET @YEAR2 = @year;
	
	DECLARE @Final_Variable nvarchar(50);
	SET @sql2= N'SELECT ' + @CRA_Code + '  FROM #temp'
	INSERT @QueryResult1  EXEC (@sql2);
	SET @Source_Value  = (SELECT SUM(CAST(RIGHT(CRA_RESULT, LEN(CRA_RESULT) - 1) as money)) FROM @QueryResult1);
	

	-- Dynamic value assignment based on passed parameter of CRA_CODE. Since Source table and Destination tables have different names, this comparision block -
	-- will map Code based source attributes to Name based destination attributes.
	IF (@CRA_Code = '["5010"]')
	   SET @Final_Variable = 'EMA';
	ELSE IF (@CRA_Code = '["4700"]')	
		SET @Final_Variable = 'Total_Revenue';
	ELSE IF (@CRA_Code ='["5100"]' )	
		SET @Final_Variable = 'Total_Expenses';
	ELSE IF (@CRA_Code = '["4860"]' )	
		SET @Final_Variable = 'EPCF';
	ELSE IF (@CRA_Code = '["5000"]' )	
		SET @Final_Variable = 'ECA';
	ELSE IF (@CRA_Code = '["5020"]' )	
		SET @Final_Variable = 'EF5020';
	ELSE IF (@CRA_Code = '["5050"]' )	
		SET @Final_Variable = 'Gift';
	ELSE IF (@CRA_Code = '["5040"]' )	
		SET @Final_Variable = 'Other_Expenditure';

    -- Destination Data processing. This block of code will store Source value for given parameters into @Destination_Value variable
    SET @sql3= 'SELECT SUM([' + @Final_Variable + '])  FROM [dbo].[Final_Table] WHERE [Year] = ' + CAST(@YEAR2 AS VARCHAR(10))
	DECLARE @QueryResult2 table(CRA_RESULT2 float);
    INSERT  @QueryResult2  EXEC(@sql3);
	SET @Destination_Value  = (SELECT * FROM @QueryResult2);


	-- Comparing Source and Destination values  and print appropriate message.
	print @year
	print @CRA_Code
	print @Final_Variable
	IF (@Source_Value = @Destination_Value)
		PRINT 'CHECK SUCCESS : ' + CAST(@Source_Value as nvarchar(255)) + ' == ' + CAST(@Destination_Value as nvarchar(255));
	ELSE 
		PRINT 'CHECK FAILED'

END;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Execution of procedure. 
-- Format : "EXEC sp_name year,Attribute,Finaicial_D Filepath"
EXEC Compare_Data_correctness 2015,'["5050"]' ,'c:\users\kruna\onedrive\desktop\ch\data\2015\1.csv';
EXEC Compare_Data_correctness 2016,'["5010"]' ,'c:\users\kruna\onedrive\desktop\ch\data\2016\1.csv';
EXEC Compare_Data_correctness 2017,'["5020"]' ,'c:\users\kruna\onedrive\desktop\ch\data\2017\1.csv';
EXEC Compare_Data_correctness 2018,'["5040"]' ,'c:\users\kruna\onedrive\desktop\ch\data\2018\1.csv';

