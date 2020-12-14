-- Data Completeness Test : Check if Source and Destination tables have number of records
-- User can dynamically select source file  and Year(2015, 2016, etc.)

ALTER PROCEDURE Compare_YearWise_RowCount
(
 @year int,
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

	-- Source and Destination Data processing. This block of code will store Source record count for into @row_count_Expected and destination record count into @row_count_Actual
    DECLARE @row_count_Expected int;
    DECLARE @row_count_Actual int;
    set @row_count_Expected = (select count(*) from #temp);
    set @row_count_Actual = (select count(*) from [dbo].[Final_Table] where [Year] = @year);

	-- Comparing Source and Destination values  and print appropriate message.
	PRINT @year;
	IF (@row_count_Expected = @row_count_Actual)
		PRINT 'CHECK SUCCESS : ' + CAST(@row_count_Expected as nvarchar(255)) + ' == ' + CAST(@row_count_Actual as nvarchar(255));
	ELSE 
		PRINT 'CHECK FAILED';
END


--------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Execution of procedure. 
-- Format : "EXEC sp_name year,Filepath". 
EXEC Compare_YearWise_RowCount 2015, 'c:\users\kruna\onedrive\desktop\ch\data\2015\1.csv';

EXEC Compare_YearWise_RowCount 2016, 'c:\users\kruna\onedrive\desktop\ch\data\2016\1.csv';

EXEC Compare_YearWise_RowCount 2017, 'c:\users\kruna\onedrive\desktop\ch\data\2017\1.csv';

EXEC Compare_YearWise_RowCount 2018, 'c:\users\kruna\onedrive\desktop\ch\data\2018\1.csv';