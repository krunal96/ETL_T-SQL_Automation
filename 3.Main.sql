-- It is a main script that handles ETL logic. 
-- Source data processing is done in-memory. Constraints and necessary validations are applied here. 
-- Error handling logic is implemented and error output is stored in the error table. 
-- Email notification functionality is implemented if fatal error occurs.

ALTER PROCEDURE Insert_fun (
	@csv1 AS NVARCHAR(50),
	@csv2 AS NVARCHAR(50),
	@final_table_name AS NVARCHAR(50)
	)
AS
BEGIN

   -- Processing of Financial_D file
	IF Object_id('tempdb..#temp', 'U') IS NOT NULL
		DROP TABLE #temp;

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


    -- Temporary table data loading script wrapped inside Try, Catch block
	-- Instances of Exception : File path not found, source schema is different than table schema, Selection of wrong file, File format different etc..
	BEGIN TRY
		BEGIN TRANSACTION

		DECLARE @sql NVARCHAR(4000) = 'BULK INSERT #temp FROM ''' + @csv1 + ''' WITH ( FIELDTERMINATOR ='','', ROWTERMINATOR =''\n'', FORMAT = ''CSV'', FIELDQUOTE = ''"'',  FIRSTROW = 2)';

		EXEC (@sql);

		COMMIT TRANSACTION
	END TRY

	BEGIN CATCH
		INSERT INTO [dbo].[db_errors]
		VALUES (
			Suser_sname(),
			Error_number(),
			Error_state(),
			Error_severity(),
			Error_line(),
			Error_procedure(),
			Error_message(),
			Getdate()
			);

		-- Transaction uncommittable. Possibility of Fatal error. Transaction rollback. Email will be sent to the appropriate user.
		IF (Xact_state()) = - 1
			ROLLBACK TRANSACTION

			DECLARE @ERROR_MSG AS NVARCHAR(255) = Error_message()

			EXEC msdb.dbo.Sp_send_dbmail 
					@profile_name = 'Administrator11 Profile',
					@recipients = 'krunaljagani7996@gmail.com',
					@body = @ERROR_MSG,
					@subject = 'SQL ETL Load Failed';

		-- Transaction committable. Exception is not fatal and it can be commited.
		IF (Xact_state()) = 1
			COMMIT TRANSACTION
	END CATCH
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	 -- Processing of Compensation file
	IF Object_id('tempdb..#temp2', 'U') IS NOT NULL
		DROP TABLE #temp2;

	CREATE TABLE #temp2 (
		["bn registration number"] [VARCHAR](50) NULL,
		["fiscal period end"] [DATETIME] NULL,
		["form id"] [VARCHAR](50) NULL,
		[300] [VARCHAR](50) NULL,
		[305] [VARCHAR](50) NULL,
		[310] [VARCHAR](50) NULL,
		[315] [VARCHAR](50) NULL,
		[320] [VARCHAR](50) NULL,
		[325] [VARCHAR](50) NULL,
		[330] [VARCHAR](50) NULL,
		[335] [VARCHAR](50) NULL,
		[340] [VARCHAR](50) NULL,
		[345] [VARCHAR](50) NULL,
		[370] [VARCHAR](50) NULL,
		[380] [VARCHAR](50) NULL,
		[390] [VARCHAR](50) NULL
		) ON [PRIMARY];

    -- Temporary table data loading script wrapped inside Try, Catch block
	-- Instances of Exception : File path not found, source schema is different than table schema, Selection of wrong file, File format different etc..
	BEGIN TRY
		BEGIN TRANSACTION

		DECLARE @sql2 NVARCHAR(4000) = 'BULK INSERT #temp2 FROM ''' + @csv2 + ''' WITH ( FIELDTERMINATOR ='','', ROWTERMINATOR =''\n'', FORMAT = ''CSV'', FIELDQUOTE = ''"'',  FIRSTROW = 2)';

		EXEC (@sql2);

		COMMIT TRANSACTION
	END TRY

	BEGIN CATCH
		INSERT INTO [dbo].[db_errors]
		VALUES (
			Suser_sname(),
			Error_number(),
			Error_state(),
			Error_severity(),
			Error_line(),
			Error_procedure(),
			Error_message(),
			Getdate()
			);

		-- Transaction uncommittable. Possibility of Fatal error. Transaction rollback. Email will be sent to the appropriate user.
		IF (Xact_state()) = - 1
			ROLLBACK TRANSACTION

			DECLARE @ERROR_MSG2 AS NVARCHAR(255) = Error_message()

			EXEC msdb.dbo.Sp_send_dbmail 
					@profile_name = 'Administrator11 Profile',
					@recipients = 'krunaljagani7996@gmail.com',
					@body = @ERROR_MSG2,
					@subject = 'SQL ETL Load Failed';
	
		-- Transaction committable. Exception is not fatal and it can be commited.
		IF (Xact_state()) = 1
			COMMIT TRANSACTION
	END CATCH

	-----------------------------------------------------------------------------------------------------------------------------------------------------------------
	
	-- If  financial file and Compensation files have different number of rows than it means either of the file is corrupt. It will terminate the stored procedure.
	DECLARE @row_count_1 int;
    DECLARE @row_count_2 int;

    set @row_count_1= (select count(*) from #temp);
    set @row_count_2 = (select count(*) from #temp2);

	IF (@row_count_1 != @row_count_2)
	BEGIN
		PRINT('Two files have unqual rows count');
		RETURN;
	END
	------------------------------------------------------------------------------------------------------------------------------------------------------------------

	-- Joining logic of Financial_D file and Compensation file.
	SELECT a.*,
		b.[300],
		b.[305],
		b.[310],
		b.[315],
		b.[320],
		b.[325],
		b.[330],
		b.[335],
		b.[340],
		b.[345],
		b.[370],
		b.[380],
		b.[390]
	INTO #temp4
	FROM #temp a
	LEFT JOIN #temp2 b
		ON a.["bn registration number"] = b.["bn registration number"]
			AND a.["fiscal period end"] = b.["fiscal period end"]

	------------------------------------------------------------------------------------------------------------------------------------------------------------------

	--Loading merged data in the destination table.
	-- Data cleaning and Data transformation is applied here. For instance : Removing $, Casting data types, Mapping CRA codes to the Business attributes etc...
	DECLARE @query NVARCHAR(4000) = 'insert into ' + 
			@final_table_name + ' select ["BN Registration Number"] as Business_Number,    
			CAST(RIGHT(["4700"], LEN(["4700"]) - 1) as money) as Total_Revenue,  
			CAST(RIGHT(["5100"], LEN(["5100"]) - 1) as money) as Total_Expenses,   
			CAST(RIGHT(["4860"], LEN(["4860"]) - 1) as money) as EPCF,   
			CAST(RIGHT([390], LEN([390]) - 1) as money) as Expenditure_Compensation,   
			CAST([300] as int) as Fulltime_Employee,   
			CAST([370] as int) as Parttime_Employee,   
			CAST(RIGHT(["5000"], LEN(["5000"]) - 1) as money) as ECA,   
			CAST(RIGHT(["5010"], LEN(["5010"]) - 1) as money) as EMA,   
			CAST(RIGHT(["5020"], LEN(["5020"]) - 1) as money) as EF5020,       
			CAST(RIGHT(["5050"], LEN(["5050"]) - 1) as money) as Gifts,   
			CAST(RIGHT(["5040"], LEN(["5040"]) - 1) as money) as Other_Expenditure,   
			YEAR(["Fiscal Period End"]) as "year",   
			MONTH(["Fiscal Period End"]) as "month",  
			DAY(["Fiscal Period End"]) as "day" from #temp4;'


	-- Final table data loading script wrapped inside Try, Catch block
	-- Instances of Exception : Type casting error, schema mismatch, Erroreneous data in the fields (String value/ special character in money field) etc..
	BEGIN TRY
		EXEC (@query);
	END TRY

	BEGIN CATCH
		INSERT INTO [dbo].[DB_Errors]
		VALUES (
			Suser_sname(),
			Error_number(),
			Error_state(),
			Error_severity(),
			Error_line(),
			Error_procedure(),
			Error_message(),
			Getdate()
			);

		DECLARE @ERROR_MSG3 AS NVARCHAR(255) = Error_message()

		EXEC msdb.dbo.Sp_send_dbmail @profile_name = 'Administrator11 Profile',
			@recipients = 'krunaljagani7996@gmail.com',
			@body = @ERROR_MSG3,
			@subject = 'SQL ETL Load Failed';

		-- Transaction committable 
		IF (Xact_state()) = 1
			COMMIT TRANSACTION
	END CATCH

END;


-----------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Execution of procedure. 
-- Format : "EXEC sp_name Financial_D file path,Compensation file path, Destination Table Name"
exec insert_fun "c:\users\kruna\onedrive\desktop\ch\data\2015\1.csv", "c:\users\kruna\onedrive\desktop\ch\data\2015\2.csv", "Final_Table";
exec insert_fun "c:\users\kruna\onedrive\desktop\ch\data\2016\1.csv", "c:\users\kruna\onedrive\desktop\ch\data\2016\2.csv", "final_table";
exec insert_fun "c:\users\kruna\onedrive\desktop\ch\data\2017\1.csv", "c:\users\kruna\onedrive\desktop\ch\data\2017\2.csv", "final_table";
exec insert_fun "c:\users\kruna\onedrive\desktop\ch\data\2018\1.csv", "c:\users\kruna\onedrive\desktop\ch\data\2018\2.csv", "final_table";
--truncate table Final_Table
--Drop table Final_Table