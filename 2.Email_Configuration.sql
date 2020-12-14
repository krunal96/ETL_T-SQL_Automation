-- Create a Database Mail account  
EXECUTE msdb.dbo.sysmail_add_account_sp  
    @account_name = 'Test11 Administrator',  
    @description = 'Mail account for administrative e-mail.',  
    @email_address = 'krunaljagani7996@gmail.com',  
    @replyto_address = 'krunaljagani7996@gmail.com',  
    @display_name = 'AdventureWorks Automated Mailer',  
    @mailserver_name = 'smtp.gmail.com',
	@username = 'krunaljagani7996@gmail.com',
	@password = 'Palakpatel@2002',
	@port = 587,
	@enable_ssl  = 1


-- Create a Database Mail profile  
EXECUTE msdb.dbo.sysmail_add_profile_sp  
    @profile_name = 'Administrator11 Profile',  
    @description = 'Profile used for administrative mail.' ;  
  
-- Add the account to the profile  
EXECUTE msdb.dbo.sysmail_add_profileaccount_sp  
    @profile_name = 'Administrator11 Profile',  
    @account_name = 'Test11 Administrator',  
    @sequence_number =1 ;  
  
-- Grant access to the profile to the DBMailUsers role  
EXECUTE msdb.dbo.sysmail_add_principalprofile_sp  
    @profile_name = 'Administrator11 Profile',  
    @principal_name = 'ApplicationUser',  
    @is_default = 1 ;  

EXEC msdb.dbo.sp_send_dbmail  
    @profile_name = 'Administrator11 Profile',  
    @recipients = 'krunaljagani7996@gmail.com',  
    @body = 'The stored procedure finished successfully.',  
    @subject = 'Automated Success Message' ;  

USE Practice_SCD
GO

SP_CONFIGURE 'show advanced options', 1
RECONFIGURE WITH OVERRIDE
GO

/* Enable Database Mail XPs Advanced Options in SQL Server */
SP_CONFIGURE 'Database Mail XPs', 1
RECONFIGURE WITH OVERRIDE
GO

SP_CONFIGURE 'show advanced options', 0
RECONFIGURE WITH OVERRIDE
GO


EXEC sp_configure 'show advanced', 1;  
RECONFIGURE; 
EXEC sp_configure; 
GO

SELECT * FROM msdb..sysmail_mailitems WHERE sent_date > DATEADD(DAY, -1,GETDATE())


 
CREATE TABLE DB_Errors
         (ErrorID        INT IDENTITY(1, 1),
          UserName       VARCHAR(100),
          ErrorNumber    INT,
          ErrorState     INT,
          ErrorSeverity  INT,
          ErrorLine      INT,
          ErrorProcedure VARCHAR(MAX),
          ErrorMessage   VARCHAR(MAX),
          ErrorDateTime  DATETIME)
GO