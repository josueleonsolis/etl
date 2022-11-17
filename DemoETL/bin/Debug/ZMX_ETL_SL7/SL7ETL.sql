USE [CSHMILLE_App]
GO

/****** Object:  StoredProcedure [dbo].[ZMX_CALLSL7]    Script Date: 11/10/2017 6:43:21 PM ******/
DROP PROCEDURE [dbo].[ZMX_CALLSL7]
GO

/****** Object:  StoredProcedure [dbo].[ZMX_CALLSL7]    Script Date: 11/10/2017 6:43:21 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




CREATE PROCEDURE [dbo].[ZMX_CALLSL7]
WITH EXECUTE AS CALLER
AS


			/*** ETL ****/
			DECLARE
			@stringConectionCS NVARCHAR(1000),
			@stringConectionERP NVARCHAR(1000),
			@PathETL NVARCHAR(1000)


			SELECT @PathETL=PathETL FROM ZMX_ConfigERPMongoose 
			SET @PathETL = REPLACE (  @PathETL, 'DemoETL.exe' , 'ETL_SL7.exe' ) 
			

			EXEC ZMX_GETStringETL @stringConectionCS output, @stringConectionERP output

			Declare @command nvarchar(4000)

			
				SET @command = @PathETL+' Extraction '+@stringConectionERP+' '+@stringConectionCS+' - - - -' + ' -' + ' -' + ' -' + ' -' + ' -' + ' -'--(- ISNULL)
		
				DECLARE @rc int
				EXEC @rc = master.dbo.xp_cmdshell @command, no_output
				print @rc


GO


