/****** Object:  StoredProcedure [dbo].[ZMX_CALLETL]    Script Date: 15/11/2017 01:49:02 p.m. ******/IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ZMX_CALLETL]') AND type in (N'P', N'PC'))
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ZMX_CALLETL]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[ZMX_CALLETL]
GO

/****** Object:  StoredProcedure [dbo].[ZMX_CALLETL]    Script Date: 15/11/2017 01:49:02 p.m. ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ZMX_CALLETL]') AND type in (N'P', N'PC'))
BEGIN
EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [dbo].[ZMX_CALLETL] AS' 
END
GO




ALTER PROCEDURE [dbo].[ZMX_CALLETL]
WITH EXECUTE AS CALLER
AS
DECLARE
@company nvarchar(100),
@serie nvarchar(50),
@folio nvarchar(50)

DECLARE cETL cursor GLOBAL
	for select company,serie,folio from ZMX_RequestERPVoucher
	Open cETL
	fetch cETL into @company,@serie,@folio
	while(@@fetch_status=0)
	begin
			/*** ETL ****/
			DECLARE
			@stringConectionCS NVARCHAR(1000),
			@stringConectionERP NVARCHAR(1000),
			@PathETL NVARCHAR(1000)


			SELECT @PathETL=PathETL FROM ZMX_ConfigERPMongoose 
			
			EXEC ZMX_GETStringETL @stringConectionCS output, @stringConectionERP output

			Declare @command nvarchar(4000)

			IF(@serie IS NULL OR @serie = '')BEGIN
				SET @serie = '-'
			END
				SET @command = @PathETL+' Extraction '+@stringConectionERP+' '+@stringConectionCS+' '+@company+ ' '+ @serie+ ' '+CAST(@folio AS nvarchar) + ' -' + ' -' + ' -' + ' -' + ' -' + ' -' + ' -'--(- ISNULL)
		
				DECLARE @rc int
				EXEC @rc = master.dbo.xp_cmdshell @command, no_output
				print @rc

				IF( @serie = '-')
					SET @serie = ''

				DELETE FROM ZMX_RequestERPVoucher WHERE company = @company and ISNULL(serie,'')=@serie and folio = @folio
		--	/*** ETL ****/
		    select @company,NULLIF(@serie,'-'),@folio
		    
	fetch cETL into @company,@serie,@folio
	end
	close cETL
	deallocate cETL






GO


