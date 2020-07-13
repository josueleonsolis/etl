/****** Object:  StoredProcedure [dbo].[ZMX_GETStringETL]    Script Date: 15/11/2017 01:54:26 p.m. ******/
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ZMX_GETStringETL]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[ZMX_GETStringETL]
GO

/****** Object:  StoredProcedure [dbo].[ZMX_GETStringETL]    Script Date: 15/11/2017 01:54:26 p.m. ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ZMX_GETStringETL]') AND type in (N'P', N'PC'))
BEGIN
EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [dbo].[ZMX_GETStringETL] AS' 
END
GO





ALTER PROC [dbo].[ZMX_GETStringETL]
(
	@stringConectionCS NVARCHAR(1000) = NULL output,
	@stringConectionERP NVARCHAR(1000)= NULL output
)
AS
/*** ETL ****/
	DECLARE
	@dataSource NVARCHAR(100),
	@catalog NVARCHAR(100),
	@user NVARCHAR(60), 
	@binarypassword VARBINARY(8000),
	@password NVARCHAR(100),
	@provider NVARCHAR(500),
	@providerERP NVARCHAR(500),
	@integratedSecurity NVARCHAR(10),
	@ERP NVARCHAR(50),
	@PathETL NVARCHAR(1000),
	@schemadb nvarchar(100)


	SELECT @PathETL=PathETL, @dataSource=nameServer, @catalog=nameDB, @user=[user],@binarypassword=[password],@provider=[Provider],@integratedSecurity=IntegratedSecurity FROM ZMX_ConfigERPMongoose 
	EXEC zmx_decryptpassword @binarypassword,@password OUTPUT 
	 SET @password = REPLACE (@password ,'@','<*>')
	SET @stringConectionCS = @dataSource+'@'+@catalog+'@'+@user+'@'+@password+'@'+@integratedSecurity
	--Print @stringConection


	SELECT @dataSource=nameServerERP, @catalog=ISNULL(nameDBERP,''), @user=[userERP],@binarypassword=[passwordERP],@providerERP=nameProviderERP,@integratedSecurity=IntegratedSecurity,@ERP=ERP,@schemadb=ISNULL(schemedb,'-') FROM ZMX_ConfigERPMongoose 
	EXEC zmx_decryptpassword @binarypassword,@password OUTPUT 
	 SET @password = REPLACE (@password ,'@','<*>')
	SET @stringConectionERP = @dataSource +'@'+@catalog+'@'+@user+'@'+@password+'@'+@integratedSecurity+'@'+@ERP+'@QGPL'--+@schemadb
	--Print @stringConectionERP





GO


