if not exists (select 1 from user_class where class_name = N'ZMX_VoucherUET' )
-- Create Class:	ZMX_VoucherUET
INSERT INTO user_class ( class_name, class_label, class_desc,
sys_has_fields, sys_has_tables, sys_apply, sys_delete ) VALUES ( N'ZMX_VoucherUET', NULL, N'Campo para el path de CFDI',          0,          0, NULL,          0 )

-- Create Field :	Uf_pathCFDI
IF NOT EXISTS(Select 1 from user_fld where fld_name = N'Uf_pathCFDI')
	INSERT INTO user_fld (   fld_name
							,fld_data_type
							,fld_initial
							,fld_decimals
							,fld_desc
							,sys_apply
							,sys_delete
							,fld_UDT
							,fld_prec
						 )
				 VALUES ( 
						N'Uf_pathCFDI'
						, N'nvarchar'
						, NULL
						,          0
						, NULL
						, NULL
						,          0
						, NULL
						,       1000 )

-- Create Class / Field xRef:	ZMX_VoucherUET, Uf_pathCFDI
IF NOT EXISTS(Select 1 from user_class_fld where class_name = N'ZMX_VoucherUET'AND fld_name = N'Uf_pathCFDI')
	INSERT INTO user_class_fld ( 
							 class_name
							,fld_name
							,sys_apply
							,sys_delete
								)
						VALUES (
								 N'ZMX_VoucherUET'
								, N'Uf_pathCFDI'
								, NULL
								,          0 
								)

-- Create Table / Class xRef :	ZMX_Voucher, ZMX_VoucherUET
IF NOT EXISTS(Select 1 from table_class where table_name = N'ZMX_Voucher' AND class_name = N'ZMX_VoucherUET' )
	INSERT INTO table_class ( 
							 table_name
							,class_name
							,table_rule
							,extend_all_recs
							,sys_apply
							,sys_delete
							,allow_record_assoc
							,active )
					 VALUES ( 
							N'ZMX_Voucher'
							,N'ZMX_VoucherUET' 
							,NULL
							,         0 
							,NULL
							,         0
							,         0
							,         1
							)



--Impact Data Base Tables
DECLARE @RedoViews TINYINT
 ,@Infobar InfobarType

EXEC UETImpactWrapperSp 
  1
 ,0
 ,1
 ,@RedoViews OUTPUT
 ,@Infobar OUTPUT
 
GO