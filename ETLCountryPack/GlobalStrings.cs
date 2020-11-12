using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETLCountryPack
{
    

    public class ConnectionData
    {
        public static string StringConnection { get; set; } = String.Empty;
        public static string ProviderConnection { get; set; } = String.Empty;
        public static string StringConnectionERP { get; set; } = String.Empty;
        public static string ProviderConnectionERP { get; set; } = String.Empty;
        public static string SchemaDefault { get; set; } = String.Empty;
    }
    public class Conceptos
    {
        public int linea;
        //public int PartsSeq;
        public string item;
        public Guid conceptoId;
        public decimal dbase;
        public string unidad;
        public string descripcion;
        //public string impuesto;
        //public string tipofactor;
        //public decimal tasaocuota;
        //public decimal importe;
    }
    //Agregado por JL para almacenar las configuraciones
    public class Configuraciones
    {
        public int CalculateXML;
    }
    //Agregado por JL - 23102019 para procesar los detalles del complemento de vehiculos
  
    public class CompleVehiDet
    {
        public string Amount;
        public string Unit;
        public string NoIdent;
        public string Descr;
        public string PUnit;
        public string Impo;
        public string Serie;
        public string Folio;
        public string Seque;
        public string CVDetalleID;
        public string VoucherID;
    }
    //
    //Agregado por JL para almacenar lo recuperado de voucherdecimal
    public class VoucherDecimals
    {
        public int Concept_PriceUnit;
        public int Concept_Amount;
        public int Concept_Discount;
        public int Concept_Imp_import;
        public int Concept_trasl_base;
        public int Concept_trasl_tasaocuota;
        public int Concept_trasl_import;
        public int Concept_reten_base;
        public int Concept_reten_tasaocuota;
        public int Concept_reten_import;
        public int Concept_part_amount;
        public int Concept_part_import;
        public int Concept_part_unitvalue;
        
      
    }

    public class GlobalStrings
    {
        //Funcion para truncar 
        public static decimal TruncateDecimal(decimal value, int precision)
        {
            decimal step = (decimal)Math.Pow(10, precision);
            decimal tmp = Math.Truncate(step * value);
            return tmp / step;
        }
        //Agregado por JL para el menejo de la existencia de las columnas
        public string ExistsCol;
        //Agregado por JL para la insercion de la info de rq a hd por fallos XA
        public static string Config { get; set; } = String.Empty;
        //
        //Agregado por JL -29102019 para el manejo de la configuracion del subtotal cuando el erp sea XA
        public static bool GetSubTotalXA { get; set; } = false;
        public static bool UseConfigDecimals { get; set; } = false;
        //Agregado por JL -25052020 para el manejo de la configuracion Multisite
        public static bool UseMultisite { get; set; } = false;
        //
        public static string SiteERP { get; set; } = String.Empty;
        public static string V0CONO { get; set; } = String.Empty;
        public static string V0SERIE { get; set; } = String.Empty;
        public static string V0FOLIO { get; set; } = String.Empty;
        public static string V9DTTM { get; set; } = String.Empty;
        public static int industrialNumber { get; set; } = 0;
        public static string ERP { get; set; } = String.Empty;
        public static string pathCFDI { get; set; } = String.Empty;
        //public static string companyId { get; set; }
        public static Guid comprobanteId { get; set; }
        public static Guid comprobanteIdUpdate { get; set; }
        public static string version { get; set; } = String.Empty;
        public static string rfcEmisor { get; set; } = String.Empty;
        public static string rfcReceptor { get; set; } = String.Empty;
        public static string lugarExpedicion { get; set; } = String.Empty;
        public static string SiteRef { get; set; } = String.Empty;
        //public static string SLVersion { get; set; } = String.Empty;
        public static string cliente { get; set; } = String.Empty;
        public static string formaPago { get; set; } = String.Empty;
        public static string metodoPago { get; set; } = String.Empty;
        //Bandera de Rollback
        public static int intRollback { get; set; } = 0;
        public static string msgNotExistUM { get; set; } = String.Empty;
        //Agregado por JL para el requerimiento sustitucion de complemento de pago
        public static string V9CFDIREL { get; set; } = String.Empty;
        //Agregado por JL para almacenar el valor de sitio
        public static string Parm_Site { get; set; } = String.Empty;
        //Agregado por jl para el almacnamiento de voucherdecimals
        public static VoucherDecimals objVoucherDec { get; set; }
        //Agregado por JL para la busqueda del sitio
        public static List<Configuraciones> ListConfiguraciones { get; set; }
        //Agregado por JL - 23102019 para procesar la informacion de los detalles del cv
        public static List<CompleVehiDet> ListDetCV { get; set; }
        //
        //Agregado por jl para la validacion de la existencia de las columnas
        public static string ValidarExistenciaColumna { get; set; } = @"IF EXISTS(SELECT 1 FROM sys.columns  WHERE Name = N'{0}' AND Object_ID = Object_ID(N'{1}'))
            BEGIN
                SELECT '1' AS Existe
            END
        ELSE
            BEGIN
                SELECT '0' AS Existe
            END";

        //Agregado por JL para la validar la existencia de la tabla
        public static string ValidateExistTable { get; set; } = @"IF EXISTS (SELECT * FROM sysobjects WHERE type = 'U' AND name = '{0}')
	        BEGIN
                SELECT '1' AS Existe
            END
        ELSE
            BEGIN
                SELECT '0' AS Existe
            END";
        //

        //Agregado por JL - 23102019 para validar la existencia de los registros
        public static string SelectExistsReg { get; set; } = @"IF EXISTS(SELECT TOP 1 * FROM {0} WITH (NOLOCK) WHERE  {1}= '{2}' AND  {3} = '{4}' AND {5} = '{6}' )
 SELECT '1' AS Exist ELSE SELECT '0' AS Exist ";
        //
        //Consultas para el encanbezado del complemento
        public static string SelectComplementVehiHD { get; set; } = @"SELECT VERS,CVEVEH,NIV,SERIE,FOLIO,CONO,COMPLE,TYPECP,SEQUEN,AMOAD,AMOENA,BRAN,MOD,TYPE,NUMENGI,NUMSER,VAL FROM MXEICV WITH (NOLOCK) WHERE SERIE='{0}' AND FOLIO='{1}' AND CONO='{2}' ";
        public static string InsertComplementVehiHD { get; set; } = @"INSERT INTO ZMX_MXEICV WITH(ROWLOCK) (VOUCHERID,VERS,CVEVEH,NIV,SERIE,FOLIO,CONO,COMPLE,TYPECP,SEQUEN,AMOAD,AMOENA,BRAN,MOD,TYPE,NUMENGI,NUMSER,VAL) VALUES('{0}',@VERS,@CVEVEH,@NIV,@SERIE,@FOLIO,@CONO,@COMPLE,@TYPECP,@SEQUEN,@AMOAD,@AMOENA,@BRAN,@MOD,@TYPE,@NUMENGI,@NUMSER,@VAL)";
        public static string DeleteComplementVehiHD { get; set; } = @"DELETE FROM  ZMX_MXEICV WITH(ROWLOCK) WHERE SERIE='{0}' AND FOLIO='{1}'";
        //
        //Consultas para el detalle del complemento
        public static string SelectComplementVehiDT { get; set; } = @"SELECT AMOUNT,UNIT,NOIDENT,DESCRI,PUNIT,IMPO,SERIE,FOLIO,SEQUEN,CONO,SEQHD FROM MXEICVD WITH (NOLOCK) WHERE SERIE='{0}' AND FOLIO='{1}' AND CONO='{2}' ";
        public static string InsertComplementVehiDT { get; set; } = @"INSERT INTO ZMX_MXEICVD WITH(ROWLOCK) (VOUCHERID,AMOUNT,UNIT,NOIDENT,DESCRI,PUNIT,IMPO,SERIE,FOLIO,SEQUEN,CONO,SEQHD) VALUES('{0}',@AMOUNT,@UNIT,@NOIDENT,@DESCRI,@PUNIT,@IMPO,@SERIE,@FOLIO,@SEQUEN,@CONO,@SEQHD)";
        public static string DeleteComplementVehiDT { get; set; } = @"DELETE FROM  ZMX_MXEICVD WITH(ROWLOCK) WHERE SERIE='{0}' AND FOLIO='{1}' ";
        //
        //Consultas para la aduana del detalle del complemento
        public static string SelectComplementVehiDTAD { get; set; } = @"SELECT NUM,ADUA,DATE,SERIE,FOLIO,SEQUEN,CONO,NIVDET,TYPE,SEQHD FROM MXEICVDA WITH (NOLOCK) WHERE SERIE='{0}' AND FOLIO='{1}' AND CONO='{2}' ";
        public static string InsertComplementVehiDTAD { get; set; } = @"INSERT INTO ZMX_MXEICVDA WITH(ROWLOCK) (CVDETALLEID,NUM,ADUA,DATE,SERIE,FOLIO,SEQUEN,CONO,VOUCHERID,NIVDET,TYPE,SEQHD) VALUES ((SELECT TOP 1 CVDETALLEID FROM ZMX_MXEICVD WHERE SERIE=@SERIE AND FOLIO=@FOLIO AND SEQUEN=@SEQUEN ),@NUM,@ADUA,@DATE,@SERIE,@FOLIO,@SEQUEN,@CONO,'{0}',@NIVDET,@TYPE,@SEQHD)";
        public static string DeleteComplementVehiDTAD { get; set; } = @"DELETE FROM  ZMX_MXEICVDA WITH(ROWLOCK) WHERE SERIE='{0}' AND FOLIO='{1}'";
        // 
        //Consultas para datos adicionales del complemento de vehiculo
        public static string SelectComplementVehComp { get; set; } = @"SELECT CONO,SERIE, FOLIO, INV AS Inventory, TRANS AS Transmission,SATY AS SaleType,ORIG AS Origin,BRAN AS Brand,MOD AS Model,CLASS AS Class,TYPE AS Type,DOOR AS Doors,CYL AS Cylinders,CAP AS Capacity,FUEL AS Fuel,ENGI AS Engine,AGEN AS Agent,EXTCOL AS ExteriorColor, INTCOL AS InteriorColor,COMM AS Comments,REPBRM AS RepresentLegBRM,TXTMOT AS TextMotor,TXTVIN AS TextVin, UUTH1, UUTH2  FROM MXEICVCP WITH (NOLOCK) WHERE SERIE='{0}' AND FOLIO='{1}' AND CONO='{2}' ";
        public static string InsertComplementVehComp { get; set; } = @"INSERT INTO ZMX_MXEICVCOMP WITH(ROWLOCK) (CONO,SERIE, FOLIO,VoucherId,Inventory,Transmission,SaleType,Origin,Brand,Model,Class,Type,Doors,Cylinders,Capacity,Fuel,Engine,Agent,ExteriorColor,InteriorColor,Comments,RepresentLegBRM,TextMotor,TextVin,UUTH1,UUTH2) VALUES (@CONO,@SERIE, @FOLIO,'{0}',@Inventory,@Transmission,@SaleType,@Origin,@Brand,@Model,@Class,@Type,@Doors,@Cylinders,@Capacity,@Fuel,@Engine,@Agent,@ExteriorColor,@InteriorColor,@Comments,@RepresentLegBRM,@TextMotor,@TextVin,@UUTH1,@UUTH2)";
        public static string DeleteComplementVehComp { get; set; } = @"DELETE FROM ZMX_MXEICVCOMP WITH(ROWLOCK) WHERE  SERIE='{0}' AND FOLIO='{1}' ";
        //
        //Consultas agregadas para insertar la informacion del complemento en zxm_complement
        public static string DeleteComplement { get; set; } = @"DELETE FROM  ZMX_Complement WITH(ROWLOCK) WHERE VoucherId='{0}'";
        public static string InsertComplement { get; set; } = @"INSERT INTO ZMX_Complement WITH(ROWLOCK) (VoucherId,complementTypeId,SiteRef) VALUES ('{0}','7','{1}')";
        //
        //Agregaro por JL - 28102019 para eliminar pagos de py y px
        public static string DELETEPY { get; set; } = @"DELETE FROM ZMX_MXEIPY WITH(ROWLOCK) WHERE VQCONO = '{0}' AND VQSERIE = '{1}' AND VQFOLIO = '{2}'";
        public static string UPDATEMXEIPY { get; set; } = @"UPDATE MXEIPY  WITH(ROWLOCK) SET SITEREF='{2}' WHERE ISNULL(VQSERIE,'')='{0}' AND VQFOLIO='{1}'";
        public static string DELETEPX { get; set; } = @"DELETE FROM ZMX_MXEIPX WITH(ROWLOCK) WHERE VRCONO = '{0}' AND VRSERIE = '{1}' AND VRFOLIO = '{2}'";
        public static string UPDATEMXEIPX { get; set; } = @"UPDATE "+ ((GlobalStrings.UseMultisite) ? "MXEIPX_MST": "MXEIPX")+ " WITH(ROWLOCK) SET VRDCUR='{2}',VREXRT='{3}',VRMTPG='{4}',VRNCUO='{5}',VRPSDO='{6}',VRMPAG='{7}',VRNSDO='{8}',VRDCID='{10}' WHERE ISNULL(VRDSER,'')='{0}' AND VRDFOL='{1}' AND VRSERIE='{2}' AND VRFOLIO='{3}' ";

        public static string ValuesPaymentInsert { get; set; }

        public static string ParamsPaymentInsert { get; set; }

        public static string ParamsPaymentUpdate { get; set; }

        public static string ParamsPaymentSelect { get; set; }
        //        
        //Agregado por JL para la busqueda del sitio
        public static string RecuperarSitio { get; set; } = @"SELECT TOP 1 ISNULL(site_ref,'DEFAULT') AS Site FROM parms_mst WITH (NOLOCK)";

        public static string SetSite { get; set; } = @"EXEC SETSITESP '{0}',NULL ";

        //Agreado por JL para manejo de la configuracion del calculo de los importes
        public static string SelectConfiguracion { get; set; } = @"SELECT CalculateXML FROM ZMX_ConfigCFDI WITH (NOLOCK) ";

        //Agregado por JL para el manejo de los numeros de los decimales
        public static string ValidateExistsVoucherDecimals { get; set; } = @"IF EXISTS (SELECT * FROM sysobjects WHERE type = 'U' AND name = 'ZMX_VoucherDecimals')
	        BEGIN
                SELECT '1' AS Existe
            END
        ELSE
            BEGIN
                SELECT '0' AS Existe
            END";
        public static string SelectVoucherDecimals { get; set; } = @"SELECT * FROM ZMX_VoucherDecimals WITH (NOLOCK) ";


        public static string VerificaStatusVoucher { get; set; } = @"IF EXISTS (SELECT TOP 1 serie,folio  FROM ZMX_Voucher WITH (NOLOCK) WHERE  SiteRef='{2}' AND serie = '{0}' AND folio = '{1}' AND voucherStatusId >= 30 ) 
SELECT TOP 1 voucherId AS comprobanteId, 'TIMBRADA'  AS Status,SiteRef AS SiteRef FROM ZMX_Voucher WHERE SiteRef='{2}' AND serie = '{0}' AND folio = '{1}'  
ELSE IF EXISTS (SELECT top 1 serie,folio FROM ZMX_Voucher  WITH (NOLOCK) WHERE SiteRef='{2}' AND serie = '{0}' AND folio = '{1}' AND voucherStatusId <= 30 )
SELECT TOP 1 voucherId AS comprobanteId, 'UPDATE' AS Status,SiteRef AS SiteRef FROM ZMX_Voucher WHERE SiteRef='{2}' AND serie = '{0}' AND folio = '{1}' AND voucherStatusId <= 30 
ELSE 
SELECT 'INSERT' AS Status ";

        public static string SelectrfcEmisorERP { get; set; } = @"SELECT V0RFCE AS rfcEmisor, UPPER(V0CLIEN) AS cliente
                                                                FROM MXEIHD WITH (NOLOCK) WHERE V0CONO ='{0}' AND V0SERIE = '{1}' AND V0FOLIO = '{2}' ";

        public static string SelectrfcEmisorCountryPack { get; set; } = @"SELECT  c.rfc AS rfcEmisor,ISNULL(c.version,'') AS version, ISNULL(cd.postalId,'') As lugarExpedicion, ISNULL(c.SiteRef,'') AS SiteRef
                                                                        FROM ZMX_Company c WITH (NOLOCK)
                                                                        LEFT JOIN ZMX_CompanyAddress cd  WITH (NOLOCK) ON c.companyId = cd.companyId ";


        public static string SelectClienteIdCountryPack { get; set; } = @"SELECT paymentMethod AS metodoPago,paymentWay AS formaPago,UPPER(customerId) AS cliente, rfc AS rfcReceptor from ZMX_Customer WITH (NOLOCK) ";
        //@"SELECT metodoPago AS metodoPago,formaPago AS formaPago,clienteID AS cliente, rfc AS rfcReceptor from ZMX_Cliente ";

        public static string SelectComprobante { get; set; } = @"SELECT V0SERIE AS serie,V0FOLIO AS folio,V0DATE AS date,'' AS stamp,'' AS numberCertificate,'' AS certificate,V0CPAG AS payCondition,
                                                   ABS(ISNULL(V0SUBT,0)) AS SubTotal,ABS(ISNULL(V0DESC,0)) AS discount,ABS(V0TOTL) AS total,'' AS confirmation, V0CURR AS currency,V0EXRT AS exchangeRate,V0PONB AS purchaseOrder,
                                                   V0PODT AS PODate,V0SLSR AS saleRepresentative,UPPER(V0CLIEN) AS customer,V0FOB AS freeLocation,V0NEMB AS shipment,V0GEMB AS billOfLanding, 
                                                   V0EMBX AS shipperVia,V0UUT1 AS UFT1,V0UUT2 AS UFT2,V0UUT3 AS UFT3,V0UUA1 AS UFAMT1,V0UUA2 AS UFAMT2,V0UUA3 AS UFAMT3,V0UUD1 AS UFD1,
                                                   V0UUD2 AS UFD2,V0UUD3 AS UFD3,V0PORV AS purchaseOrderRevision,'' AS relationType,NEWID() AS voucherId,UPPER(SUBSTRING(V0TCOMP,1,1)) AS voucherType,V0ORDNO AS customerOrder, ISNULL(V0EXRTU,0) AS exchangeRateUSD
                                                   FROM MXEIHD WITH (NOLOCK) WHERE V0CONO = '{0}' AND V0SERIE = '{1}' AND V0FOLIO = '{2}' ";

        //Agregado para multisite
        public static string SelectComprobanteSL { get; set; } = @"IF EXISTS(SELECT 1 FROM sys.columns  WHERE Name = N'V0MLSITE' AND Object_ID = Object_ID(N'MXEIHD'))
BEGIN
EXECUTE sp_executesql N'SELECT V0SERIE AS serie,V0FOLIO AS folio,V0DATE AS date,'''' AS stamp,'''' AS numberCertificate,'''' AS certificate,V0CPAG AS payCondition,
                                                   ABS(ISNULL(V0SUBT,0)) AS SubTotal,ABS(ISNULL(V0DESC,0)) AS discount,ABS(V0TOTL) AS total,'''' AS confirmation, V0CURR AS currency,V0EXRT AS exchangeRate,V0PONB AS purchaseOrder,
                                                   V0PODT AS PODate,V0SLSR AS saleRepresentative,UPPER(V0CLIEN) AS customer,V0FOB AS freeLocation,V0NEMB AS shipment,V0GEMB AS billOfLanding, 
                                                   V0EMBX AS shipperVia,V0UUT1 AS UFT1,V0UUT2 AS UFT2,V0UUT3 AS UFT3,V0UUA1 AS UFAMT1,V0UUA2 AS UFAMT2,V0UUA3 AS UFAMT3,V0UUD1 AS UFD1,
                                                   V0UUD2 AS UFD2,V0UUD3 AS UFD3,V0PORV AS purchaseOrderRevision,'''' AS relationType,NEWID() AS voucherId,UPPER(SUBSTRING(V0TCOMP,1,1)) AS voucherType,V0ORDNO AS customerOrder, ISNULL(V0EXRTU,0) AS exchangeRateUSD, V0MLSITE AS MLSITE
                                                   FROM MXEIHD WITH (NOLOCK) WHERE V0CONO = ''{0}'' AND V0SERIE = ''{1}'' AND V0FOLIO = ''{2}'' '
												   END
												   ELSE
												   BEGIN 
												   EXECUTE sp_executesql N'SELECT V0SERIE AS serie,V0FOLIO AS folio,V0DATE AS date,'''' AS stamp,'''' AS numberCertificate,'''' AS certificate,V0CPAG AS payCondition,
                                                   ABS(ISNULL(V0SUBT,0)) AS SubTotal,ABS(ISNULL(V0DESC,0)) AS discount,ABS(V0TOTL) AS total,'''' AS confirmation, V0CURR AS currency,V0EXRT AS exchangeRate,V0PONB AS purchaseOrder,
                                                   V0PODT AS PODate,V0SLSR AS saleRepresentative,UPPER(V0CLIEN) AS customer,V0FOB AS freeLocation,V0NEMB AS shipment,V0GEMB AS billOfLanding, 
                                                   V0EMBX AS shipperVia,V0UUT1 AS UFT1,V0UUT2 AS UFT2,V0UUT3 AS UFT3,V0UUA1 AS UFAMT1,V0UUA2 AS UFAMT2,V0UUA3 AS UFAMT3,V0UUD1 AS UFD1,
                                                   V0UUD2 AS UFD2,V0UUD3 AS UFD3,V0PORV AS purchaseOrderRevision,'''' AS relationType,NEWID() AS voucherId,UPPER(SUBSTRING(V0TCOMP,1,1)) AS voucherType,V0ORDNO AS customerOrder, ISNULL(V0EXRTU,0) AS exchangeRateUSD
                                                   FROM MXEIHD WITH (NOLOCK) WHERE V0CONO = ''{0}'' AND V0SERIE = ''{1}'' AND V0FOLIO = ''{2}'' '
												   END ";

        public static string SelectComprobanteSLNoMulti { get; set; } = @"SELECT V0SERIE AS serie,V0FOLIO AS folio,V0DATE AS date,'' AS stamp,'' AS numberCertificate,'' AS certificate, V0CPAG AS payCondition,
                                                    ABS(ISNULL(V0SUBT, 0)) AS SubTotal, ABS(ISNULL(V0DESC, 0)) AS discount, ABS(V0TOTL) AS total, '' AS confirmation, V0CURR AS currency, V0EXRT AS exchangeRate, V0PONB AS purchaseOrder,
                                                    V0PODT AS PODate, V0SLSR AS saleRepresentative, UPPER(V0CLIEN) AS customer, V0FOB AS freeLocation, V0NEMB AS shipment, V0GEMB AS billOfLanding,
                                                    V0EMBX AS shipperVia, V0UUT1 AS UFT1, V0UUT2 AS UFT2, V0UUT3 AS UFT3, V0UUA1 AS UFAMT1, V0UUA2 AS UFAMT2, V0UUA3 AS UFAMT3, V0UUD1 AS UFD1,
                                                    V0UUD2 AS UFD2, V0UUD3 AS UFD3, V0PORV AS purchaseOrderRevision, '' AS relationType, NEWID() AS voucherId, UPPER(SUBSTRING(V0TCOMP, 1, 1)) AS voucherType, V0ORDNO AS customerOrder, ISNULL(V0EXRTU, 0) AS exchangeRateUSD 
                                                    FROM MXEIHD WITH(NOLOCK) WHERE V0CONO = '{0}' AND V0SERIE = '{1}' AND V0FOLIO = '{2}' ";

        public static string SelectMonedaCountryPack { get; set; } = @"SELECT c.currency AS currency,cc.CurrencyERP AS currencyERP  FROM ZMX_Currency c WITH (NOLOCK)
                                                                     LEFT JOIN ZMX_CurrencyConversion cc WITH (NOLOCK) ON cc.Currency=c.currency ";

        public static string InsertVoucherWithColumn { get; set; } = @"INSERT INTO ZMX_Voucher  WITH(ROWLOCK) (version,serie,folio,date,stamp,numberCertificate,certificate,payCondition,SubTotal,discount,total,confirmation,paymentWay,
                                                            currency,expeditionPlace,paymentMethod,exchangeRate,purchaseOrder,PODate,saleRepresentative,customer,freeLocation,shipment,billOfLandig,
                                                            shipperVia,UFT1,UFT2,UFT3,UFAMT1,UFAMT2,UFAMT3,UFD1,UFD2,UFD3,purchaseOrderRevision,relationType,voucherId,voucherType,customerOrder,SiteRef,exchangeRateUSD,siteERP,Uf_pathCFDI,MLSITE)
                                                            VALUES('{0}',@serie,@folio,@date,@stamp,@numberCertificate,@certificate,@payCondition,@SubTotal,@discount,@total,@confirmation,'{1}',@currency,'{2}','{3}',
                                                            @exchangeRate,@purchaseOrder,@PODate,@saleRepresentative,@customer,@freeLocation,@shipment,@billOfLanding,@shipperVia,@UFT1,@UFT2,@UFT3,@UFAMT1,@UFAMT2,@UFAMT3,
                                                            @UFD1,@UFD2,@UFD3,@purchaseOrderRevision,@relationType,@voucherId,@voucherType,@customerOrder,'{4}',@exchangeRateUSD,'{5}','{6}',@MLSITE) ";




        public static string InsertVoucher { get; set; } = @"INSERT INTO ZMX_Voucher  WITH(ROWLOCK) (version,serie,folio,date,stamp,numberCertificate,certificate,payCondition,SubTotal,discount,total,confirmation,paymentWay,
                                                            currency,expeditionPlace,paymentMethod,exchangeRate,purchaseOrder,PODate,saleRepresentative,customer,freeLocation,shipment,billOfLandig,
                                                            shipperVia,UFT1,UFT2,UFT3,UFAMT1,UFAMT2,UFAMT3,UFD1,UFD2,UFD3,purchaseOrderRevision,relationType,voucherId,voucherType,customerOrder,SiteRef,exchangeRateUSD,siteERP,Uf_pathCFDI)
                                                            VALUES('{0}',@serie,@folio,@date,@stamp,@numberCertificate,@certificate,@payCondition,@SubTotal,@discount,@total,@confirmation,'{1}',@currency,'{2}','{3}',
                                                            @exchangeRate,@purchaseOrder,@PODate,@saleRepresentative,@customer,@freeLocation,@shipment,@billOfLanding,@shipperVia,@UFT1,@UFT2,@UFT3,@UFAMT1,@UFAMT2,@UFAMT3,
                                                            @UFD1,@UFD2,@UFD3,@purchaseOrderRevision,@relationType,@voucherId,@voucherType,@customerOrder,'{4}',@exchangeRateUSD,'{5}','{6}') ";

        public static string UpdateVoucherWithColumn { get; set; } = @"UPDATE ZMX_Voucher  WITH(ROWLOCK) SET version='{0}',date=@date,stamp=@stamp,numberCertificate=@numberCertificate,certificate=@certificate,payCondition=@payCondition,
                                                            SubTotal=@SubTotal,discount=@discount,total=@total,confirmation=@confirmation,paymentWay='{1}',currency=@currency,expeditionPlace='{2}',
                                                            paymentMethod='{3}',exchangeRate=@exchangeRate,purchaseOrder=@purchaseOrder,PODate=@PODate,saleRepresentative=@saleRepresentative,customer=@customer,
                                                            freeLocation=@freeLocation,shipment=@shipment,billOfLandig=@billOfLanding,shipperVia=@shipperVia,UFT1=@UFT1,UFT2=@UFT2,UFT3=@UFT3,UFAMT1=@UFAMT1,
                                                            UFAMT2=@UFAMT2,UFAMT3=@UFAMT3,UFD1=@UFD1,UFD2=@UFD2,UFD3=@UFD3,purchaseOrderRevision=@purchaseOrderRevision,relationType=@relationType,
                                                            voucherType=@voucherType,customerOrder=@customerOrder,SiteRef='{4}',exchangeRateUSD=@exchangeRateUSD, siteERP='{6}',Uf_pathCFDI='{7}',MLSITE=@MLSITE
                                                            WHERE voucherId = '{5}' ";

        public static string UpdateVoucher { get; set; } = @"UPDATE ZMX_Voucher  WITH(ROWLOCK) SET version='{0}',date=@date,stamp=@stamp,numberCertificate=@numberCertificate,certificate=@certificate,payCondition=@payCondition,
                                                            SubTotal=@SubTotal,discount=@discount,total=@total,confirmation=@confirmation,paymentWay='{1}',currency=@currency,expeditionPlace='{2}',
                                                            paymentMethod='{3}',exchangeRate=@exchangeRate,purchaseOrder=@purchaseOrder,PODate=@PODate,saleRepresentative=@saleRepresentative,customer=@customer,
                                                            freeLocation=@freeLocation,shipment=@shipment,billOfLandig=@billOfLanding,shipperVia=@shipperVia,UFT1=@UFT1,UFT2=@UFT2,UFT3=@UFT3,UFAMT1=@UFAMT1,
                                                            UFAMT2=@UFAMT2,UFAMT3=@UFAMT3,UFD1=@UFD1,UFD2=@UFD2,UFD3=@UFD3,purchaseOrderRevision=@purchaseOrderRevision,relationType=@relationType,
                                                            voucherType=@voucherType,customerOrder=@customerOrder,SiteRef='{4}',exchangeRateUSD=@exchangeRateUSD, siteERP='{6}',Uf_pathCFDI='{7}'
                                                            WHERE voucherId = '{5}' ";

        /*{0}= rfcEmisor,{1}=Cono,{2}=Serie,{3}=Folio*/
        public static string SelectEmisorErp { get; set; } = @"SELECT V2UUT1 AS UFT1, V2UUD1 AS UFD1, '{0}' AS rfc FROM MXEIAD WITH (NOLOCK) WHERE V2TREG='C' AND V2CONO='{1}' AND V2SERIE='{2}'AND V2FOLIO='{3}' ";

        /*{0}= rfcEmisor*/
        public static string SelectEmisorCountryPack { get; set; } = @"SELECT rfc,CAST(ISNULL(companyName,'')AS NVARCHAR(254) ) AS name,ISNULL(regim,'') AS  taxRegime FROM ZMX_Company WITH (NOLOCK) WHERE rfc='{0}' ";

        /*{0}= comprobante,{1}= SiteRef*/
        public static string InsertEmisor { get; set; } = @"INSERT INTO ZMX_Emitter  WITH(ROWLOCK) (voucherId,SiteRef,rfc,name,taxRegime,UFT1,UFD1)
                                                          VALUES('{0}','{1}',@rfc, @name, @taxRegime,@UFT1,@UFD1) ";

        public static string UpdateEmisor { get; set; } = @"UPDATE ZMX_Emitter  WITH(ROWLOCK) SET SiteRef='{1}',rfc=@rfc,name=@name,taxRegime=@taxRegime,UFT1=@UFT1,UFD1=@UFD1 
                                                          WHERE voucherId = '{0}' ";

        /*{0}= rfcEmisor,{1}=Cono,{2}=Serie,{3}=Folio*/
        public static string SelectReceptorAddressErp { get; set; } = @"SELECT V2UUT1 AS UFT1, V2UUD1 AS UFD1, '{0}' AS rfc, V2SEAD AS address FROM MXEIAD WITH (NOLOCK) WHERE V2TREG='R' AND V2CONO='{1}' AND V2SERIE='{2}'AND V2FOLIO='{3}' ";

        /*{0}= rfcEmisor,{1}=Cono,{2}=Serie,{3}=Folio*/
        public static string SelectReceptorShiptopErp { get; set; } = @"SELECT V2SEAD AS shipto, '{0}' AS rfc FROM MXEIAD WITH (NOLOCK) WHERE V2TREG='S' AND V2CONO='{1}' AND V2SERIE='{2}'AND V2FOLIO='{3}' ";

        /*{0}= rfcEmisor*/
        public static string SelectReceptorCountryPack { get; set; } = @"SELECT rfc,CAST(ISNULL(name,'')AS NVARCHAR(254) ) AS name ,ISNULL(residFiscal,'') AS  residFiscal,numRegIdTrib AS numRegIdTrib,ISNULL(cfdiUse,'') AS usecfdiId, curp AS curp FROM ZMX_Customer WITH (NOLOCK) WHERE rfc='{0}' AND UPPER(customerId)='{1}' ";

        /*{0}= comprobante,{1}= SiteRef*/
        public static string InsertReceptor { get; set; } = @"INSERT INTO ZMX_Receiver  WITH(ROWLOCK) (voucherId,SiteRef,rfc,name,residFiscal,numRegIdTrib,usecfdiId,curp,address,shipto,UFT1,UFD1)
                                                            VALUES('{0}','{1}',@rfc, @name, @residFiscal,@numRegIdTrib,@usecfdiId,@curp,@address,@shipto,@UFT1,@UFD1) ";

        public static string UpdateReceptor { get; set; } = @" UPDATE ZMX_Receiver  WITH(ROWLOCK) SET SiteRef='{1}',rfc=@rfc,name=@name,residFiscal=@residFiscal,numRegIdTrib=@numRegIdTrib,usecfdiId=@usecfdiId,
                                                            curp=@curp,address=@address,shipto=@shipto,UFT1=@UFT1,UFD1=@UFD1 WHERE voucherId='{0}' ";

        public static string SelectNodoConcepto { get; set; } = @"SELECT V3CNID AS noIdentification,ABS(V3CANT) AS quantity,ABS(V3UNPR) AS unitvalue,ABS(ISNULL(V3DISC,0)) AS discount,NEWID() AS conceptId,
                                                                V3UUT1 AS UFT1,V3UUT2 AS UFT2,V3UUT3 AS UFT3,V3UUA1 AS UFAMT1,V3UUA2 AS UFAMT2,V3UUA3 AS UFAMT3,V3SEQN AS sequence, ISNULL(V3DESC,'NULL') AS description,
                                                                '' AS Accountpattern, ISNULL(V3UNMSR,'NULL') AS unitId,  ISNULL(V3CEUW,0) AS weightPiece, ABS(V3IMPO) AS total FROM MXEIDT WITH (NOLOCK) WHERE V3CONO='{0}' AND V3SERIE='{1}' AND V3FOLIO='{2}' ";
        //Agregado por JL para XA, se quita la columna del peso
        public static string SelectNodoConceptoXA { get; set; } = @"SELECT V3CNID AS noIdentification,ABS(V3CANT) AS quantity,ABS(V3UNPR) AS unitvalue,ABS(ISNULL(V3DISC,0)) AS discount,NEWID() AS conceptId,
                                                                V3UUT1 AS UFT1,V3UUT2 AS UFT2,V3UUT3 AS UFT3,V3UUA1 AS UFAMT1,V3UUA2 AS UFAMT2,V3UUA3 AS UFAMT3,V3SEQN AS sequence, ISNULL(V3DESC,'NULL') AS description,
                                                                '' AS Accountpattern, ISNULL(V3UNMSR,'NULL') AS unitId, ABS(V3IMPO) AS total FROM MXEIDT WITH (NOLOCK) WHERE V3CONO='{0}' AND V3SERIE='{1}' AND V3FOLIO='{2}' ";

        //Agregado por JL para el desarrollo de los impuestos falntantes
        public static string SelectNodoNewConcepto { get; set; } = @"SELECT V3CNID AS noIdentification,ABS(V3CANT) AS quantity,ABS(V3UNPR) AS unitvalue,ABS(ISNULL(V3DISC,0)) AS discount,NEWID() AS conceptId,
                                                                V3UUT1 AS UFT1,V3UUT2 AS UFT2,V3UUT3 AS UFT3,V3UUA1 AS UFAMT1,V3UUA2 AS UFAMT2,V3UUA3 AS UFAMT3,V3SEQN AS sequence, ISNULL(V3DESC,'NULL') AS description,
                                                                '' AS Accountpattern, ISNULL(V3UNMSR,'NULL') AS unitId,  ISNULL(V3CEUW,0) AS weightPiece FROM MXEIDT WITH (NOLOCK) WHERE V3CONO='{0}' AND V3SERIE='{1}' AND V3FOLIO='{2}' AND V3CNID='{3}' AND V3SEQN= '{4}'";

        public static string SelectNodoConceptoUpdate { get; set; } = @"SELECT V3CNID AS noIdentification,ABS(V3CANT) AS quantity,ABS(V3UNPR) AS unitvalue,ABS(ISNULL(V3DISC,0)) AS discount,CAST('{4}' AS uniqueidentifier) AS conceptId,
V3UUT1 AS UFT1,V3UUT2 AS UFT2,V3UUT3 AS UFT3,V3UUA1 AS UFAMT1,V3UUA2 AS UFAMT2,V3UUA3 AS UFAMT3,V3SEQN AS sequence, ISNULL(V3DESC,'NULL') AS description,
'' AS Accountpattern,ISNULL(V3UNMSR,'NULL') AS unitId, ISNULL(V3CEUW,0) AS weightPiece FROM MXEIDT WITH (NOLOCK) WHERE V3CONO='{0}' AND V3SERIE='{1}'AND V3FOLIO='{2}' AND V3SEQN= '{3}' ";

        //public static string SelectConceptoCountryPack { get; set; } = @"IF EXISTS (SELECT * FROM ZMX_ContractsCustomers WHERE item='{0}' AND customer='{1}') 
        //                                                                SELECT i.prodServId ,cc.custItem AS noIdentification,cc.usrFinl AS descripcion,cc.custUM AS unit,i.unitId,'{0}' AS item FROM ZMX_ContractsCustomers cc
        //                                                                LEFT JOIN ZMX_Items i ON cc.item=i.noIdentification WHERE cc.item='{0}' AND cc.customer='{1}' 
        //                                                                ELSE SELECT prodServId ,noIdentification ,description ,unit ,unitId,noIdentification AS item FROM ZMX_Items WHERE noIdentification = '{0}' ";
        //Se modifica por bugs de codigos repetidos = ISNULL(@UFT3,@noIdentification)
        public static string InsertNodoConcepto { get; set; } = @"SET @UFT3 = NULLIF(@UFT3,'');
Insert Into ZMX_concept  WITH(ROWLOCK) (voucherId,SiteRef,prodServId,noIdentification,quantity,unitId,
unit,description,unitvalue,total,discount,conceptId,UFT1,UFT2,UFT3,
UFAMT1,UFAMT2,UFAMT3,sequence,Accountpattern,weightPiece)
VALUES('{0}','{1}',@prodServId,ISNULL(@UFT3,@noIdentification),@quantity,@unitId,
@unit,@description,@unitvalue,@total,@discount,@conceptId,@UFT1,@UFT2,@UFT3,
@UFAMT1,@UFAMT2,@UFAMT3,@sequence,@Accountpattern,@weightPiece) ";

        //Agregado por JL para insertar la informacion del concepto para XA, no considera el peso
        public static string InsertNodoConceptoXA { get; set; } = @"SET @UFT3 = NULLIF(@UFT3,'');
Insert Into ZMX_concept  WITH(ROWLOCK) (voucherId,SiteRef,prodServId,noIdentification,quantity,unitId,
unit,description,unitvalue,total,discount,conceptId,UFT1,UFT2,UFT3,
UFAMT1,UFAMT2,UFAMT3,sequence,Accountpattern,weightPiece)
VALUES('{0}','{1}',@prodServId,ISNULL(@UFT3,@noIdentification),@quantity,@unitId,
@unit,@description,@unitvalue,@total,@discount,@conceptId,@UFT1,@UFT2,@UFT3,
@UFAMT1,@UFAMT2,@UFAMT3,@sequence,@Accountpattern) ";

        //{0}=voucherId,{1}=SiteRef,{2}=conceptId
        //Se modifica por bugs de codigos repetidos = ISNULL(@UFT3,@noIdentification)
        public static string UpdateNodoConcepto { get; set; } = @"  SET @UFT3 = NULLIF(@UFT3,'');
        IF EXISTS (SELECT TOP 1 voucherId,sequence FROM ZMX_concept WHERE voucherId ='{0}' AND sequence=@sequence and conceptId='{2}')
UPDATE ZMX_concept WITH(ROWLOCK) SET SiteRef='{1}',prodServId=@prodServId,noIdentification=ISNULL(@UFT3,@noIdentification),quantity=@quantity,unitId=@unitId,
unit=@unit,description=@description,unitvalue=@unitvalue,total=@total,discount=@discount,conceptId=@conceptId,UFT1=@UFT1,
UFT2=@UFT2,UFT3=@UFT3,UFAMT1=@UFAMT1,UFAMT2=@UFAMT2,UFAMT3=@UFAMT3,sequence=@sequence,Accountpattern=@Accountpattern,weightPiece=@weightPiece
WHERE voucherId = '{0}' AND sequence=@sequence and conceptId = '{2}' 
ELSE Insert Into ZMX_concept WITH(ROWLOCK) (voucherId,SiteRef,prodServId,noIdentification,quantity,unitId,
unit,description,unitvalue,total,discount,conceptId,UFT1,UFT2,UFT3,
UFAMT1,UFAMT2,UFAMT3,sequence,Accountpattern,weightPiece)
VALUES('{0}','{1}',@prodServId,@noIdentification,@quantity,@unitId,
@unit,@description,@unitvalue,@total,@discount,'{2}',@UFT1,@UFT2,@UFT3,
@UFAMT1,@UFAMT2,@UFAMT3,@sequence,@Accountpattern,@weightPiece); ";

        //Agregado por JL 13112019 para la eliminacion de los conceptos
        public static string DeleteConceptosInvoice { get; set; } = @"
DELETE ZMX_CustomsInformation FROM ZMX_CustomsInformation i INNER JOIN ZMX_Concept c ON c.conceptId=i.conceptId
WHERE c.voucherId=  '{0}' ;
DELETE ZMX_TaxConcept FROM ZMX_TaxConcept t INNER JOIN ZMX_Concept c ON c.conceptId=t.conceptId
WHERE c.voucherId=  '{0}' ; 
DELETE ZMX_CONCEPT WHERE VOUCHERID='{0}'";

        //public static string SelectConceptoERP { get; set; } = @"SELECT V3CNID AS noIdentification FROM MXEIDT WHERE V3CONO='{0}' AND V3SERIE='{1}'AND V3FOLIO='{2}'";

        public static string SelectConceptoCountryPack { get; set; } = @"IF EXISTS (SELECT top 1 item FROM ZMX_ContractsCustomers  WITH (NOLOCK) WHERE item='{0}' AND customer='{1}') 
                                                                        SELECT i.prodServId ,/*cc.custItem AS noIdentification,*/cc.usrFinl AS description, ltrim(rtrim(cc.custUM)) AS unit,i.unitId,'{0}' AS item FROM ZMX_ContractsCustomers cc WITH (NOLOCK) 
                                                                        LEFT JOIN ZMX_Items i WITH (NOLOCK) ON cc.item=i.noIdentification WHERE cc.item='{0}' AND cc.customer='{1}' 
                                                                        ELSE SELECT ISNULL(prodServId,'') AS prodServId,/*noIdentification ,*/ ISNULL(description,'') AS description,ISNULL( ltrim(rtrim(unit)),'') AS unit ,ISNULL( ltrim(rtrim(unitId)),'') AS unitId FROM ZMX_Items WITH (NOLOCK) WHERE noIdentification = '{0}' ";


        public static string SelecClaveUnidad { get; set; } = @"IF EXISTS(SELECT top 1 umERP FROM ZMX_RelationUM WITH (NOLOCK) WHERE umERP='{0}')
SELECT ISNULL( ltrim(rtrim(c.unitId)),'') AS claveUnidad ,
CASE WHEN (ISNULL(r.factor,1))=0 THEN 1 ELSE ISNULL(r.factor,1) END AS factor,  ltrim(rtrim(c.name)) AS nombreUnidad
    FROM ZMX_UM c WITH (NOLOCK)
    LEFT JOIN ZMX_RelationUM r WITH (NOLOCK) ON c.unitId = r.um
    WHERE r.umERP='{0}'  
ELSE
SELECT ISNULL(c.unitId,'') AS claveUnidad ,
CASE WHEN (ISNULL(r.factor,1))=0 THEN 1 ELSE ISNULL(r.factor,1) END AS factor,  ltrim(rtrim(c.name)) AS nombreUnidad
    FROM ZMX_UM c WITH (NOLOCK) 
    LEFT JOIN ZMX_RelationUM r WITH (NOLOCK) ON c.unitId = r.um
    WHERE c.unitId = '{0}' ";

        /* @"SELECT ISNULL(c.unitId,'') AS claveUnidad ,ISNULL(r.factor,1) AS factor, c.name AS nombreUnidad
         FROM ZMX_UM c
         LEFT JOIN ZMX_RelationUM r ON c.unitId = r.um
         WHERE r.umERP='{0}' OR c.unitId = '{0}' "; */

        public static string SelectImpuestosERP { get; set; } = @"SELECT V5IMPU AS impuestoERP,V5UUT1 AS UFT1, V5UUD1 AS UFD1, V5UUA1 AS UFAMT1, V5SEQN AS sequence , NEWID() AS taxConceptId, ISNULL(V5IMPO,0) AS total  
                                                                FROM MXEITX WITH (NOLOCK) INNER JOIN MXEIDT WITH (NOLOCK) ON V3SEQN=V5SEQN AND V3CONO=V5CONO AND V3SERIE=V5SERIE AND V3FOLIO=V5FOLIO
                                                                WHERE V5CONO = '{0}' AND V5SERIE = '{1}' AND V5FOLIO = '{2}'  ";
        //Se modifica por error en prinsel
        //SELECT V5IMPU AS impuestoERP,V5UUT1 AS UFT1, V5UUD1 AS UFD1, V5UUA1 AS UFAMT1, V5SEQN AS sequence , NEWID() AS taxConceptId  FROM MXEITX
        //WHERE V5CONO = '{0}' AND V5SERIE = '{1}' AND V5FOLIO = '{2}' ";
        public static string DeleteImpuestos { get; set; } = @"
DELETE ZMX_CustomsInformation FROM ZMX_CustomsInformation i INNER JOIN ZMX_Concept c ON c.conceptId=i.conceptId
WHERE c.voucherId=  '{0}' ;
DELETE ZMX_TaxConcept FROM ZMX_TaxConcept t INNER JOIN ZMX_Concept c ON c.conceptId=t.conceptId
WHERE c.voucherId=  '{0}' ; ";

        //Agregado por JL, para el problema de los conceptos faltantes
        public static string DeleteConceptos { get; set; } = @"
DELETE ZMX_CustomsInformation FROM ZMX_CustomsInformation i INNER JOIN ZMX_Concept c ON c.conceptId=i.conceptId
WHERE c.voucherId=  '{0}' ;
DELETE ZMX_TaxConcept FROM ZMX_TaxConcept t INNER JOIN ZMX_Concept c ON c.conceptId=t.conceptId
WHERE c.voucherId=  '{0}' ; 
DELETE  FROM ZMX_Concept WHERE voucherId=  '{0}' ; ";


        public static string SelectImpuestosCountryPack { get; set; } = @"SELECT taxId,factor,tasaOquote,type, erpSat AS impuestoERP, (SELECT CalculateXML FROM ZMX_ConfigCFDI WITH (NOLOCK) ) AS  Calculate FROM zmx_taxconversion WITH (NOLOCK) ";

        //Se Mod. por Frisco. Sequitan las siguientes lineas
        //DELETE ZMX_CustomsInformation WHERE conceptId =@conceptId ;
        //DELETE ZMX_TaxConcept WHERE conceptId =@conceptId AND type=@type; 
        public static string InsertNodoImpuestos { get; set; } = @"INSERT INTO ZMX_TaxConcept WITH(ROWLOCK) (SiteRef,base,tax,typeFactor,tasaOquote,total,taxConceptId,conceptId,type,UFT1,UFD1,UFAMT1)
                                                                   VALUES ('{0}',@base,@taxId,@factor,@tasaOquote,@total,@taxConceptId,@conceptId,@type,@UFT1,@UFD1,@UFAMT1) ";

        public static string SelectExistsAddenda { get; set; } = @"IF EXISTS ( SELECT * FROM MXEIA1 WITH (NOLOCK) WHERE V1CONO='{0}' AND V1SERIE='{1}' AND V1FOLIO='{2}'  )
 SELECT '1' AS Exist ELSE SELECT '0' AS Exist ";

        public static string SelectAddenda { get; set; } = @"SELECT V1SEQA AS seq,V1ADDA AS addendum FROM MXEIA1 WITH (NOLOCK) 
WHERE V1CONO='{0}' AND V1SERIE='{1}' AND V1FOLIO='{2}' ";

        //DELETE ZMX_VoucherAddendum WHERE voucherId = '{0}' AND SiteRef = '{1}' AND seq = @seq;
        public static string InsertAddenda { get; set; } = @"DELETE ZMX_VoucherAddendum WHERE voucherId='{0}' AND seq=@seq ;
INSERT INTO ZMX_VoucherAddendum WITH(ROWLOCK) (voucherId,SiteRef,seq,addendum)
VALUES ('{0}',( SELECT ISNULL(SiteRef,'') FROM ZMX_Company ),@seq,@addendum) ";
        //VALUES ('{0}','{1}',@seq,@addendum) ";

        public static string InicializaRegistroCountryPack { get; set; } = @"SELECT 1";

        //{0}=serie,{1}=folio
        public static string ValidaVoucherLog { get; set; } = @"IF EXISTS(SELECT top  1 serie, folio FROM ZMX_VoucherLog WITH (NOLOCK) WHERE SiteRef='{2}' AND serie='{0}' AND folio='{1}')
SELECT '1' AS Exist
ELSE 
SELECT '0' AS Exist ";

        //{0}= SiteRef,{1}=StatusId,{2}=Message,{3}=voucher,{4}=serie,{5}=folio
        public static string InsertVoucherLog { get; set; } = @"INSERT INTO ZMX_VoucherLog WITH(ROWLOCK) (SiteRef,voucherStatusId,Message,registration,voucherId,serie,folio)
VALUES ('{0}','{1}','{2}',GETDATE(),'{3}','{4}','{5}') ";

        //{0}= SiteRef,{1}=StatusId,{2}=Message,{3}=voucher,{4}=serie,{5}=folio
        //Modificado por JL para quitar la actualizacion del voucherid
        public static string UpdateVoucherLog { get; set; } = @"UPDATE ZMX_VoucherLog WITH(ROWLOCK) SET SiteRef='{0}',voucherStatusId='{1}', Message = '{2}', registration = GETDATE(),voucherId='{3}' WHERE SiteRef='{0}' AND serie='{4}' AND folio='{5}' ";

        public static string InicializaRegistroERP { get; set; } = @"SELECT 1";

        public static string ValidaVoucherMXEIRQ { get; set; } = @"IF EXISTS(SELECT TOP 1 V9CONO,V9SERIE FROM MXEIRQ WITH (NOLOCK) WHERE V9CONO='{0}' AND V9SERIE='{1}' AND V9FOLIO= '{2}')
SELECT '1' AS Exist
ELSE 
SELECT '0' AS Exist";

        //{0}=Cono,{1}=Serie,{2}=Folio,{3}=Message,{4}=INLS,{5}=ENDS,{6}=STS
        public static string InsertVoucherMXEIRQ { get; set; } = @"INSERT INTO MXEIRQ WITH(ROWLOCK)(V9DTTM,V9CONO,V9SERIE,V9FOLIO,V9ERRD,V9INLS,V9ENDS,V9STS)
Values(GETDATE(),'{0}','{1}','{2}','{3}','{4}','{5}','{6}') ";

        //{0}=Cono,{1}=Serie,{2}=Folio,{3}=Message,{4}=INLS,{5}=ENDS,{6}=STS
        public static string UpdateVoucherMXEIRQ { get; set; } = @"UPDATE MXEIRQ WITH(ROWLOCK) SET V9DTTM =GETDATE(),  V9ERRD = '{3}' , V9INLS = '{4}' ,V9ENDS = '{5}',V9STS = '{6}'
WHERE V9CONO = '{0}' AND V9SERIE = '{1}' AND V9FOLIO = '{2}' ";

        public static string Rollback { get; set; } = @"DELETE ZMX_CfdiRelated WHERE voucherId = '{0}' ;
DELETE ZMX_PaymentDocRelated FROM ZMX_PaymentDocRelated dr INNER JOIN ZMX_payment p ON p.paymentId=dr.paymentId
WHERE p.voucherId='{0}' ;
DELETE ZMX_payment WHERE voucherId='{0}' ;
DELETE ZMX_payments WHERE voucherId='{0}' ;
DELETE ZMX_ComplFTEspecificDescription WHERE voucherId = '{0}' ; 
                                                        DELETE ZMX_ComplementFT_Detail WHERE voucherId = '{0}' ;
                                                        DELETE ZMX_Complement_Foreignatrade WHERE voucherId = '{0}' ; 
                                                        DELETE ZMX_Legends WHERE voucherId = '{0}' ; 
                                                        DELETE ZMX_FiscalLegends WHERE voucherId = '{0}' ;
                                                        DELETE ZMX_Complement WHERE voucherId = '{0}' ; 
                                                        DELETE ZMX_VoucherAddendum WHERE voucherId = '{0}' ;
                                                        DELETE ZMX_CustomsInformation WHERE conceptId IN ( {0} );
                                                        DELETE ZMX_TaxConcept WHERE conceptId IN ( {1} ) ;
                                                        DELETE ZMX_Concept WHERE voucherId = '{0}' ;
                                                        DELETE ZMX_Receiver WHERE voucherId = '{0}' ;
                                                        DELETE ZMX_Emitter WHERE  voucherId = '{0}' ;
                                                        DELETE ZMX_Voucher WHERE voucherId = '{0}' ;                                                       
                                                        DELETE ZMX_MXEICVDA WHERE CVDETALLID IN (SELECT CVDETALLEID FROM ZMX_MXEICVD WHERE VOUCHERID='{0}');
        DELETE ZMX_MXEICVCOMP WHERE VOUCHERID = '{0}';
        DELETE ZMX_MXEICVD WHERE VOUCHERID = '{0}';
        DELETE ZMX_MXEICV WHERE VOUCHERID = '{0}'; ";

        public static string SelectInvoiceType { get; set; } = @"IF EXISTS(SELECT TOP 1  V0CONO , V0SERIE FROM MXEIHD WITH (NOLOCK) WHERE V0CONO='{0}' AND V0SERIE='{1}' AND V0FOLIO='{2}')
	SELECT UPPER(SUBSTRING(V0TCOMP,1,1)) AS TipoComprobante FROM MXEIHD WITH (NOLOCK) WHERE V0CONO='{0}' AND V0SERIE='{1}' AND V0FOLIO='{2}' ";
        //(!string.IsNullOrEmpty(GlobalStrings.ParamsPaymentSelect) ? GlobalStrings.ParamsPaymentSelect.Remove(GlobalStrings.ParamsPaymentSelect.Length - 1, 1) : GlobalStrings.ParamsPaymentSelect)
        public static string SelectPaymentsInvoice { get; set; } = @" IF EXISTS(SELECT 1 FROM sys.columns  WHERE Name = N'VQUUT1' AND Object_ID = Object_ID(N'MXEIPY'))
BEGIN
EXECUTE sp_executesql N'SELECT UPPER(SUBSTRING(V0TCOMP,1,1)) AS VQTCOMP,UPPER(V0CLIEN) AS VQCLIEN,  ISNULL(VQCONO,'''') AS VQCONO,  
  ISNULL(VQSERIE,'''') AS VQSERIE,  ISNULL(VQFOLIO,'''') AS VQFOLIO,  
  ISNULL(VQSEQN,0) AS VQSEQN,  ISNULL(VQPDAT,'''') AS VQPDAT,  
  ISNULL(VQSTMT,'''') AS VQSTMT,  ISNULL(VQPCUR,'''') AS VQPCUR,  ISNULL(VQEXRT,0) AS VQEXRT,  ISNULL(VQPAMT,0) AS VQPAMT,
  ISNULL(VQPMOP,'''') AS VQPMOP,  ISNULL(VQDACT,'''') AS VQDACT,
  ISNULL(VRCONO,'''') AS VRCONO,  ISNULL(VRSERIE,'''') AS VRSERIE,  ISNULL(VRFOLIO,'''') AS VRFOLIO,  
  ISNULL(VRSEQN,0) AS VRSEQN,  ISNULL(VRSEQD,0) AS VRSEQD,  ISNULL(VRDCID,'''') AS VRDCID,
  ISNULL(VRDSER,'''') AS VRDSER,  ISNULL(VRDFOL,'''') AS VRDFOL,  ISNULL(VRDCUR,'''') AS VRDCUR,  
  ISNULL(VREXRT,0) AS VREXRT,  ISNULL(VRMTPG,'''') AS VRMTPG,  ISNULL(VRNCUO,0) AS VRNCUO,  
  ISNULL(VRPSDO,0) AS VRPSDO,  ISNULL(VRMPAG,0) AS VRMPAG,  ISNULL(VRNSDO,0) AS VRNSDO,
  ISNULL(VQUUT1,'''') AS VQUUT1,CONVERT (datetime,(CASE WHEN isnull(V0DATE ,'''') = ''19000101'' THEN convert(varchar(10),GETDATE() ,103) ELSE convert(varchar(10), V0DATE, 103) END),103) AS V0DATE {3}
  FROM MXEIHD WITH (NOLOCK)  
  LEFT JOIN MXEIPY WITH (NOLOCK) ON V0CONO=VQCONO AND V0SERIE=VQSERIE AND V0FOLIO=VQFOLIO 
  LEFT JOIN MXEIPX WITH (NOLOCK) ON V0CONO=VRCONO AND V0SERIE=VRSERIE AND V0FOLIO=VRFOLIO 
  WHERE V0CONO=''{0}'' AND V0SERIE=''{1}'' AND V0FOLIO=''{2}'' '
  END
  ELSE
  BEGIN
  EXECUTE sp_executesql N'SELECT UPPER(SUBSTRING(V0TCOMP,1,1)) AS VQTCOMP,UPPER(V0CLIEN) AS VQCLIEN,  ISNULL(VQCONO,'''') AS VQCONO,  
  ISNULL(VQSERIE,'''') AS VQSERIE,  ISNULL(VQFOLIO,'''') AS VQFOLIO,  
  ISNULL(VQSEQN,0) AS VQSEQN,  ISNULL(VQPDAT,'''') AS VQPDAT,  
  ISNULL(VQSTMT,'''') AS VQSTMT,  ISNULL(VQPCUR,'''') AS VQPCUR,  ISNULL(VQEXRT,0) AS VQEXRT,  ISNULL(VQPAMT,0) AS VQPAMT,
  ISNULL(VQPMOP,'''') AS VQPMOP,  ISNULL(VQDACT,'''') AS VQDACT,
  ISNULL(VRCONO,'''') AS VRCONO,  ISNULL(VRSERIE,'''') AS VRSERIE,  ISNULL(VRFOLIO,'''') AS VRFOLIO,  
  ISNULL(VRSEQN,0) AS VRSEQN,  ISNULL(VRSEQD,0) AS VRSEQD,  ISNULL(VRDCID,'''') AS VRDCID,
  ISNULL(VRDSER,'''') AS VRDSER,  ISNULL(VRDFOL,'''') AS VRDFOL,  ISNULL(VRDCUR,'''') AS VRDCUR,  
  ISNULL(VREXRT,0) AS VREXRT,  ISNULL(VRMTPG,'''') AS VRMTPG,  ISNULL(VRNCUO,0) AS VRNCUO,  
  ISNULL(VRPSDO,0) AS VRPSDO,  ISNULL(VRMPAG,0) AS VRMPAG,  ISNULL(VRNSDO,0) AS VRNSDO,
  CONVERT (datetime,(CASE WHEN isnull(V0DATE ,'''') = ''19000101'' THEN convert(varchar(10),GETDATE() ,103) ELSE convert(varchar(10), V0DATE, 103) END),103) AS V0DATE {3}
  FROM MXEIHD WITH (NOLOCK)  
  LEFT JOIN MXEIPY WITH (NOLOCK) ON V0CONO=VQCONO AND V0SERIE=VQSERIE AND V0FOLIO=VQFOLIO 
  LEFT JOIN MXEIPX WITH (NOLOCK) ON V0CONO=VRCONO AND V0SERIE=VRSERIE AND V0FOLIO=VRFOLIO 
  WHERE V0CONO=''{0}'' AND V0SERIE=''{1}'' AND V0FOLIO=''{2}'' ' 
  END ";
        //ISNULL(V0DATE,'''') 
        public static string ValidateCol { get; set; }
        //Agregado por JL para la valiacion de exietncia las columnas  CAST(ISNULL(NULLIF(@V0DATE,''), GETDATE())AS DATETIME)

        public static string InsertPaymentsInvoice { get; set; } = @"IF NOT EXISTS(SELECT top 1 serie,folio  FROM ZMX_Voucher WITH (NOLOCK) WHERE  SiteRef='{4}' AND  serie = '{1}' AND folio = '{2}')
  INSERT INTO ZMX_Voucher WITH(ROWLOCK) (SiteRef, version, serie, folio, date, numberCertificate, certificate, subTotal, total, currency, expeditionPlace, voucherType, voucherId, customer, relationType, siteERP, Uf_pathCFDI)
  VALUES('{4}','3.3','{1}','{2}', ISNULL(@V0DATE, convert(varchar(10),GETDATE() ,103)) ,'','','0','0','XXX','','P',NEWID(),@VQCLIEN,'04','{0}','{3}')
  ELSE UPDATE ZMX_Voucher WITH(ROWLOCK) SET SiteRef='{4}', version = '3.3', currency = 'XXX', voucherType = 'P', customer = @VQCLIEN, relationType = '04', siteERP = '{0}', Uf_pathCFDI = '{3}'
  WHERE  SiteRef='{4}' AND serie = '{1}' AND folio = '{2}'
  IF EXISTS( SELECT top 1 VQSERIE,VQFOLIO FROM ZMX_MXEIPY WITH (NOLOCK) WHERE VQCONO='{0}' AND VQSERIE='{1}' AND VQFOLIO='{2}' AND VQSEQN=@VQSEQN)
  UPDATE ZMX_MXEIPY WITH(ROWLOCK) SET VQTCOMP=@VQTCOMP,VQCLIEN=@VQCLIEN,VQSEQN=@VQSEQN,VQPDAT=@VQPDAT,VQSTMT=@VQSTMT,VQPCUR=@VQPCUR,VQEXRT=@VQEXRT,VQPAMT=@VQPAMT,VQPMOP=@VQPMOP,VQDACT=@VQDACT
  WHERE VQCONO='{0}' AND VQSERIE='{1}' AND VQFOLIO='{2}' AND VQSEQN=@VQSEQN
  ELSE
  INSERT INTO ZMX_MXEIPY WITH(ROWLOCK) (VQTCOMP,VQCLIEN,VQCONO,VQSERIE,VQFOLIO,VQSEQN,VQPDAT,VQSTMT,VQPCUR,VQEXRT,VQPAMT,VQPMOP,VQDACT)
  VALUES(@VQTCOMP,@VQCLIEN,@VQCONO,@VQSERIE,@VQFOLIO,@VQSEQN,@VQPDAT,@VQSTMT,@VQPCUR,@VQEXRT,@VQPAMT,@VQPMOP,@VQDACT) 
  IF EXISTS( SELECT top 1 VRCONO,VRSERIE,VRFOLIO FROM ZMX_MXEIPX WITH (NOLOCK) WHERE VRCONO='{0}' AND VRSERIE='{1}' AND VRFOLIO='{2}' AND VRSEQN=@VRSEQN AND VRSEQD=@VRSEQD )
  UPDATE ZMX_MXEIPX WITH(ROWLOCK) SET VRSEQN=@VRSEQN,VRSEQD=@VRSEQD,VRDCID=@VRDCID,VRDSER=@VRDSER,VRDFOL=@VRDFOL,VRDCUR=@VRDCUR,VREXRT=@VREXRT,VRMTPG=@VRMTPG,VRNCUO=@VRNCUO,VRPSDO=@VRPSDO,VRMPAG=@VRMPAG,VRNSDO=@VRNSDO
  WHERE VRCONO='{0}' AND VRSERIE='{1}' AND VRFOLIO='{2}' AND VRSEQN=@VRSEQN AND VRSEQD=@VRSEQD
  ELSE
  INSERT INTO ZMX_MXEIPX WITH(ROWLOCK) (VRCONO,VRSERIE,VRFOLIO,VRSEQN,VRSEQD,VRDCID,VRDSER,VRDFOL,VRDCUR,VREXRT,VRMTPG,VRNCUO,VRPSDO,VRMPAG,VRNSDO)
  VALUES(@VRCONO,@VRSERIE,@VRFOLIO,@VRSEQN,@VRSEQD,@VRDCID,@VRDSER,@VRDFOL,@VRDCUR,@VREXRT,@VRMTPG,@VRNCUO,@VRPSDO,@VRMPAG,@VRNSDO) ";



        public static string InsertPaymentsInvoiceWhitCol { get; set; } = @"IF NOT EXISTS(SELECT top 1 serie,folio  FROM ZMX_Voucher WITH (NOLOCK) WHERE  SiteRef='{4}' AND  serie = '{1}' AND folio = '{2}')
        INSERT INTO ZMX_Voucher WITH(ROWLOCK) (SiteRef, version, serie, folio, date, numberCertificate, certificate, subTotal, total, currency, expeditionPlace, voucherType, voucherId, customer, relationType, siteERP, Uf_pathCFDI {5})
        VALUES('{4}','3.3','{1}','{2}', ISNULL(@V0DATE, convert(varchar(10),GETDATE() ,103)) ,'','','0','0','XXX','','P',NEWID(),@VQCLIEN,'04','{0}','{3}' {6})
        ELSE UPDATE ZMX_Voucher WITH(ROWLOCK) SET SiteRef='{4}', version = '3.3', currency = 'XXX', voucherType = 'P', customer = @VQCLIEN, relationType = '04', siteERP = '{0}', Uf_pathCFDI = '{3}' {7} WHERE  SiteRef='{4}' AND serie = '{1}' AND folio = '{2}' 
        IF EXISTS( SELECT top 1 VQSERIE,VQFOLIO FROM ZMX_MXEIPY WITH (NOLOCK) WHERE VQCONO='{0}' AND VQSERIE='{1}' AND VQFOLIO='{2}' AND VQSEQN=@VQSEQN)
        UPDATE ZMX_MXEIPY WITH(ROWLOCK) SET VQTCOMP=@VQTCOMP,VQCLIEN=@VQCLIEN,VQSEQN=@VQSEQN,VQPDAT=@VQPDAT,VQSTMT=@VQSTMT,VQPCUR=@VQPCUR,VQEXRT=@VQEXRT,VQPAMT=@VQPAMT,VQPMOP=@VQPMOP,VQDACT=@VQDACT,VQUUT1=@VQUUT1
        WHERE VQCONO='{0}' AND VQSERIE='{1}' AND VQFOLIO='{2}' AND VQSEQN=@VQSEQN
        ELSE
        INSERT INTO ZMX_MXEIPY WITH(ROWLOCK) (VQTCOMP,VQCLIEN,VQCONO,VQSERIE,VQFOLIO,VQSEQN,VQPDAT,VQSTMT,VQPCUR,VQEXRT,VQPAMT,VQPMOP,VQDACT,VQUUT1)
        VALUES(@VQTCOMP,@VQCLIEN,@VQCONO,@VQSERIE,@VQFOLIO,@VQSEQN,@VQPDAT,@VQSTMT,@VQPCUR,@VQEXRT,@VQPAMT,@VQPMOP,@VQDACT,@VQUUT1) 
        IF EXISTS( SELECT top 1 VRCONO,VRSERIE,VRFOLIO FROM ZMX_MXEIPX WITH (NOLOCK) WHERE VRCONO='{0}' AND VRSERIE='{1}' AND VRFOLIO='{2}' AND VRSEQN=@VRSEQN AND VRSEQD=@VRSEQD )
        UPDATE ZMX_MXEIPX WITH(ROWLOCK) SET VRSEQN=@VRSEQN,VRSEQD=@VRSEQD,VRDCID=@VRDCID,VRDSER=@VRDSER,VRDFOL=@VRDFOL,VRDCUR=@VRDCUR,VREXRT=@VREXRT,VRMTPG=@VRMTPG,VRNCUO=@VRNCUO,VRPSDO=@VRPSDO,VRMPAG=@VRMPAG,VRNSDO=@VRNSDO
        WHERE VRCONO='{0}' AND VRSERIE='{1}' AND VRFOLIO='{2}' AND VRSEQN=@VRSEQN AND VRSEQD=@VRSEQD
        ELSE
        INSERT INTO ZMX_MXEIPX WITH(ROWLOCK) (VRCONO,VRSERIE,VRFOLIO,VRSEQN,VRSEQD,VRDCID,VRDSER,VRDFOL,VRDCUR,VREXRT,VRMTPG,VRNCUO,VRPSDO,VRMPAG,VRNSDO)
        VALUES(@VRCONO,@VRSERIE,@VRFOLIO,@VRSEQN,@VRSEQD,@VRDCID,@VRDSER,@VRDFOL,@VRDCUR,@VREXRT,@VRMTPG,@VRNCUO,@VRPSDO,@VRMPAG,@VRNSDO)";
        

        public static string DeleteComments { get; set; } = @"Delete ZMX_Comment WHERE company_id='{0}' AND voucher_prefix='{1}' AND voucher_num='{2}' ";

        public static string SelectComments { get; set; } = @"IF EXISTS (SELECT TOP 1 V7SERIE,V7FOLIO FROM MXEICO WITH (NOLOCK) WHERE V7CONO='{0}' AND V7SERIE='{1}' AND V7FOLIO='{2}' )
SELECT '1' AS Exist,V7SEQN AS detail_seq,V7SEQC AS comment_seq,ISNULL(V7COMT,'') AS comment 
FROM MXEICO WITH (NOLOCK) WHERE V7CONO='{0}' AND V7SERIE='{1}' AND V7FOLIO='{2}' 
ELSE
SELECT '0' AS Exist , '' AS detail_seq,'' AS comment_seq,'' AS comment";

        public static string InsertComments { get; set; } = @"IF (@Exist = '1')
BEGIN
	--DELETE ZMX_Comment WHERE company_id='{0}' AND voucher_prefix='{1}' AND voucher_num='{2}' AND detail_seq=@detail_seq AND comment_seq=@comment_seq ;
	INSERT INTO ZMX_Comment WITH(ROWLOCK) (SiteRef,company_id,voucher_prefix,voucher_num,detail_seq,comment_seq,comment)
	VALUES('{3}','{0}','{1}','{2}',@detail_seq,@comment_seq,@comment)
   -- DELETE ZMX_Comment WHERE company_id='{0}' AND voucher_prefix='{1}' AND voucher_num='{2}' AND detail_seq=@detail_seq AND comment_seq=@comment_seq + 1 ;
END
";
        //Se agrega el campo VJIIMP-RAK 300119
        public static string SelectCfdiRelacionados { get; set; } = @"IF EXISTS(SELECT 1 FROM sys.columns  WHERE Name = N'VJIIMP' AND Object_ID = Object_ID(N'MXEIRC'))

BEGIN


EXECUTE sp_executesql N'IF EXISTS (SELECT TOP 1 VJCONO,VJSERIE FROM MXEIRC WITH (NOLOCK) WHERE VJCONO=''{0}'' AND VJSERIE=''{1}'' AND VJFOLIO=''{2}'' )
BEGIN
SELECT ''1'' AS Exist, ISNULL(VJISERIE,'''') AS VJISERIE,ISNULL(VJIFOLIO,'''') AS VJIFOLIO,ISNULL(VJCONO,'''') AS VJCONO, ISNULL(VJSEQN,0) AS VJSEQN, ISNULL(VJIIMP,0) AS VJIIMP FROM MXEIRC WITH (NOLOCK)
WHERE VJCONO=''{0}'' AND VJSERIE=''{1}'' AND VJFOLIO=''{2}''

END
ELSE

SELECT ''0'' AS Exist ,'''' AS VJISERIE, '''' AS VJIFOLIO, '''' AS VJCONO, 0 AS VJSEQN, 0 AS VJIIMP '

END 

ELSE

BEGIN

EXECUTE sp_executesql N'IF EXISTS (SELECT TOP 1 VJCONO,VJSERIE FROM MXEIRC WITH (NOLOCK) WHERE VJCONO=''{0}'' AND VJSERIE=''{1}'' AND VJFOLIO=''{2}'' )
BEGIN

SELECT ''1'' AS Exist, ISNULL(VJISERIE,'''') AS VJISERIE,ISNULL(VJIFOLIO,'''') AS VJIFOLIO,ISNULL(VJCONO,'''') AS VJCONO, ISNULL(VJSEQN,0) AS VJSEQN FROM MXEIRC WITH (NOLOCK)
WHERE VJCONO=''{0}'' AND VJSERIE=''{1}'' AND VJFOLIO=''{2}''

END

ELSE
SELECT ''0'' AS Exist ,'''' AS VJISERIE, '''' AS VJIFOLIO, '''' AS VJCONO, 0 AS VJSEQN'
END";

        

        public static string InsertCfdiRelacionados { get; set; } = @"IF (@Exist = '1')
BEGIN
	DELETE ZMX_CfdiRelated WHERE voucherId='{0}' AND serie=ISNULL(@VJISERIE,'') AND folio=@VJIFOLIO ;
	INSERT INTO ZMX_CfdiRelated WITH(ROWLOCK) (SiteRef,voucherId,uuid,serie,folio,ERPsites)
	SELECT '{1}','{0}',(SELECT ISNULL(uuid,'00000000-0000-0000-0000-000000000000') FROM  ZMX_Voucher v WITH (NOLOCK) 
    INNER JOIN ZMX_FiscalStamp f  WITH (NOLOCK)ON f.voucherId=v.voucherId
WHERE v.serie=@VJISERIE AND v.folio=@VJIFOLIO),ISNULL(@VJISERIE,''),@VJIFOLIO,@VJCONO 
END ";

        public static string InsertCfdiRelacionadosAmount { get; set; } = @"IF (@Exist = '1')
BEGIN
	DELETE ZMX_CfdiRelated WHERE voucherId='{0}' AND serie=ISNULL(@VJISERIE,'') AND folio=@VJIFOLIO ;
	INSERT INTO ZMX_CfdiRelated WITH(ROWLOCK) (SiteRef,voucherId,uuid,serie,folio,ERPsites,AMOUNT)
	SELECT '{1}','{0}',(SELECT ISNULL(uuid,'00000000-0000-0000-0000-000000000000') FROM  ZMX_Voucher v WITH (NOLOCK) 
    INNER JOIN ZMX_FiscalStamp f  WITH (NOLOCK)ON f.voucherId=v.voucherId
WHERE v.serie=@VJISERIE AND v.folio=@VJIFOLIO),ISNULL(@VJISERIE,''),@VJIFOLIO,@VJCONO ,isnull(@VJIIMP,0)
END ";

        public static string InsertCfdiRelacionadosPagos { get; set; } = @" IF (EXISTS (SELECT * 
                 FROM INFORMATION_SCHEMA.TABLES 
                 WHERE TABLE_SCHEMA = 'dbo' 
                 AND  TABLE_NAME = 'ZMX_MXEIRelatedPayment'))
  BEGIN  
IF (@Exist = '1')
BEGIN
	DELETE ZMX_MXEIRelatedPayment WHERE VJCONO=@VJCONO AND VJSERIE='{1}' AND VJFOLIO='{2}' AND VJSEQN=@VJSEQN AND VJISERIE=ISNULL(@VJISERIE,'') AND VJIFOLIO=ISNULL(@VJIFOLIO,'') ;
	INSERT INTO ZMX_MXEIRelatedPayment WITH(ROWLOCK) (VJCONO,VJSERIE,VJFOLIO,VJSEQN,VJISERIE,VJIFOLIO)
	VALUES ('{0}','{1}','{2}',ISNULL(@VJSEQN,''),ISNULL(@VJISERIE,''),ISNULL(@VJIFOLIO,'') )
END  
END";

        public static string TestConnection { get; set; } = @"SELECT 1 ";
//NEW-------------------------
        public static string SelectExistsPedimentos { get; set; } = @"IF EXISTS ( SELECT TOP 1 V4CONO,V4SERIE FROM MXEIPD WITH (NOLOCK) WHERE V4CONO='{0}' AND V4SERIE='{1}' AND V4FOLIO='{2}'  )
 SELECT '1' AS Exist ELSE SELECT '0' AS Exist ";

        public static string SelectPedimentos { get; set; } = @"IF EXISTS (SELECT TOP 1 V4CONO,V4SERIE FROM MXEIPD WITH (NOLOCK) WHERE V4CONO='{0}' AND V4SERIE='{1}' AND V4FOLIO='{2}' AND V4SEQN='{3}')
SELECT '1' AS Exist, V4PEDI AS pedimentNumber, ISNULL(V4UUT1,'') AS UFT1, ISNULL(V4UUD1,'') AS UFD1 FROM MXEIPD WITH (NOLOCK)
WHERE V4CONO='{0}' AND V4SERIE='{1}' AND V4FOLIO='{2}' AND V4SEQN='{3}' AND V4SEQT=0 
ORDER BY V4SEQP 
ELSE 
SELECT '0' AS Exist , '' AS pedimentNumber, '' AS UFT1, '' AS UFD1  ";

        public static string InsertPedimentos { get; set; } = @"IF (@Exist = '1')
BEGIN 
	INSERT INTO ZMX_CustomsInformation WITH(ROWLOCK) (SiteRef,pedimentNumber,conceptId,UFT1,UFD1)
	VALUES ('{1}',@pedimentNumber,'{0}',@UFT1,@UFD1)
END ";

        public static string DeletePedimentos { get; set; } = @"DELETE ZMX_CustomsInformation WHERE conceptId='{0}' ";

        public static string UpdateVoucherMXEIRQUUID { get; set; } = @"UPDATE MXEIRQ WITH(ROWLOCK) SET V9ERRD = '{3}', V9INLS = '{4}', V9ENDS = '{5}', V9STS = '{6}', V9UUID = '{7}', V9DATER = '{8}', V9NCERR = '{9}', V9FPCD = '{10}',V9CFDIREL='{11}'WHERE V9CONO = '{0}' AND V9SERIE = '{1}' AND V9FOLIO = '{2}' ";

        //Agregado por jl para actualizar en la hd para XA
        public static string UpdateVoucherMXEIHD { get; set; } = @" UPDATE " + ConnectionData.SchemaDefault + ".MXEIHD SET V0UUID = '{7}', V0DATER = '{8}', V0NCERR = '{9}', V0FPCD = '{10}'  " +
" WHERE V0CONO = '{0}' AND V0SERIE = '{1}' AND V0FOLIO = '{2}' ";

        public static string UpdateVoucherMXEIHDSL { get; set; } = @" UPDATE MXEIHD SET V0UUID = '{7}', V0DATER = '{8}', V0NCERR = '{9}', V0FPCD = '{10}', V0CDATE='{11}' " +
" WHERE V0CONO = '{0}' AND V0SERIE = '{1}' AND V0FOLIO = '{2}' ";
        //

        public static string SelectVerificaPOS { get; set; } = @"IF EXISTS ( SELECT TOP 1 V2CONO,V2SERIE FROM MXEIAD  WITH (NOLOCK) WHERE V2CONO='{0}' AND V2SERIE='{1}' AND V2FOLIO='{2}' AND V2UUT1 = 'POS')
 SELECT '1' AS Exist ELSE SELECT '0' AS Exist ";

        //Se Agreaga V0UUT1 para agregar RFC a ZMX_Receiver
        public static string SelectPOS { get; set; } = @"SELECT ISNULL(V0FPCD,'')V0FPCD,ISNULL(V0PWCD,'')V0PWCD,ISNULL(V0UUT1,'')V0UUT1,V2CONO,V2SERIE,V2FOLIO,
V2TREG,ISNULL(V2CALL,'') V2CALL,ISNULL(V2NEXT,'') V2NEXT,
ISNULL(V2NINT,'') V2NINT,ISNULL(V2COLO,'') V2COLO,ISNULL(V2LOCL,'') V2LOCL,ISNULL(V2REFR,'') V2REFR,
ISNULL(V2MUNI,'') V2MUNI,ISNULL(V2ESTA,'') V2ESTA,ISNULL(V2PAIS,'') V2PAIS,ISNULL(V2CPOS,'') V2CPOS,
ISNULL(V2LGEM,'') V2LGEM,ISNULL(V2TELEF,'') V2TELEF,ISNULL(V2UUT1,'') V2UUT1,
ISNULL(V2UUD1,'') V2UUD1,--ISNULL(V2CPLE,'') V2CPLE,ISNULL(V2CECR,'') V2CECR,
--ISNULL(V2CEIT,'') V2CEIT,ISNULL(V2CERF,'') V2CERF,ISNULL(V2CENM,'') V2CENM,
--ISNULL(V2CTID,'') V2CTID,ISNULL(V2STID,'') V2STID,ISNULL(V2DELG,'') V2DELG,
--ISNULL(V2LOCS,'') V2LOCS,ISNULL(V2CNID,'') V2CNID,ISNULL(V2USGE,'') 
ISNULL(V2USGE,'')V2USGE ,ISNULL(V2SEAD,0) V2SEAD 
FROM MXEIAD WITH (NOLOCK) LEFT JOIN MXEIHD WITH (NOLOCK) ON V2FOLIO=V0FOLIO AND V2SERIE=V0SERIE AND V2CONO=V0CONO
WHERE V2CONO='{0}' AND V2SERIE='{1}' AND V2FOLIO='{2}' -- AND V2TREG <> 'C' ";

        //WHERE V2CONO = '{0}' AND V2SERIE = '{1}' AND V2FOLIO = '{2}' AND V2TREG = 'S' ";
        public static string InsertPOS { get; set; } = @" IF( @V0UUT1 != '' AND  @V2TREG = 'S' AND @V2UUT1='POS')
BEGIN  UPDATE ZMX_Receiver  WITH(ROWLOCK) SET usecfdiId=@V2USGE, rfc=@V0UUT1 WHERE voucherId = '{0}' END
ELSE IF ( @V2UUT1='POS')BEGIN UPDATE ZMX_Receiver WITH(ROWLOCK) SET usecfdiId=@V2USGE  WHERE voucherId = '{0}' END;
IF( @V2TREG = 'R' AND @V2UUT1='POS' )
BEGIN UPDATE ZMX_Receiver  WITH(ROWLOCK) SET name=@V2LGEM WHERE voucherId = '{0}' END;
IF (@V2UUT1='POS')
BEGIN UPDATE ZMX_Voucher  WITH(ROWLOCK) SET paymentMethod=@V0FPCD , paymentWay=@V0PWCD WHERE voucherId ='{0}' END;
DELETE ZMX_MXEIAD WITH(ROWLOCK) WHERE V2CONO=@V2CONO AND V2SERIE=@V2SERIE AND V2FOLIO=@V2FOLIO AND V2TREG=@V2TREG;
INSERT INTO ZMX_MXEIAD  WITH(ROWLOCK) (V2CONO,V2SERIE,V2FOLIO,V2TREG,V2CALL,V2NEXT,V2NINT,V2COLO,V2LOCL,V2REFR,V2MUNI,V2ESTA,V2PAIS,V2CPOS,V2LGEM,V2TELEF,V2UUT1,V2UUD1,V2USGE,V2SEAD)
VALUES(@V2CONO,@V2SERIE,@V2FOLIO,@V2TREG,@V2CALL,@V2NEXT,@V2NINT,@V2COLO,@V2LOCL,@V2REFR,@V2MUNI,@V2ESTA,@V2PAIS,@V2CPOS,@V2LGEM,@V2TELEF,@V2UUT1,@V2UUD1,@V2USGE,@V2SEAD) ";


        //Querys para Complemento de impuestos locales
        public static string SelectExistsImpuestosLocales { get; set; } = @"IF EXISTS(SELECT TOP 1 VLCONO,VLSERIE FROM  MXEITXL MXEITXL WITH (NOLOCK) WHERE VLCONO= '{0}' AND VLSERIE = '{1}' AND VLFOLIO = '{2}' )
 SELECT '1' AS Exist ELSE SELECT '0' AS Exist ";

        public static string DeleteImpuestosLocales { get; set; } = @"DELETE ZMX_ComplementTaxLocal WHERE voucherId='{0}' ";

        public static string SelectImpuestosLocales { get; set; } = @"SELECT ISNULL(VLTXTY,'')TipoTax,ISNULL(VLIMPO,0)Importe,ISNULL(VLTASA,'')Tasa,ISNULL(VLDESC,'')Descripcion FROM MXEITXL WITH (NOLOCK) WHERE VLCONO= '{0}' AND VLSERIE = '{1}' AND VLFOLIO = '{2}'  ";

        public static string InsertImpuestosLocales { get; set; } = @"INSERT INTO ZMX_ComplementTaxLocal  WITH(ROWLOCK) (voucherId,TipoTax,Importe,Tasa,Descripcion)
VALUES('{0}',@TipoTax,@Importe,@Tasa,@Descripcion)";



        //Querys para Complemento detallista
        public static string SelectExistsDetallista { get; set; } = @"IF EXISTS(SELECT TOP 1 A0CONO,A0SERIE FROM MXADHD WITH (NOLOCK) WHERE A0CONO= '{0}' AND A0SERIE = '{1}' AND A0FOLIO = '{2}' )
 SELECT '1' AS Exist ELSE SELECT '0' AS Exist ";

        public static string SelectMXADHD { get; set; } = @"IF EXISTS (SELECT TOP 1 A0CONO,A0SERIE  FROM MXADHD WITH (NOLOCK) WHERE A0CONO='{0}' AND A0SERIE='{1}' AND A0FOLIO='{2}' )
SELECT '1' AS Exist,A0CONO,A0SERIE,A0FOLIO,ISNULL(A0TYPE,'')A0TYPE,ISNULL(A0CNVR,'')A0CNVR,ISNULL(A0DCVR,'')A0DCVR,
ISNULL(A0DCST,'')A0DCST,ISNULL(A0DDTE,'')A0DDTE,ISNULL(A0ENTY,'')A0ENTY,
ISNULL(A0UNID,'')A0UNID,ISNULL(A0BYER,'')A0BYER,ISNULL(A0CONT,'')A0CONT,
ISNULL(A0SELR,'')A0SELR,ISNULL(A0ASLR,'')A0ASLR,ISNULL(A0ASLT,'')A0ASLT,
ISNULL(A0PTEV,'')A0PTEV,ISNULL(A0PTRT,'')A0PTRT,ISNULL(A0NPTT,'')A0NPTT,
ISNULL(A0PTTP,'')A0PTTP,ISNULL(A0PTPV,0)A0PTPV,ISNULL(A0DSTY,'')A0DSTY,
ISNULL(A0DSPC,0)A0DSPC,ISNULL(A0TOTI,0)A0TOTI,ISNULL(A0TOBT,0)A0TOBT,ISNULL(A0TOAP,0)A0TOAP		
FROM MXADHD WITH (NOLOCK) WHERE A0CONO='{0}' AND A0SERIE='{1}' AND A0FOLIO='{2}' 
ELSE
SELECT '0' AS Exist , '' A0CONO,'' A0SERIE,'' A0FOLIO,'' A0TYPE,''A0CNVR,''A0DCVR,
'' A0DCST,'' A0DDTE,''A0ENTY,'' A0UNID,'' A0BYER,'' A0CONT,
'' A0SELR,'' A0ASLR,'' A0ASLT,
'' A0PTEV,'' A0PTRT,'' A0NPTT,
'' A0PTTP,0 A0PTPV,'' A0DSTY,
0 A0DSPC,0 A0TOTI,0 A0TOBT,0 A0TOAP	";

        public static string InsertMXADHD { get; set; } = @"IF (@Exist = '1')
BEGIN
DELETE ZMX_MXADHD WHERE  A0CONO=@A0CONO AND A0SERIE=@A0SERIE AND A0FOLIO=@A0FOLIO;
INSERT INTO ZMX_MXADHD  WITH(ROWLOCK) (A0CONO,A0SERIE,A0FOLIO,A0TYPE,A0CNVR,A0DCVR,
A0DCST,A0DDTE,A0ENTY,A0UNID,A0BYER,A0CONT,A0SELR,A0ASLR,A0ASLT,
A0PTEV,A0PTRT,A0NPTT,A0PTTP,A0PTPV,A0DSTY,A0DSPC,A0TOTI,A0TOBT,A0TOAP)
VALUES (@A0CONO,@A0SERIE,@A0FOLIO,@A0TYPE,@A0CNVR,@A0DCVR,
@A0DCST,@A0DDTE,@A0ENTY,@A0UNID,@A0BYER,@A0CONT,@A0SELR,@A0ASLR,@A0ASLT,
@A0PTEV,@A0PTRT,@A0NPTT,@A0PTTP,@A0PTPV,@A0DSTY,@A0DSPC,@A0TOTI,@A0TOBT,@A0TOAP) 
END ";

        public static string SelectMXADSI { get; set; } = @"IF EXISTS (SELECT top 1 A1SERIE FROM MXADSI WITH (NOLOCK) WHERE A1CONO='{0}' AND A1SERIE='{1}' AND A1FOLIO='{2}')
SELECT '1' AS Exist,A1CONO,A1SERIE,A1FOLIO,A1SEQ,ISNULL(A1CODE,'')A1CODE,ISNULL(A1TEXT,'')A1TEXT 
FROM MXADSI WITH (NOLOCK) WHERE A1CONO='{0}' AND A1SERIE='{1}' AND A1FOLIO='{2}' 
ELSE
SELECT '0' AS Exist,'' A1CONO,'' A1SERIE,'' A1FOLIO,'' A1SEQ,'' A1CODE,'' A1TEXT ";

        public static string InsertMXADSI { get; set; } = @"IF (@Exist = '1')
BEGIN
DELETE ZMX_MXADSI WHERE A1CONO=@A1CONO AND A1SERIE=@A1SERIE AND A1FOLIO=@A1FOLIO;
INSERT INTO ZMX_MXADSI  WITH(ROWLOCK) (A1CONO,A1SERIE,A1FOLIO,A1SEQ,A1CODE,A1TEXT) 
VALUES (@A1CONO,@A1SERIE,@A1FOLIO,@A1SEQ,@A1CODE,@A1TEXT) 
END ";

        public static string SelectMXADCU { get; set; } = @"IF EXISTS (SELECT TOP 1 A2CONO,A2SERIE FROM MXADCU WITH (NOLOCK) WHERE A2CONO='{0}' AND A2SERIE='{1}' AND A2FOLIO='{2}' )
SELECT '1' AS Exist, A2CONO,A2SERIE,A2FOLIO,A2SEQ,ISNULL(A2CODE,'')A2CODE,ISNULL(A2CUFN,'')A2CUFN,ISNULL(A2CURT,0)A2CURT  
FROM MXADCU WITH (NOLOCK) WHERE A2CONO='{0}' AND A2SERIE='{1}' AND A2FOLIO='{2}' 
ELSE 
SELECT '0' AS Exist,'' A2CONO,'' A2SERIE,'' A2FOLIO,'' A2SEQ, '' A2CODE,'' A2CUFN,0 A2CURT  ";

        public static string InsertMXADCU { get; set; } = @"IF (@Exist = '1')
BEGIN
DELETE ZMX_MXADCU WHERE A2CONO=@A2CONO AND A2SERIE=@A2SERIE AND A2FOLIO=@A2FOLIO ;
INSERT INTO ZMX_MXADCU  WITH(ROWLOCK) (A2CONO,A2SERIE,A2FOLIO,A2SEQ,A2CODE,A2CUFN,A2CURT )
VALUES (@A2CONO,@A2SERIE,@A2FOLIO,@A2SEQ,@A2CODE,@A2CUFN,@A2CURT )
END ";

        public static string SelectMXADPD { get; set; } = @"IF EXISTS (SELECT TOP 1 A3SERIE,A3CONO FROM MXADPD WITH (NOLOCK) WHERE A3CONO='{0}' AND A3SERIE='{1}' AND A3FOLIO='{2}' )
SELECT '1' AS Exist,A3CONO,A3SERIE,A3FOLIO,A3SEQN,A3SEQP,ISNULL(A3APGLN,'')A3APGLN,ISNULL(A3ALPT,'')A3ALPT,ISNULL(A3PEDI,'')A3PEDI,
ISNULL(A3PEDF,'')A3PEDF,ISNULL(A3ADNM,'')A3ADNM,ISNULL(A3ADCT,'')A3ADCT
FROM MXADPD WITH (NOLOCK) WHERE A3CONO='{0}' AND A3SERIE='{1}' AND A3FOLIO='{2}'
ELSE 
SELECT '0' AS Exist,'' A3CONO,'' A3SERIE,'' A3FOLIO,'' A3SEQN,'' A3SEQP,'' A3APGLN,'' A3ALPT,'' A3PEDI,
'' A3PEDF,'' A3ADNM,'' A3ADCT ";

        public static string InsertMXADPD { get; set; } = @"IF (@Exist = '1')
BEGIN
DELETE ZMX_MXADPD WHERE A3CONO=@A3CONO AND A3SERIE=@A3SERIE AND A3FOLIO=@A3FOLIO ;
INSERT INTO  ZMX_MXADPD  WITH(ROWLOCK)(A3CONO,A3SERIE,A3FOLIO,A3SEQN,A3SEQP,A3APGLN,A3ALPT, A3PEDI,A3PEDF,A3ADNM,A3ADCT )
VALUES(@A3CONO,@A3SERIE,@A3FOLIO,@A3SEQN,@A3SEQP,@A3APGLN,@A3ALPT,@A3PEDI,@A3PEDF,@A3ADNM,@A3ADCT )  
END ";

        public static string SelectMXADCG { get; set; } = @"IF EXISTS (SELECT TOP 1 A4CONO,A4SERIE FROM MXADCG WITH (NOLOCK) WHERE A4CONO='{0}' AND A4SERIE='{1}' AND A4FOLIO='{2}' )
SELECT '1' AS Exist,A4CONO,A4SERIE,A4FOLIO,A4SEQN,A4SEQ,A4SEQD,ISNULL(A4CGTY,'')A4CGTY,ISNULL(A4STLT,'')A4STLT,ISNULL(A4SSTY,'')A4SSTY,
ISNULL(A4SSBS,'')A4SSBS,ISNULL(A4SSPC,0)A4SSPC,ISNULL(A4SSRU,0)A4SSRU,ISNULL(A4SSAU,0)A4SSAU
FROM MXADCG WITH (NOLOCK) WHERE A4CONO='{0}' AND A4SERIE='{1}' AND A4FOLIO='{2}' 
ELSE 
SELECT '0' AS Exist,'' A4CONO,'' A4SERIE,'' A4FOLIO,'' A4SEQN,'' A4SEQ,'' A4SEQD,'' A4CGTY,'' A4STLT,'' A4SSTY,
'' A4SSBS, 0 A4SSPC,0 A4SSRU, 0 A4SSAU ";

        public static string InsertMXADCG { get; set; } = @"IF (@Exist = '1')
BEGIN
DELETE ZMX_MXADCG WHERE A4CONO=@A4CONO AND A4SERIE=@A4SERIE AND A4FOLIO=@A4FOLIO ;
INSERT INTO  ZMX_MXADCG  WITH(ROWLOCK)(A4CONO,A4SERIE,A4FOLIO,A4SEQN,A4SEQ,A4SEQD,A4CGTY,A4STLT,A4SSTY,A4SSBS,A4SSPC,A4SSRU,A4SSAU )
VALUES(@A4CONO,@A4SERIE,@A4FOLIO,@A4SEQN,@A4SEQ,@A4SEQD,@A4CGTY,@A4STLT,@A4SSTY,@A4SSBS,@A4SSPC,@A4SSRU,@A4SSAU )
END ";

        public static string SelectMXADDT { get; set; } = @"IF EXISTS (SELECT TOP 1 A5SERIE,A5CONO FROM MXADDT WITH (NOLOCK) WHERE A5CONO='{0}' AND A5SERIE='{1}' AND A5FOLIO='{2}' )
SELECT '1' AS Exist, A5CONO,A5SERIE,A5FOLIO,A5SEQN,ISNULL(A5TYPE,'')A5TYPE,ISNULL(A5CANT,0)A5CANT,ISNULL(A5UNMSR,'')A5UNMSR,
ISNULL(A5EAN,'')A5EAN,ISNULL(A5LANG,'')A5LANG,ISNULL(A5DESC,'')A5DESC,ISNULL(A5UNPR,0)A5UNPR,
ISNULL(A5UNPRLC,0)A5UNPRLC,ISNULL(A5NTPR,0)A5NTPR,ISNULL(A5NTPRLC,0)A5NTPRLC,ISNULL(A5GRAM,0)A5GRAM,
ISNULL(A5GRAMLC,0)A5GRAMLC,ISNULL(A5NTAM,0)A5NTAM,ISNULL(A5NTAMLC,0)A5NTAMLC,ISNULL(A5SSCC,'')A5SSCC,
ISNULL(A5SSCT,'')A5SSCT,ISNULL(A5PLQT,0)A5PLQT,ISNULL(A5PLDS,'')A5PLDS,ISNULL(A5PLTY,'')A5PLTY,ISNULL(A5TRPM,'')A5TRPM
FROM MXADDT WITH (NOLOCK) WHERE A5CONO='{0}' AND A5SERIE='{1}' AND A5FOLIO='{2}'
ELSE 
SELECT '0' AS Exist,'' A5CONO,'' A5SERIE,'' A5FOLIO,'' A5SEQN,'' A5TYPE,0 A5CANT,'' A5UNMSR,
'' A5EAN,'' A5LANG,'' A5DESC,0 A5UNPR, 0 A5UNPRLC,0 A5NTPR,0 A5NTPRLC,0 A5GRAM,
0 A5GRAMLC,0 A5NTAM,0 A5NTAMLC,'' A5SSCC,
'' A5SSCT,'' A5PLQT, '' A5PLDS,'' A5PLTY,'' A5TRPM  ";

        public static string InsertMXADDT { get; set; } = @"IF (@Exist = '1')
BEGIN
DELETE ZMX_MXADDT WHERE A5CONO=@A5CONO AND A5SERIE=@A5SERIE AND A5FOLIO=@A5FOLIO AND A5SEQN=@A5SEQN ;
INSERT INTO  ZMX_MXADDT  WITH(ROWLOCK)(A5CONO,A5SERIE,A5FOLIO,A5SEQN,A5TYPE,A5CANT,A5UNMSR,
A5EAN,A5LANG,A5DESC,A5UNPR,A5UNPRLC,A5NTPR,A5NTPRLC,A5GRAM,A5GRAMLC,A5NTAM,A5NTAMLC,A5SSCC,A5PLQT,A5PLDS,A5PLTY,A5TRPM ) 
VALUES(@A5CONO,@A5SERIE,@A5FOLIO,@A5SEQN,@A5TYPE,@A5CANT,@A5UNMSR,
@A5EAN,@A5LANG,@A5DESC,@A5UNPR,@A5UNPRLC,@A5NTPR,@A5NTPRLC,@A5GRAM,@A5GRAMLC,@A5NTAM,@A5NTAMLC,@A5SSCC,@A5PLQT,@A5PLDS,@A5PLTY,@A5TRPM ) 
END ";

        public static string SelectMXADRF { get; set; } = @"IF EXISTS (SELECT TOP 1 A6CONO,A6SERIE FROM MXADRF WITH (NOLOCK) WHERE A6CONO='{0}' AND A6SERIE='{1}' AND A6FOLIO='{2}' )
SELECT '1' AS Exist, A6CONO,A6SERIE,A6FOLIO,A6TYPE,A6SEQ,ISNULL(A6RFTY,'')A6RFTY,ISNULL(A6RFID,0)A6RFID,ISNULL(A6RFDT,'')A6RFDT
FROM MXADRF WITH (NOLOCK) WHERE A6CONO='{0}' AND A6SERIE='{1}' AND A6FOLIO='{2}'
ELSE 
SELECT '0' AS Exist,'' A6CONO,'' A6SERIE,'' A6FOLIO,'' A6TYPE,'' A6SEQ,'' A6RFTY,0 A6RFID,'' A6RFDT ";

        public static string InsertMXADRF { get; set; } = @"IF (@Exist = '1')
BEGIN
DELETE ZMX_MXADRF WHERE A6CONO=@A6CONO AND A6SERIE=@A6SERIE AND A6FOLIO=@A6FOLIO AND A6TYPE=@A6TYPE AND A6SEQ=@A6SEQ;
INSERT INTO  ZMX_MXADRF  WITH(ROWLOCK) (A6CONO,A6SERIE,A6FOLIO,A6TYPE,A6SEQ,A6RFTY,A6RFID,A6RFDT)
VALUES(@A6CONO,@A6SERIE,@A6FOLIO,@A6TYPE,@A6SEQ,@A6RFTY,@A6RFID,@A6RFDT) 
END ";

        public static string SelectMXADAD { get; set; } = @"IF EXISTS (SELECT TOP 1 A7SERIE,A7CONO FROM MXADAD WITH (NOLOCK) WHERE A7CONO='{0}' AND A7SERIE='{1}' AND A7FOLIO='{2}' )
SELECT '1' AS Exist,A7CONO,A7SERIE,A7FOLIO,A7TREG,ISNULL(A7GLN,'')A7GLN,ISNULL(A7NAME,'')A7NAME,ISNULL(A7STRT,'')A7STRT,
ISNULL(A7CITY,'')A7CITY,ISNULL(A7ZIPC,'')A7ZIPC,ISNULL(A7APID,'')A7APID,ISNULL(A7APTY,'')A7APTY
FROM MXADAD WITH (NOLOCK) WHERE A7CONO='{0}' AND A7SERIE='{1}' AND A7FOLIO='{2}'
ELSE
SELECT '0' AS Exist,'' A7CONO,'' A7SERIE,'' A7FOLIO,'' A7TREG,'' A7GLN,'' A7NAME,'' A7STRT,
'' A7CITY,'' A7ZIPC,'' A7APID,'' A7APTY ";

        public static string InsertMXADAD { get; set; } = @"IF (@Exist = '1')
BEGIN
DELETE ZMX_MXADAD WHERE A7CONO=@A7CONO AND A7SERIE=@A7SERIE AND A7FOLIO=@A7FOLIO ;
INSERT INTO ZMX_MXADAD  WITH(ROWLOCK) (A7CONO,A7SERIE,A7FOLIO,A7TREG,A7GLN,A7NAME,A7STRT,A7CITY,A7ZIPC,A7APID,A7APTY)
VALUES(@A7CONO,@A7SERIE,@A7FOLIO,@A7TREG,@A7GLN,@A7NAME,@A7STRT,@A7CITY,@A7ZIPC,@A7APID,@A7APTY )
END ";

        public static string SelectMXADII { get; set; } = @"IF EXISTS (SELECT TOP 1 A8CONO,A8SERIE FROM MXADII WITH (NOLOCK) WHERE A8CONO='{0}' AND A8SERIE='{1}' AND A8FOLIO='{2}' )
SELECT '1' AS Exist,A8CONO,A8SERIE,A8FOLIO,A8SEQN,A8SEQA,ISNULL(A8ALTID,'')A8ALTID,ISNULL(A8ALTTY,'')A8ALTTY
FROM MXADII WITH (NOLOCK) WHERE A8CONO='{0}' AND A8SERIE='{1}' AND A8FOLIO='{2}'
ELSE
SELECT '0' AS Exist,'' A8CONO,'' A8SERIE,'' A8FOLIO,'' A8SEQN,'' A8SEQA,'' A8ALTID,'' A8ALTTY ";

        public static string InsertMXADII { get; set; } = @"IF (@Exist = '1')
BEGIN
DELETE ZMX_MXADII WHERE A8CONO=@A8CONO AND A8SERIE=@A8SERIE AND A8FOLIO=@A8FOLIO AND A8SEQN=@A8SEQN AND A8SEQA=@A8SEQA ;
INSERT INTO ZMX_MXADII  WITH(ROWLOCK) (A8CONO,A8SERIE,A8FOLIO,A8SEQN,A8SEQA,A8ALTID,A8ALTTY)
VALUES(@A8CONO,@A8SERIE,@A8FOLIO,@A8SEQN,@A8SEQA,@A8ALTID,@A8ALTTY )
END ";

        public static string SelectMXADIQ { get; set; } = @"IF EXISTS (SELECT TOP 1 A9CONO,A9SERIE FROM MXADIQ WITH (NOLOCK) WHERE A9CONO='{0}' AND A9SERIE='{1}' AND A9FOLIO='{2}' )
SELECT '1' AS Exist,A9CONO,A9SERIE,A9FOLIO,A9SEQN,A9SEQ,ISNULL(A9ADQT,0)A9ADQT,ISNULL(A9ADTY,'')A9ADTY
FROM MXADIQ WITH (NOLOCK) WHERE A9CONO='{0}' AND A9SERIE='{1}' AND A9FOLIO='{2}'
ELSE
SELECT '0' AS Exist,'' A9CONO,'' A9SERIE,'' A9FOLIO,'' A9SEQN,'' A9SEQ,0 A9ADQT,'' A9ADTY ";

        public static string InsertMXADIQ { get; set; } = @"IF (@Exist = '1')
BEGIN
DELETE ZMX_MXADIQ WHERE A9CONO=@A9CONO AND A9SERIE=@A9SERIE AND A9FOLIO=@A9FOLIO ;
INSERT INTO ZMX_MXADIQ  WITH(ROWLOCK) (A9CONO,A9SERIE,A9FOLIO,A9SEQN,A9SEQ,A9ADQT,A9ADTY)VALUES(@A9CONO,@A9SERIE,@A9FOLIO,@A9SEQN,@A9SEQ,@A9ADQT,@A9ADTY )
END ";

        public static string SelectMXADEA { get; set; } = @"IF EXISTS (SELECT TOP 1 AACONO,AASERIE FROM MXADEA WITH (NOLOCK) WHERE AACONO='{0}' AND AASERIE='{1}' AND AAFOLIO='{2}' )
SELECT '1' AS Exist,AACONO,AASERIE,AAFOLIO,AASEQN,AASEQEA,ISNULL(AALOT,'')AALOT,ISNULL(AAPDTE,'')AAPDTE
FROM MXADEA WITH (NOLOCK) WHERE AACONO='{0}' AND AASERIE='{1}' AND AAFOLIO='{2}'
ELSE
SELECT '0' AS Exist,'' AACONO,'' AASERIE,'' AAFOLIO,'' AASEQN,'' AASEQEA,'' AALOT,'' AAPDTE ";

        public static string InsertMXADEA { get; set; } = @"IF (@Exist = '1')
BEGIN
DELETE ZMX_MXADEA WHERE AACONO=@AACONO AND AASERIE=@AASERIE AND AAFOLIO=@AAFOLIO ;
INSERT INTO ZMX_MXADEA  WITH(ROWLOCK) (AACONO,AASERIE,AAFOLIO,AASEQN,AASEQEA,AALOT,AAPDTE)VALUES(@AACONO,@AASERIE,@AAFOLIO,@AASEQN,@AASEQEA,@AALOT,@AAPDTE) 
END ";

        public static string SelectMXADTX { get; set; } = @"IF EXISTS (SELECT TOP 1 ABCONO,ABSERIE FROM MXADTX WITH (NOLOCK) WHERE ABCONO='{0}' AND ABSERIE='{1}' AND ABFOLIO='{2}' )
SELECT '1' AS Exist,ABCONO,ABSERIE,ABFOLIO,ABSEQN,ABSEQT,ABTXTY,ISNULL(ABIMPU,'')ABIMPU,ISNULL(ABREFNO,'')ABREFNO,
ISNULL(ABTASA,0)ABTASA,ISNULL(ABIMPO,0)ABIMPO,ISNULL(ABIMPOLC,0)ABIMPOLC
FROM MXADTX WITH (NOLOCK) WHERE ABCONO='{0}' AND ABSERIE='{1}' AND ABFOLIO='{2}'
ELSE
SELECT '0' AS Exist,'' ABCONO,'' ABSERIE,'' ABFOLIO,'' ABSEQN,'' ABSEQT,'' ABTXTY,'' ABIMPU,'' ABREFNO,
'' ABTASA,'' ABIMPO,'' ABIMPOLC ";

        public static string InsertMXADTX { get; set; } = @"IF (@Exist = '1')
BEGIN
DELETE ZMX_MXADTX WHERE ABCONO=@ABCONO AND ABSERIE=@ABSERIE AND ABFOLIO=@ABFOLIO ;
INSERT INTO ZMX_MXADTX  WITH(ROWLOCK)(ABCONO,ABSERIE,ABFOLIO,ABSEQN,ABSEQT,ABTXTY,ABIMPU,ABREFNO,ABTASA,ABIMPO,ABIMPOLC)
VALUES(@ABCONO,@ABSERIE,@ABFOLIO,@ABSEQN,@ABSEQT,@ABTXTY,@ABIMPU,@ABREFNO,@ABTASA,@ABIMPO,@ABIMPOLC) 
END ";

        public static string SelectMXADXC { get; set; } = @"IF EXISTS (SELECT TOP 1 AECONO,AESERIE FROM MXADXC WITH (NOLOCK) WHERE AECONO='{0}' AND AESERIE='{1}' AND AEFOLIO='{2}' )
SELECT '1' AS Exist,AECONO,AESERIE,AEFOLIO,ISNULL(AECOID,'')AECOID
FROM MXADXC WITH (NOLOCK) WHERE AECONO='{0}' AND AESERIE='{1}' AND AEFOLIO='{2}'
ELSE
SELECT '0' AS Exist,'' AECONO,'' AESERIE,'' AEFOLIO,'' AECOID ";

        public static string InsertMXADXC { get; set; } = @"IF (@Exist = '1')
BEGIN
DELETE ZMX_MXADXC WHERE AECONO=@AECONO AND AESERIE=@AESERIE AND AEFOLIO=@AEFOLIO ;
INSERT INTO ZMX_MXADXC  WITH(ROWLOCK)(AECONO,AESERIE,AEFOLIO,AECOID)VALUES(@AECONO,@AESERIE,@AEFOLIO,@AECOID) 
END ";

    }
    class GlobalStringsXA  //====================================================================================================
    {
        public static string SelectrfcEmisorERP { get; set; } = @"SELECT RTRIM(V0RFCE) AS rfcEmisor,RTRIM(V0CLIEN) AS cliente
                                                                FROM MXEIHD WHERE V0CONO ='{0}' AND V0SERIE = '{1}' AND V0FOLIO = '{2}' ";

        //OJO se comento el exchangeRateUSD x que aun no se a creado --V0SUBT
        public static string SelectComprobante { get; set; } = @"SELECT  RTRIM(V0SERIE) AS serie, RTRIM(V0FOLIO) AS folio,V0DATE AS date,'' AS stamp,'' AS numberCertificate,'' AS certificate,V0CPAG AS payCondition,
V0SUBT AS SubTotal,0 AS discount,V0TOTL AS total,'' AS confirmation, LTRIM(RTRIM(V0CURR)) AS currency,V0EXRT AS exchangeRate,V0PONB AS purchaseOrder,
CASE WHEN (SELECT COUNT(*) FROM MXEIHD WHERE V0PODT='0001-01-01' AND V0CONO='{0}' AND V0SERIE='{1}' AND V0FOLIO='{2}') = 1 THEN ' '
ELSE (SELECT  (cast(V0PODT as char(200) ccsid 037))  FROM MXEIHD WHERE  V0CONO='{0}' AND V0SERIE='{1}' AND V0FOLIO='{2}') END AS PODate,
V0SLSR AS saleRepresentative,V0CLIEN AS customer,V0FOB AS freeLocation,V0NEMB AS shipment,V0GEMB AS billOfLanding, 
V0EMBX AS shipperVia,V0UUT1 AS UFT1,V0UUT2 AS UFT2,V0UUT3 AS UFT3,V0UUA1 AS UFAMT1,V0UUA2 AS UFAMT2,V0UUA3 AS UFAMT3,
CASE WHEN (SELECT COUNT(*) FROM MXEIHD WHERE V0UUD1='0001-01-01' AND V0CONO='{0}' AND V0SERIE='{1}' AND V0FOLIO='{2}') = 1 THEN ' '
ELSE (SELECT  (cast(V0UUD1 as char(200) ccsid 037))  FROM MXEIHD WHERE  V0CONO='{0}' AND V0SERIE='{1}' AND V0FOLIO='{2}') END AS UFD1,
CASE WHEN (SELECT COUNT(*) FROM MXEIHD WHERE V0UUD2='0001-01-01' AND V0CONO='{0}' AND V0SERIE='{1}' AND V0FOLIO='{2}') = 1 THEN ' '
ELSE (SELECT  (cast(V0UUD2 as char(200) ccsid 037))  FROM MXEIHD WHERE  V0CONO='{0}' AND V0SERIE='{1}' AND V0FOLIO='{2}') END AS UFD2,
CASE WHEN (SELECT COUNT(*) FROM MXEIHD WHERE V0UUD3='0001-01-01' AND V0CONO='{0}' AND V0SERIE='{1}' AND V0FOLIO='{2}') = 1 THEN ' '
ELSE (SELECT  (cast(V0UUD3 as char(200) ccsid 037))  FROM MXEIHD WHERE  V0CONO='{0}' AND V0SERIE='{1}' AND V0FOLIO='{2}') END AS UFD3,                              
V0PORV AS purchaseOrderRevision,'' AS relationType, '00000000-0000-0000-0000-000000000000' AS voucherId,UPPER(SUBSTRING(V0TCOMP,1,1)) AS voucherType,V0ORDNO AS customerOrder,COALESCE(V0EXRTU,0)exchangeRateUSD --1 AS exchangeRateUSD , IFNULL(V0EXRTU,0) AS exchangeRateUSD 
FROM MXEIHD WHERE V0CONO = '{0}' AND V0SERIE = '{1}' AND V0FOLIO = '{2}'  ";

        //Agregado por jl para el recalculo de los importes
        public static string SelectComprobante0 { get; set; } = @"SELECT  RTRIM(V0SERIE) AS serie, RTRIM(V0FOLIO) AS folio,V0DATE AS date,'' AS stamp,'' AS numberCertificate,'' AS certificate,V0CPAG AS payCondition,
0 AS SubTotal,0 AS discount,V0TOTL AS total,'' AS confirmation, LTRIM(RTRIM(V0CURR)) AS currency,V0EXRT AS exchangeRate,V0PONB AS purchaseOrder,
CASE WHEN (SELECT COUNT(*) FROM MXEIHD WHERE V0PODT='0001-01-01' AND V0CONO='{0}' AND V0SERIE='{1}' AND V0FOLIO='{2}') = 1 THEN ' '
ELSE (SELECT  (cast(V0PODT as char(200) ccsid 037))  FROM MXEIHD WHERE  V0CONO='{0}' AND V0SERIE='{1}' AND V0FOLIO='{2}') END AS PODate,
V0SLSR AS saleRepresentative,V0CLIEN AS customer,V0FOB AS freeLocation,V0NEMB AS shipment,V0GEMB AS billOfLanding, 
V0EMBX AS shipperVia,V0UUT1 AS UFT1,V0UUT2 AS UFT2,V0UUT3 AS UFT3,V0UUA1 AS UFAMT1,V0UUA2 AS UFAMT2,V0UUA3 AS UFAMT3,
CASE WHEN (SELECT COUNT(*) FROM MXEIHD WHERE V0UUD1='0001-01-01' AND V0CONO='{0}' AND V0SERIE='{1}' AND V0FOLIO='{2}') = 1 THEN ' '
ELSE (SELECT  (cast(V0UUD1 as char(200) ccsid 037))  FROM MXEIHD WHERE  V0CONO='{0}' AND V0SERIE='{1}' AND V0FOLIO='{2}') END AS UFD1,
CASE WHEN (SELECT COUNT(*) FROM MXEIHD WHERE V0UUD2='0001-01-01' AND V0CONO='{0}' AND V0SERIE='{1}' AND V0FOLIO='{2}') = 1 THEN ' '
ELSE (SELECT  (cast(V0UUD2 as char(200) ccsid 037))  FROM MXEIHD WHERE  V0CONO='{0}' AND V0SERIE='{1}' AND V0FOLIO='{2}') END AS UFD2,
CASE WHEN (SELECT COUNT(*) FROM MXEIHD WHERE V0UUD3='0001-01-01' AND V0CONO='{0}' AND V0SERIE='{1}' AND V0FOLIO='{2}') = 1 THEN ' '
ELSE (SELECT  (cast(V0UUD3 as char(200) ccsid 037))  FROM MXEIHD WHERE  V0CONO='{0}' AND V0SERIE='{1}' AND V0FOLIO='{2}') END AS UFD3,                              
V0PORV AS purchaseOrderRevision,'' AS relationType, '00000000-0000-0000-0000-000000000000' AS voucherId,UPPER(SUBSTRING(V0TCOMP,1,1)) AS voucherType,V0ORDNO AS customerOrder,COALESCE(V0EXRTU,0)exchangeRateUSD --1 AS exchangeRateUSD , IFNULL(V0EXRTU,0) AS exchangeRateUSD 
FROM MXEIHD WHERE V0CONO = '{0}' AND V0SERIE = '{1}' AND V0FOLIO = '{2}'  ";


        public static string SelectEmisorErp { get; set; } = @" SELECT V2UUT1 AS UFT1, 
CASE WHEN (SELECT COUNT(*) FROM MXEIAD WHERE V2UUD1='0001-01-01' AND V2TREG='C' AND V2CONO='{1}' AND V2SERIE='{2}'AND V2FOLIO='{3}' ) = 1 THEN ' '
ELSE (SELECT  (cast(V2UUD1 as char(200) ccsid 037))  FROM MXEIAD WHERE V2TREG='C' AND V2CONO='{1}' AND V2SERIE='{2}'AND V2FOLIO='{3}' ) END AS UFD1,
  '{0}' AS rfc FROM MXEIAD WHERE V2TREG='C' AND V2CONO='{1}' AND V2SERIE='{2}'AND V2FOLIO='{3}' ";

        /*{0}= rfcEmisor,{1}=Cono,{2}=Serie,{3}=Folio*/
        public static string SelectReceptorAddressErp { get; set; } = @"SELECT V2UUT1 AS UFT1, 
CASE WHEN (SELECT COUNT(*) FROM MXEIAD WHERE V2UUD1='0001-01-01' AND V2TREG='R' AND V2CONO='{1}' AND V2SERIE='{2}'AND V2FOLIO='{3}' ) = 1 THEN ' '
ELSE (SELECT  (cast(V2UUD1 as char(200) ccsid 037))  FROM MXEIAD WHERE V2TREG='R' AND V2CONO='{1}' AND V2SERIE='{2}'AND V2FOLIO='{3}' ) END AS UFD1,
'{0}' AS rfc, 0 AS address  --V2SEAD AS address 
FROM MXEIAD WHERE V2TREG='R' AND V2CONO='{1}' AND V2SERIE='{2}'AND V2FOLIO='{3}' ";

        /*{0}= rfcEmisor,{1}=Cono,{2}=Serie,{3}=Folio*/
        public static string SelectReceptorShiptopErp { get; set; } = @"SELECT 0 AS shipto, --V2SEAD AS shipto,
                                                                       '{0}' AS rfc FROM MXEIAD WHERE V2TREG='S' AND V2CONO='{1}' AND V2SERIE='{2}'AND V2FOLIO='{3}' ";

        public static string SelectNodoConcepto { get; set; } = @"SELECT V3CNID AS noIdentification,ABS(V3CANT) AS quantity,ABS(V3UNPR) AS unitvalue, ABS(V3DISC) AS discount,
                                                                '00000000-0000-0000-0000-000000000000' AS conceptId, V3DESC AS description,
                                                                V3UUT1 AS UFT1,V3UUT2 AS UFT2,V3UUT3 AS UFT3,V3UUA1 AS UFAMT1,V3UUA2 AS UFAMT2,V3UUA3 AS UFAMT3,V3SEQN AS sequence,
                                                                '' AS Accountpattern,COALESCE(V3UNMSR,'NULL') AS unitId FROM MXEIDT WHERE V3CONO='{0}' AND V3SERIE='{1}' AND V3FOLIO='{2}'  ";

        public static string SelectNodoConceptoUpdate { get; set; } = @"SELECT V3CNID AS noIdentification,ABS(V3CANT) AS quantity,ABS(V3UNPR) AS unitvalue, ABS(V3DISC) AS discount,
                                                                      CAST('{4}' AS uniqueidentifier) AS conceptId, V3DESC AS description,
                                                                        V3UUT1 AS UFT1,V3UUT2 AS UFT2,V3UUT3 AS UFT3,V3UUA1 AS UFAMT1,V3UUA2 AS UFAMT2,V3UUA3 AS UFAMT3,V3SEQN AS sequence,
                                                                        '' AS Accountpattern,COALESCE(V3UNMSR,'NULL') AS unitId FROM MXEIDT WHERE V3CONO='{0}' AND V3SERIE='{1}'AND V3FOLIO='{2}' AND V3SEQN= '{3}' ";
        //**************************IndustualNumber**************************//
        //--CASE WHEN  (SELECT COUNT(*) FROM MXEIDT WHERE (V3UUT3 IS NULL OR V3UUT3='' ) AND V3CONO='{0}' AND V3SERIE='{1}' AND V3FOLIO='{2}'  ) !=0 THEN V3CNID
        //--ELSE RTRIM(SUBSTR(V3UUT3,1,15)) END AS noIdentification
        public static string getItems_IndustrialNumber { get; set; } = @"SELECT 
CASE WHEN COALESCE(V3UUT3,'') = '' THEN V3CNID
ELSE RTRIM(SUBSTR(V3UUT3,1,15)) END AS noIdentification,
V0CLIEN AS clienteId,V3SEQN AS sequence,COALESCE(V3UNMSR,'NULL') AS unitId FROM MXEIDT
                            LEFT JOIN MXEIHD ON V0CONO =V3CONO AND V0SERIE = V3SERIE AND V0FOLIO = V3FOLIO 
                            WHERE V3CONO='{0}' AND V3SERIE='{1}' AND V3FOLIO='{2}' ";

        //BUG HUCHINSON SEAL
        public static string getItemsUpdate_IndustrialNumber { get; set; } = @"SELECT 
 V3CNID  AS noIdentification,
V0CLIEN AS clienteId,V3SEQN AS sequence,COALESCE(V3UNMSR,'NULL') AS unitId  FROM MXEIDT
                            LEFT JOIN MXEIHD ON V0CONO =V3CONO AND V0SERIE = V3SERIE AND V0FOLIO = V3FOLIO 
                            WHERE V3CONO='{0}' AND V3SERIE='{1}' AND V3FOLIO='{2}' ";


        //OJO se comento el V3DISC x que aun no se a creado
        public static string SelectConcepto_IndustrialNumber { get; set; } = @"SELECT V3CNID AS  industrialNumber,
CASE WHEN  (SELECT COUNT(*) FROM MXEIDT WHERE (V3UUT3 IS NULL OR V3UUT3='' ) AND V3CONO='{0}' AND V3SERIE='{1}' AND V3FOLIO='{2}' ) !=0 THEN V3CNID
ELSE RTRIM(SUBSTR(V3UUT3,1,15)) END AS noIdentification,
                                                                ABS(V3CANT) AS quantity,ABS(V3UNPR) AS unitvalue, ABS(V3DISC) AS discount,--0 AS discount,
                                                                '00000000-0000-0000-0000-000000000000' AS conceptId, V3DESC AS description,
                                                                V3UUT1 AS UFT1,V3UUT2 AS UFT2,V3UUT3 AS UFT3,V3UUA1 AS UFAMT1,V3UUA2 AS UFAMT2,V3UUA3 AS UFAMT3,V3SEQN AS sequence,
                                                                '' AS Accountpattern,COALESCE(V3UNMSR,'NULL') AS unitId FROM MXEIDT WHERE V3CONO='{0}' AND V3SERIE='{1}' AND V3FOLIO='{2}'  ";

        //OJO se comento el V3DISC x que aun no se a creado
        public static string SelectConceptoUpdate_IndustrialNumber { get; set; } = @"SELECT V3CNID AS  industrialNumber,
CASE WHEN  (SELECT COUNT(*) FROM MXEIDT WHERE (V3UUT3 IS NULL OR V3UUT3='' ) AND V3CONO='{0}' AND V3SERIE='{1}' AND V3FOLIO='{2}'  AND V3SEQN= '{3}' ) !=0 THEN V3CNID
ELSE RTRIM(SUBSTR(V3UUT3,1,15)) END AS noIdentification,
                                                                        ABS(V3CANT) AS quantity,ABS(V3UNPR) AS unitvalue, ABS(V3DISC) AS discount, --0 AS discount,
                                                                        CAST('{4}' as char(36) ccsid 037) AS conceptId, V3DESC AS description,
--CAST('{4}' AS uniqueidentifier) AS conceptId, V3DESC AS description,
                                                                        --'00000000-0000-0000-0000-000000000000' AS conceptId, V3DESC AS description,
                                                                        V3UUT1 AS UFT1,V3UUT2 AS UFT2,V3UUT3 AS UFT3,V3UUA1 AS UFAMT1,V3UUA2 AS UFAMT2,V3UUA3 AS UFAMT3,V3SEQN AS sequence,
                                                                        '' AS Accountpattern,COALESCE(V3UNMSR,'NULL') AS unitId FROM MXEIDT WHERE V3CONO='{0}' AND V3SERIE='{1}'AND V3FOLIO='{2}' AND V3SEQN= '{3}' ";

        public static string InsertNodoConcepto { get; set; } = @"Insert Into ZMX_concept (voucherId,SiteRef,prodServId,noIdentification,quantity,unitId,
unit,description,unitvalue,total,discount,conceptId,UFT1,UFT2,UFT3,
UFAMT1,UFAMT2,UFAMT3,sequence,Accountpattern)
VALUES('{0}','{1}',@prodServId,@industrialNumber,@quantity,@unitId,
@unit,@description,@unitvalue,@total,@discount,@conceptId,@UFT1,@UFT2,@UFT3,
@UFAMT1,@UFAMT2,@UFAMT3,@sequence,@Accountpattern) ";

        //{0}=voucherId,{1}=SiteRef,{2}=conceptId
        public static string UpdateNodoConcepto { get; set; } = @"IF EXISTS (SELECT * FROM ZMX_concept WHERE voucherId ='{0}' AND sequence=@sequence)     
UPDATE ZMX_concept SET SiteRef='{1}',prodServId=@prodServId,noIdentification=@industrialNumber,quantity=@quantity,unitId=@unitId,
unit=@unit,description=@description,unitvalue=@unitvalue,total=@total,discount=@discount,conceptId=@conceptId,UFT1=@UFT1,
UFT2=@UFT2,UFT3=@UFT3,UFAMT1=@UFAMT1,UFAMT2=@UFAMT2,UFAMT3=@UFAMT3,sequence=@sequence,Accountpattern=@Accountpattern,
WHERE voucherId = '{0}' AND  sequence=@sequence 
ELSE Insert Into ZMX_concept (voucherId,SiteRef,prodServId,noIdentification,quantity,unitId,
unit,description,unitvalue,total,discount,conceptId,UFT1,UFT2,UFT3,
UFAMT1,UFAMT2,UFAMT3,sequence,Accountpattern)
VALUES('{0}','{1}',@prodServId,@noIdentification,@quantity,@unitId,
@unit,@description,@unitvalue,@total,@discount,'{2}',@UFT1,@UFT2,@UFT3,
@UFAMT1,@UFAMT2,@UFAMT3,@sequence,@Accountpattern) ;
 --DELETE ZMX_TaxConcept WHERE conceptId ='{2}'";
        //*******************************************************************//

        public static string SelectImpuestosERP { get; set; } = @"SELECT LTRIM(RTRIM(V5IMPU)) AS impuestoERP,
 V5UUT1 AS UFT1,-- COALESCE(V5UUD1,'01/01/1753') UFD1,
CASE WHEN (SELECT COUNT(*) FROM MXEIHD WHERE V5UUD1='0001-01-01' AND V0CONO='{0}' AND V0SERIE='{1}' AND V0FOLIO='{2}') = 1 THEN ' '
ELSE (SELECT  (cast(V0UUD3 as char(200) ccsid 037))  FROM MXEIHD WHERE  V0CONO='{0}' AND V0SERIE='{1}' AND V0FOLIO='{2}') END AS UFD1, 
V5UUA1 AS UFAMT1, 
V5SEQN AS sequence ,
 '00000000-0000-0000-0000-000000000000' AS taxConceptId,
 COALESCE(V5IMPO,0) AS total 
FROM MXEITX INNER JOIN MXEIDT ON V3SEQN=V5SEQN AND V3CONO=V5CONO AND V3SERIE=V5SERIE AND V3FOLIO=V5FOLIO
WHERE V5CONO = '{0}' AND V5SERIE = '{1}' AND V5FOLIO = '{2}' ";
/*ELECT LTRIM(RTRIM(V5IMPU)) AS impuestoERP, V5UUT1 AS UFT1, 
CASE WHEN (SELECT COUNT(*) FROM MXEITX WHERE V5UUD1='0001-01-01' AND V5CONO = '{0}' AND V5SERIE = '{1}' AND V5FOLIO = '{2}') = 1 THEN ' '
ELSE (SELECT  (cast(V5UUD1 as char(200) ccsid 037))  FROM MXEITX WHERE V5CONO = '{0}' AND V5SERIE = '{1}' AND V5FOLIO = '{2}') END AS UFD1,
V5UUA1 AS UFAMT1, V5SEQN AS sequence , '00000000-0000-0000-0000-000000000000' AS taxConceptId  FROM MXEITX WHERE V5CONO = '{0}' AND V5SERIE = '{1}' AND V5FOLIO = '{2}' "; 
*/
        //V1CONO='{0}' V1SERIE='{1}' V1FOLIO='{2}' 
        public static string SelectExistsAddenda { get; set; } = @" SELECT  CASE WHEN (
 (SELECT 1 AS EXISTS  from sysibm.sysdummy1 WHERE EXISTS ( SELECT * FROM MXEIA1 WHERE V1CONO='{0}' AND V1SERIE='{1}' AND V1FOLIO='{2}'  ) ) =1
)
THEN ( select  '1' from sysibm.sysdummy1 ) 
ELSE  ( select  '0' from sysibm.sysdummy1 ) END AS Exist
 from sysibm.sysdummy1 WHERE EXISTS ( SELECT * FROM MXEIA1 ) ";

        public static string SelectAddenda { get; set; } = @"SELECT V1SEQA AS seq,V1ADDA AS addendum FROM MXEIA1 
WHERE V1CONO='{0}' AND V1SERIE='{1}' AND V1FOLIO='{2}' ";

        public static string InicializaRegistroERP { get; set; } = @"SELECT  '1' AS EXISTS FROM QGPL.MXEIRQ  FETCH FIRST 1 ROWS ONLY WITH UR "; //@"SELECT 1 AS EXISTS FROM sysibm.sysdummy1 ";

        //V9CONO='{0}' V9SERIE='{1}' V9FOLIO= '{2}'
        public static string ValidaVoucherMXEIRQ { get; set; } = @" SELECT  CASE WHEN (
 (SELECT 1 AS EXISTS  from sysibm.sysdummy1 WHERE EXISTS ( SELECT * FROM MXEIRQ WHERE V9CONO='{0}' AND V9SERIE='{1}' AND V9FOLIO='{2}'  AND VARCHAR_FORMAT (V9DTTM,'YYYY-MM-DD') = 'TIMESTAMP' ) )=1
)
THEN ( select  '1' from sysibm.sysdummy1 ) 
ELSE  ( select  '0' from sysibm.sysdummy1 ) END AS Exist
 from sysibm.sysdummy1 WHERE EXISTS ( SELECT * FROM MXEIRQ )"; //Falta ver si hay que considerar InsertVoucherMXEIRQ AND UPDATEVoucherMXEIRQ

        //{0}=Cono,{1}=Serie,{2}=Folio,{3}=Message,{4}=INLS,{5}=ENDS,{6}=STS
        public static string InsertVoucherMXEIRQ { get; set; } = @"INSERT INTO MXEIRQ(V9DTTM,V9CONO,V9SERIE,V9FOLIO,V9ERRD,V9INLS,V9ENDS,V9STS)
Values((SELECT CURRENT TIMESTAMP FROM sysibm.sysdummy1),'{0}','{1}','{2}','{3}','{4}','{5}','{6}') ";

        //{0}=Cono,{1}=Serie,{2}=Folio,{3}=Message,{4}=INLS,{5}=ENDS,{6}=STS  V9DTTM =(SELECT CURRENT TIMESTAMP FROM sysibm.sysdummy1), 
        public static string UpdateVoucherMXEIRQ { get; set; } = @"UPDATE MXEIRQ SET  V9ERRD = '{3}' , V9INLS = '{4}' ,V9ENDS = '{5}',V9STS = '{6}'
WHERE V9CONO = '{0}' AND V9SERIE = '{1}' AND V9FOLIO = '{2}' AND VARCHAR_FORMAT (V9DTTM,'YYYY-MM-DD') = 'TIMESTAMP' ";

        public static string UpdateVoucherMXEIRQUUID { get; set; } = @"UPDATE MXEIRQ SET V9ERRD = '{3}' , V9INLS = '{4}' ,V9ENDS = '{5}',V9STS = '{6}', V9UUID = '{7}', V9DATER = '{8}', V9NCERR = '{9}', V9FPCD = '{10}'
WHERE V9CONO = '{0}' AND V9SERIE =  '{1}' AND V9FOLIO = '{2}' AND VARCHAR_FORMAT (V9DTTM,'YYYY-MM-DD') = 'TIMESTAMP' ";

        //public static string UpdateVoucherMXEIHDUUID { get; set; } = @"UPDATE MXEIHD SET V0UUID = '{3}', V0FPCD = '{4}'
        //WHERE V0CONO = '{0}' AND V0SERIE =  '{1}' AND V0FOLIO = '{2}' ";


        //  "UPDATE MXEIRQ SET  V9ERRD = '{3}' , V9INLS = '{4}' ,V9ENDS = '{5}',V9STS = '{6}'
        //WHERE V9CONO = '{0}' AND V9SERIE = '{1}' AND V9FOLIO = '{2}' AND VARCHAR_FORMAT (V9DTTM,'YYYY-MM-DD') = 'TIMESTAMP' ;";

        //V0CONO='{0}' V0SERIE='{1}' V0FOLIO='{2}'
        public static string SelectInvoiceType { get; set; } = @"SELECT UPPER(SUBSTRING(V0TCOMP,1,1)) AS TipoComprobante FROM  MXEIHD  
    WHERE EXISTS ( SELECT * FROM MXEIHD WHERE V0CONO='{0}' AND V0SERIE='{1}' AND V0FOLIO='{2}'  )  
    AND  V0CONO='{0}' AND V0SERIE='{1}' AND V0FOLIO='{2}' ";

        public static string SelectPaymentsInvoice { get; set; } = @"SELECT UPPER(SUBSTRING(V0TCOMP,1,1)) AS VQTCOMP,V0CLIEN AS VQCLIEN,  VQCONO AS VQCONO,  
  COALESCE(VQSERIE,'') AS VQSERIE,  COALESCE(VQFOLIO,'') AS VQFOLIO,  
  COALESCE(VQSEQN,0) AS VQSEQN,  VQPDAT AS VQPDAT,  
  COALESCE(VQSTMT,'') AS VQSTMT,  COALESCE(VQPCUR,'') AS VQPCUR,  COALESCE(VQEXRT,0) AS VQEXRT,  COALESCE(VQPAMT,0) AS VQPAMT,
  COALESCE(VQPMOP,'') AS VQPMOP,  COALESCE(VQDACT,'') AS VQDACT,
  COALESCE(VRCONO,'') AS VRCONO,  COALESCE(VRSERIE,'') AS VRSERIE,  COALESCE(VRFOLIO,'') AS VRFOLIO,  
  COALESCE(VRSEQN,0) AS VRSEQN,  COALESCE(VRSEQD,0) AS VRSEQD,  COALESCE(VRDCID,'') AS VRDCID,
  COALESCE(VRDSER,'') AS VRDSER,  COALESCE(VRDFOL,'') AS VRDFOL,  COALESCE(VRDCUR,'') AS VRDCUR,  
  COALESCE(VREXRT,0) AS VREXRT,  COALESCE(VRMTPG,'') AS VRMTPG,  COALESCE(VRNCUO,0) AS VRNCUO,  
  COALESCE(VRPSDO,0) AS VRPSDO,  COALESCE(VRMPAG,0) AS VRMPAG,  COALESCE(VRNSDO,0) AS VRNSDO ,  
CASE WHEN (SELECT COUNT(*) FROM MXEIHD WHERE V0DATE=VARCHAR_FORMAT('0001-01-01','YYYY-MM-DD') AND V0CONO='{0}' AND V0SERIE='{1}' AND V0FOLIO='{2}') = 1 THEN (VARCHAR_FORMAT(V0DATE,'YYYY-MM-DD') )
ELSE (SELECT  (cast(V0DATE as char(200) ccsid 037))  FROM MXEIHD WHERE  V0CONO='{0}' AND V0SERIE='{1}' AND V0FOLIO='{2}') END AS V0DATE
  FROM MXEIHD  
  LEFT JOIN MXEIPY ON V0CONO=VQCONO AND V0SERIE=VQSERIE AND V0FOLIO=VQFOLIO 
  LEFT JOIN MXEIPX ON V0CONO=VRCONO AND V0SERIE=VRSERIE AND V0FOLIO=VRFOLIO 
  WHERE V0CONO='{0}' AND V0SERIE='{1}' AND V0FOLIO='{2}'  ";

        // V7CONO='{0}' V7SERIE='{1}' V7FOLIO='{2}'
        public static string SelectComments { get; set; } = @"SELECT '1' Exist,V7SEQN AS detail_seq,V7SEQC AS comment_seq, V7COMT AS comment 
FROM MXEICO WHERE V7CONO='{0}' AND V7SERIE='{1}' AND V7FOLIO='{2}' ";
        /*( SELECT '1' AS Exist,V7SEQN AS detail_seq,V7SEQC AS comment_seq,V7COMT AS comment  
        FROM MXEICO WHERE EXISTS ( SELECT * FROM MXEICO MC WHERE MC.V7CONO='{0}' AND MC.V7SERIE='{1}' AND MC.V7FOLIO='{2}'  ) AND V7CONO='{0}' AND V7SERIE='{1}' AND V7FOLIO='{2}' )
        UNION ( SELECT '0' AS Exist , '0' AS detail_seq,  '0' AS comment_seq, '' AS comment FROM sysibm.sysdummy1) ";*/
        //---------------------------------

        public static string SelectCfdiRelacionados { get; set; } = @"SELECT CASE WHEN  (SELECT COUNT(*) FROM MXEIRC RC WHERE RC.VJCONO='{0}' AND RC.VJSERIE='{1}' AND RC.VJFOLIO='{2}'   ) !=0 THEN '1'
ELSE '0' END AS Exist,  
(CAST(COALESCE(VJISERIE, '') as char(100) ccsid 037))  AS  VJISERIE,
(CAST(COALESCE(VJIFOLIO, '') as char(100) ccsid 037))  AS  VJIFOLIO,
(CAST(COALESCE(VJCONO, '') as char(100) ccsid 037))  AS  VJCONO,
(CAST(COALESCE(VJSEQN, 0 ) as char(100) ccsid 037))  AS  VJSEQN
FROM MXEIRC WHERE VJCONO='{0}' AND VJSERIE='{1}' AND VJFOLIO='{2}'  ";
/*(SELECT CASE WHEN  (SELECT COUNT(*) FROM MXEIRC WHERE VJCONO='{0}' AND VJSERIE='{1}' AND VJFOLIO='{2}'   ) !=0 THEN '1'
ELSE '0' END AS Exist,
CASE WHEN  (SELECT COUNT(*) FROM MXEIRC WHERE VJISERIE IS NULL AND VJCONO='{0}' AND VJSERIE='{1}' AND VJFOLIO='{2}'   ) !=0 THEN ''
ELSE (SELECT VJISERIE FROM MXEIRC WHERE VJCONO='{0}' AND VJSERIE='{1}' AND VJFOLIO='{2}' ) END AS VJISERIE,
CASE WHEN  (SELECT COUNT(*) FROM MXEIRC WHERE VJIFOLIO IS NULL AND VJCONO='{0}' AND VJSERIE='{1}' AND VJFOLIO='{2}'   ) !=0 THEN ''
ELSE (SELECT VJIFOLIO FROM MXEIRC WHERE VJCONO='{0}' AND VJSERIE='{1}' AND VJFOLIO='{2}' ) END AS VJIFOLIO,
CASE WHEN  (SELECT COUNT(*) FROM MXEIRC WHERE VJCONO IS NULL AND VJCONO='{0}' AND VJSERIE='{1}' AND VJFOLIO='{2}'   ) !=0 THEN ''
ELSE (SELECT VJCONO FROM MXEIRC WHERE VJCONO='{0}' AND VJSERIE='{1}' AND VJFOLIO='{2}' ) END AS VJCONO
FROM MXEIRC WHERE VJCONO='{0}' AND VJSERIE='{1}' AND VJFOLIO='{2}' )
UNION
( SELECT '0' AS Exist, '0' AS VJISERIE,'0' AS VJIFOLIO,'0' AS VJCONO FROM sysibm.sysdummy1 ); ";*/

//( SELECT '0' AS Exist, IFNULL(VJISERIE,'') AS VJISERIE,IFNULL(VJIFOLIO,'') AS VJIFOLIO,IFNULL(VJCONO,'') AS VJCONO 
//FROM MXEIRC WHERE EXISTS ( SELECT * FROM MXEIRC ) FETCH FIRST 1 ROW ONLY )  ";

        public static string TestConnection { get; set; } = @"SELECT 1 FROM MXEIRQ ";
        //ValStrings
        //----------------------------------------------------------------
        //Se ale agrega XA UPPER y RTRIM (SL solo tiene el UPPER)
        public static string getRFCs { get; set; } = @"SELECT V0RFCE AS rfcCompania, RTRIM(UPPER(V0CLIEN)) AS clienteId, V0RFCR AS rfcCliente
                                                       FROM MXEIHD WHERE V0CONO ='{0}' AND V0SERIE = '{1}' AND V0FOLIO = '{2}' ";
        //***ESTE ES XA***//
        public static string getItems { get; set; } = @"SELECT V3CNID AS noIdentification, V0CLIEN AS clienteId,V3SEQN AS sequence FROM MXEIDT
LEFT JOIN MXEIHD ON V0CONO =V3CONO AND V0SERIE = V3SERIE AND V0FOLIO = V3FOLIO 
WHERE V3CONO='{0}' AND V3SERIE='{1}' AND V3FOLIO='{2}' ";
        
        public static string getTaxes { get; set; } = @"SELECT DISTINCT V5IMPU AS claveImpuesto FROM MXEITX
WHERE V5CONO = '{0}' AND V5SERIE = '{1}' AND V5FOLIO = '{2}' ";

        public static string SelectExistsPedimentos { get; set; } = @"SELECT  CASE WHEN (
 (SELECT 1 AS EXISTS  from sysibm.sysdummy1 WHERE EXISTS ( SELECT * FROM MXEIPD WHERE V4CONO='{0}' AND V4SERIE='{1}' AND V4FOLIO='{2}'  ) ) = 1
)
THEN ( select  '1' from sysibm.sysdummy1 ) 
ELSE  ( select  '0' from sysibm.sysdummy1 ) END AS Exist
 from sysibm.sysdummy1 WHERE EXISTS ( SELECT * FROM " + ConnectionData.SchemaDefault + ".MXEIPD ) ";


        public static string SelectPedimentos { get; set; } = @" SELECT  
CASE WHEN (
 (SELECT 1 AS EXISTS   FROM  sysibm.sysdummy1 WHERE EXISTS ( SELECT * FROM " + ConnectionData.SchemaDefault + ".MXEIPD WHERE V4CONO='{0}' AND V4SERIE='{1}' AND V4FOLIO='{2}'  AND V4SEQN='{3}' AND V4SEQT=0 ) ) = 1) " +
" THEN ( SELECT '1'  FROM  sysibm.sysdummy1 ) " +
" ELSE  ( SELECT '0'  FROM  sysibm.sysdummy1 ) END AS Exist, " +
" CASE WHEN( (SELECT 1 AS pedimentNumber  from sysibm.sysdummy1 WHERE EXISTS ( SELECT * FROM " + ConnectionData.SchemaDefault + ".MXEIPD WHERE V4CONO='{0}' AND V4SERIE='{1}' AND V4FOLIO='{2}'  AND V4SEQN='{3}' AND V4SEQT=0 ) ) = 1) " +
" THEN ( SELECT  COALESCE(V4PEDI,'')   FROM " + ConnectionData.SchemaDefault + ".MXEIPD WHERE V4CONO='{0}' AND V4SERIE='{1}' AND V4FOLIO='{2}'  AND V4SEQN='{3}' AND V4SEQT=0 ) " +
" ELSE  ( SELECT  '0'  FROM  sysibm.sysdummy1 ) END AS pedimentNumber," +
" CASE WHEN( (SELECT 1 AS UFT1  from sysibm.sysdummy1 WHERE EXISTS ( SELECT * FROM " + ConnectionData.SchemaDefault + ".MXEIPD WHERE V4CONO='{0}' AND V4SERIE='{1}' AND V4FOLIO='{2}'  AND V4SEQN='{3}' AND V4SEQT=0 ) ) = 1) " +
" THEN ( SELECT  COALESCE(V4UUT1,'')    FROM " + ConnectionData.SchemaDefault + ".MXEIPD WHERE V4CONO='{0}' AND V4SERIE='{1}' AND V4FOLIO='{2}'  AND V4SEQN='{3}' AND V4SEQT=0 ) " +
" ELSE  ( SELECT  '0'  FROM  sysibm.sysdummy1 ) END AS UFT1, " +
" TIMESTAMP_FORMAT(CASE WHEN( (SELECT 1 AS UFD1  from sysibm.sysdummy1 WHERE EXISTS ( SELECT * FROM " + ConnectionData.SchemaDefault + ".MXEIPD WHERE V4CONO='{0}' AND V4SERIE='{1}' AND V4FOLIO='{2}'  AND V4SEQN='{03}' AND V4SEQT=0 ) ) = 1) " +
" THEN ( SELECT CASE WHEN V4UUD1 = '0001-01-01' THEN  COALESCE( VARCHAR_FORMAT ('1900-01-01','YYYY-MM-DD'),'0001-01-01') ELSE COALESCE( VARCHAR_FORMAT (V4UUD1,'YYYY-MM-DD'),'0001-01-01') END    FROM " + ConnectionData.SchemaDefault + ".MXEIPD WHERE V4CONO='{0}' AND V4SERIE='{1}' AND V4FOLIO='{2}'  AND V4SEQN='{03}' AND V4SEQT=0 ) " +
" ELSE  ( SELECT  '0'  FROM  sysibm.sysdummy1 ) END ,'YYYYMMDD') AS UFD1 " +
" FROM  sysibm.sysdummy1 WHERE EXISTS ( SELECT * FROM " + ConnectionData.SchemaDefault + ".MXEIPD ) ";

        public static string SelectGetSchema { get; set; } = @"SELECT V9DLIB AS SCHEMA,  VARCHAR_FORMAT( V9DTTM,'YYYY-MM-DD') AS TIMESTAMP FROM MXEIRQ WHERE V9CONO= '{0}' AND V9SERIE='{1}' AND V9FOLIO='{2}' ";

        public static string SelectVerificaPOS { get; set; } = @"SELECT '1' AS Exist FROM MXEIAD AD WHERE AD.V2CONO='{0}' AND AD.V2SERIE='{1}' AND AD.V2FOLIO='{2}' AND AD.V2TREG='S'  ";

        /*SELECT  CASE WHEN (
 (SELECT 1 AS EXISTS  from sysibm.sysdummy1 WHERE EXISTS ( SELECT * FROM MXEIAD AD WHERE AD.V2CONO='{0}' AND AD.V2SERIE='{1}' AND AD.V2FOLIO='{2}' AND AD.V2TREG='S' ) ) =1
)
THEN ( select  '1' from sysibm.sysdummy1 ) 
ELSE  ( select  '0' from sysibm.sysdummy1 ) END AS Exist
 from sysibm.sysdummy1 WHERE EXISTS ( SELECT * FROM MXEIAD ) ";*/


        public static string SelectPOS { get; set; } = @"SELECT RTRIM(V2CONO) AS V2CONO,
CASE WHEN (SELECT COUNT(*) FROM MXEIAD WHERE V2SERIE IS NULL AND V2CONO='{0}' AND V2SERIE='{1}' AND V2FOLIO='{2}'  AND UPPER(V2TREG) = 'S' ) != 0 THEN '' 
ELSE (SELECT  RTRIM(V2SERIE) FROM MXEIAD WHERE V2CONO='{0}' AND V2SERIE='{1}' AND V2FOLIO='{2}' AND UPPER(V2TREG) = 'S'  ) END AS V2SERIE,
RTRIM(V2FOLIO) AS V2FOLIO, (cast(V2TREG as char(1) ccsid 037)) AS V2TREG,
(CAST(COALESCE(V2CALL, '') as char(100) ccsid 037))  AS  V2CALL,
(CAST(COALESCE(V2NEXT, '') as char(20) ccsid 037))  AS  V2NEXT,
(CAST(COALESCE(V2NINT, '') as char(20) ccsid 037))  AS  V2NINT,
(CAST(COALESCE(V2COLO, '') as char(50) ccsid 037))  AS  V2COLO,
(CAST(COALESCE(V2LOCL, '') as char(50) ccsid 037))  AS  V2LOCL,
(CAST(COALESCE(V2REFR, '') as char(50) ccsid 037))  AS  V2REFR,
(CAST(COALESCE(V2MUNI, '') as char(50) ccsid 037))  AS  V2MUNI,
(CAST(COALESCE(V2ESTA, '') as char(50) ccsid 037))  AS  V2ESTA,
(CAST(COALESCE(V2PAIS, '') as char(50) ccsid 037))  AS  V2PAIS,
(CAST(COALESCE(V2CPOS, '') as char(20) ccsid 037))  AS  V2CPOS,
(CAST(COALESCE(V2LGEM, '') as char(150) ccsid 037))  AS  V2LGEM,
(CAST(COALESCE(V2TELEF, '') as char(60) ccsid 037))  AS  V2TELEF,
(CAST(COALESCE(V2UUT1, '') as char(50) ccsid 037))  AS  V2UUT1,
(CAST(COALESCE(V2SEAD, '') as char(50) ccsid 037))  AS  V2SEAD,
CASE WHEN (SELECT COUNT(*) FROM MXEIAD WHERE V2UUD1='0001-01-01' AND V2CONO='{0}' AND V2SERIE='{1}' AND V2FOLIO='{2}' AND UPPER(V2TREG) = 'S')!= 0 THEN ''
ELSE (SELECT  (cast(V2UUD1 as char(200) ccsid 037))  FROM MXEIAD WHERE  V2CONO='{0}' AND V2SERIE='{1}' AND V2FOLIO='{2}' AND UPPER(V2TREG) = 'S') END AS V2UUD1,
COALESCE(V2USGE, '') AS V2USGE
FROM  MXEIAD  WHERE V2CONO='{0}' AND V2SERIE='{1}' AND V2FOLIO='{2}' AND UPPER(V2TREG) = 'S' ";

        public static string InsertPOS { get; set; } = @" DELETE ZMX_MXEIAD WHERE V2CONO=@V2CONO AND V2SERIE=@V2SERIE AND V2FOLIO=@V2FOLIO ;
INSERT INTO ZMX_MXEIAD (V2CONO,V2SERIE,V2FOLIO,V2TREG,V2CALL,V2NEXT,V2NINT,V2COLO,V2LOCL,V2REFR,V2MUNI,V2ESTA,V2PAIS,V2CPOS,V2LGEM,V2TELEF,V2UUT1,V2UUD1,V2USGE,V2SEAD)
VALUES (@V2CONO,@V2SERIE,@V2FOLIO,@V2TREG,@V2CALL,@V2NEXT,@V2NINT,@V2COLO,@V2LOCL,@V2REFR,@V2MUNI,@V2ESTA,@V2PAIS,@V2CPOS,@V2LGEM,@V2TELEF,@V2UUT1,@V2UUD1,@V2USGE,@V2SEAD); ";

        //Querys para Complemento de impuestos locales
        public static string SelectExistsImpuestosLocales { get; set; } = @"SELECT  CASE WHEN (
 (SELECT 1 AS EXISTS  from sysibm.sysdummy1 WHERE EXISTS ( SELECT * FROM MXEITXL WHERE VLCONO='{0}' AND VLSERIE='{1}' AND VLFOLIO='{2}'  ) ) =1
)
THEN ( select  '1' from sysibm.sysdummy1 ) 
ELSE  ( select  '0' from sysibm.sysdummy1 ) END AS Exist
 from sysibm.sysdummy1 WHERE EXISTS ( SELECT * FROM MXEITXL )  ";

        public static string SelectImpuestosLocales { get; set; } = @"SELECT COALESCE(VLTXTY,'')TipoTax,COALESCE(VLIMPO,0)Importe,COALESCE(VLTASA,'')Tasa,COALESCE(VLDESC,'')Descripcion FROM MXEITXL WHERE VLCONO= '{0}' AND VLSERIE = '{1}' AND VLFOLIO = '{2}'  ";

        //Querys para Complemento detallista
        //30-12-2017 MOD. FED
        public static string SelectExistsDetallista { get; set; } = @"SELECT  CASE WHEN (
 (SELECT 1 AS EXISTS  from sysibm.sysdummy1 WHERE EXISTS ( SELECT * FROM MXADHD WHERE A0CONO='{0}' AND A0SERIE='{1}' AND A0FOLIO='{2}'  ) ) =1
)
THEN ( select  '1' from sysibm.sysdummy1 ) 
ELSE  ( select  '0' from sysibm.sysdummy1 ) END AS Exist
 from sysibm.sysdummy1 WHERE EXISTS ( SELECT * FROM MXADHD )  ";

        public static string SelectMXADHD { get; set; } = @"(SELECT '1' AS Exist,A0CONO,A0SERIE,A0FOLIO,COALESCE(A0TYPE,'')A0TYPE,COALESCE(A0CNVR,'')A0CNVR,COALESCE(A0DCVR,'')A0DCVR,
COALESCE(A0DCST,'')A0DCST,--COALESCE(A0DDTE,'0001-01-01')A0DDTE,
CASE WHEN (SELECT COUNT(*) FROM MXADHD HD WHERE HD.A0DDTE='0001-01-01' AND HD.A0CONO='{0}' AND HD.A0SERIE='{1}' AND HD.A0FOLIO='{2}') = 1 THEN ' '
ELSE (SELECT  (cast(A0DDTE as char(200) ccsid 037))  FROM MXADHD HD WHERE  HD.A0CONO='{0}' AND HD.A0SERIE='{1}' AND HD.A0FOLIO='{2}') END AS A0DDTE, 
COALESCE(A0ENTY,'')A0ENTY,
COALESCE(A0UNID,'')A0UNID,COALESCE(A0BYER,'')A0BYER,COALESCE(A0CONT,'')A0CONT,
COALESCE(A0SELR,'')A0SELR,COALESCE(A0ASLR,'')A0ASLR,COALESCE(A0ASLT,'')A0ASLT,
COALESCE(A0PTEV,'')A0PTEV,COALESCE(A0PTRT,'')A0PTRT,COALESCE(A0NPTT,'')A0NPTT,
COALESCE(A0PTTP,'')A0PTTP,COALESCE(A0PTPV,0)A0PTPV,COALESCE(A0DSTY,'')A0DSTY,
COALESCE(A0DSPC,0)A0DSPC,COALESCE(A0TOTI,0)A0TOTI,COALESCE(A0TOBT,0)A0TOBT,COALESCE(A0TOAP,0)A0TOAP
FROM MXADHD WHERE  EXISTS (SELECT * FROM MXADHD HD WHERE HD.A0CONO='{0}' AND HD.A0SERIE='{1}' AND HD.A0FOLIO='{2}' ) AND A0CONO='{0}' AND A0SERIE='{1}' AND A0FOLIO='{2}' )
UNION (
SELECT '0' AS Exist , '' A0CONO,'' A0SERIE,'' A0FOLIO,'' A0TYPE,''A0CNVR,''A0DCVR,
'' A0DCST,'0001-01-01' A0DDTE,''A0ENTY,
'' A0UNID,'' A0BYER,'' A0CONT,
'' A0SELR,'' A0ASLR,'' A0ASLT,
'' A0PTEV,'' A0PTRT,'' A0NPTT,
'' A0PTTP,0 A0PTPV,'' A0DSTY,
0 A0DSPC,0 A0TOTI,0 A0TOBT,0 A0TOAP FROM sysibm.sysdummy1)  ";

        public static string SelectMXADSI { get; set; } = @"(SELECT '1' AS Exist,A1CONO,A1SERIE,A1FOLIO,A1SEQ,COALESCE(A1CODE,'')A1CODE,COALESCE(A1TEXT,'')A1TEXT 
FROM MXADSI WHERE EXISTS (SELECT * FROM MXADSI SI WHERE SI.A1CONO='{0}' AND SI.A1SERIE='{1}' AND SI.A1FOLIO='{2}' ) AND A1CONO='{0}' AND A1SERIE='{1}' AND A1FOLIO='{2}' )
UNION (
SELECT '0' AS Exist,'' A1CONO,'' A1SERIE,'' A1FOLIO,0 A1SEQ,'' A1CODE,'' A1TEXT   FROM sysibm.sysdummy1 )   ";

        public static string SelectMXADCU { get; set; } = @"( SELECT '1' AS Exist, A2CONO,A2SERIE,A2FOLIO,A2SEQ,COALESCE(A2CODE,'')A2CODE,COALESCE(A2CUFN,'')A2CUFN,COALESCE(A2CURT,0)A2CURT  
FROM MXADCU  WHERE EXISTS (SELECT * FROM MXADCU CU WHERE CU.A2CONO='{0}' AND CU.A2SERIE='{1}' AND CU.A2FOLIO='{2}' ) AND A2CONO='{0}' AND A2SERIE='{1}' AND A2FOLIO='{2}' )
UNION (
SELECT '0' AS Exist,'' A2CONO,'' A2SERIE,'' A2FOLIO,0 A2SEQ, '' A2CODE,'' A2CUFN,0 A2CURT  FROM sysibm.sysdummy1 )  ";
        
        public static string SelectMXADPD { get; set; } = @"( SELECT '1' AS Exist,A3CONO,A3SERIE,A3FOLIO,A3SEQN,A3SEQP,COALESCE(A3APGLN,'')A3APGLN,COALESCE(A3ALPT,'')A3ALPT,COALESCE(A3PEDI,'')A3PEDI,
--COALESCE(A3PEDF,'0001-01-01')A3PEDF,
CASE WHEN (SELECT COUNT(*) FROM MXADPD PD WHERE PD.A3PEDF='0001-01-01' AND PD.A3CONO='{0}' AND PD.A3SERIE='{1}' AND PD.A3FOLIO='{2}') = 1 THEN ' '
ELSE (SELECT  (cast(A3PEDF as char(200) ccsid 037))  FROM MXADPD PD WHERE  PD.A3CONO='{0}' AND PD.A3SERIE='{1}' AND PD.A3FOLIO='{2}') END AS A3PEDF, 
COALESCE(A3ADNM,'')A3ADNM,COALESCE(A3ADCT,'')A3ADCT
FROM MXADPD WHERE EXISTS (SELECT *  FROM MXADPD PD WHERE PD.A3CONO='{0}' AND PD.A3SERIE='{1}' AND PD.A3FOLIO='{2}'  ) AND A3CONO='{0}' AND A3SERIE='{1}' AND A3FOLIO='{2}' )
UNION (
SELECT '0' AS Exist,'' A3CONO,'' A3SERIE,'' A3FOLIO,0 A3SEQN,0 A3SEQP,'' A3APGLN,'' A3ALPT,'' A3PEDI,
'0001-01-01' A3PEDF,'' A3ADNM,'' A3ADCT  FROM sysibm.sysdummy1  ) ";

        public static string SelectMXADCG { get; set; } = @"(SELECT '1' AS Exist,A4CONO,A4SERIE,A4FOLIO,A4SEQN,A4SEQ,A4SEQD,COALESCE(A4CGTY,'')A4CGTY,COALESCE(A4STLT,'')A4STLT,COALESCE(A4SSTY,'')A4SSTY,
COALESCE(A4SSBS,'')A4SSBS,COALESCE(A4SSPC,0)A4SSPC,COALESCE(A4SSRU,0)A4SSRU,COALESCE(A4SSAU,0)A4SSAU
FROM MXADCG WHERE EXISTS (SELECT * FROM MXADCG CG  WHERE Cg.A4CONO='{0}' AND CG.A4SERIE='{1}' AND CG.A4FOLIO='{2}' ) AND A4CONO='{0}' AND A4SERIE='{1}' AND A4FOLIO='{2}' )
UNION (
SELECT '0' AS Exist,'' A4CONO,'' A4SERIE,'' A4FOLIO,0 A4SEQN,0 A4SEQ,'' A4SEQD,'' A4CGTY,'' A4STLT,'' A4SSTY,'' A4SSBS, 0 A4SSPC,0 A4SSRU, 0 A4SSAU FROM sysibm.sysdummy1 )  ";

        public static string SelectMXADDT { get; set; } = @"(SELECT '1' AS Exist, A5CONO,A5SERIE,A5FOLIO,A5SEQN,COALESCE(A5TYPE,'')A5TYPE,COALESCE(A5CANT,0)A5CANT,COALESCE(A5UNMSR,'')A5UNMSR,
COALESCE(A5EAN,'')A5EAN,COALESCE(A5LANG,'')A5LANG,COALESCE(A5DESC,'')A5DESC,COALESCE(A5UNPR,0)A5UNPR,
COALESCE(A5UNPRLC,0)A5UNPRLC,COALESCE(A5NTPR,0)A5NTPR,COALESCE(A5NTPRLC,0)A5NTPRLC,COALESCE(A5GRAM,0)A5GRAM,
COALESCE(A5GRAMLC,0)A5GRAMLC,COALESCE(A5NTAM,0)A5NTAM,COALESCE(A5NTAMLC,0)A5NTAMLC,COALESCE(A5SSCC,'')A5SSCC,
COALESCE(A5SSCT,'')A5SSCT,COALESCE(A5PLQT,0)A5PLQT,COALESCE(A5PLDS,'')A5PLDS,COALESCE(A5PLTY,'')A5PLTY,COALESCE(A5TRPM,'')A5TRPM
FROM MXADDT WHERE EXISTS ( SELECT * FROM MXADDT DT WHERE DT.A5CONO='{0}' AND DT.A5SERIE='{1}' AND DT.A5FOLIO='{2}' ) AND A5CONO='{0}' AND A5SERIE='{1}' AND A5FOLIO='{2}' )
UNION (
SELECT '0' AS Exist,'' A5CONO,'' A5SERIE,'' A5FOLIO,0 A5SEQN,'' A5TYPE,0 A5CANT,'' A5UNMSR,
'' A5EAN,'' A5LANG,'' A5DESC,0 A5UNPR, 0 A5UNPRLC,0 A5NTPR,0 A5NTPRLC,0 A5GRAM,
0 A5GRAMLC,0 A5NTAM,0 A5NTAMLC,'' A5SSCC,
'' A5SSCT,0 A5PLQT, '' A5PLDS,'' A5PLTY,'' A5TRPM  FROM sysibm.sysdummy1 )  ";

        public static string SelectMXADRF { get; set; } = @"( SELECT '1' AS Exist, A6CONO,A6SERIE,A6FOLIO,A6TYPE,A6SEQ,COALESCE(A6RFTY,'')A6RFTY,COALESCE(A6RFID,0)A6RFID,
--COALESCE(A6RFDT,'0001-01-01')A6RFDT
CASE WHEN (SELECT COUNT(*) FROM MXADRF RF WHERE RF.A6RFDT='0001-01-01' AND RF.A6CONO='{0}' AND RF.A6SERIE='{1}' AND RF.A6FOLIO='{2}') = 1 THEN ' '
ELSE (SELECT  (cast(A6RFDT as char(200) ccsid 037))  FROM MXADRF RF WHERE  RF.A6CONO='{0}' AND RF.A6SERIE='{1}' AND RF.A6FOLIO='{2}') END AS A6RFDT 
FROM MXADRF WHERE EXISTS (SELECT * FROM MXADRF RF WHERE RF.A6CONO='{0}' AND RF.A6SERIE='{1}' AND RF.A6FOLIO='{2}' ) AND A6CONO='{0}' AND A6SERIE='{1}' AND A6FOLIO='{2}'  )
UNION ( 
SELECT '0' AS Exist,'' A6CONO,'' A6SERIE,'' A6FOLIO,'' A6TYPE,0 A6SEQ,'' A6RFTY,0 A6RFID,'0001-01-01' A6RFDT   FROM sysibm.sysdummy1 )   ";

        public static string SelectMXADAD { get; set; } = @"( SELECT '1' AS Exist,A7CONO,A7SERIE,A7FOLIO,A7TREG,COALESCE(A7GLN,'')A7GLN,COALESCE(A7NAME,'')A7NAME,COALESCE(A7STRT,'')A7STRT,
COALESCE(A7CITY,'')A7CITY,COALESCE(A7ZIPC,'')A7ZIPC,COALESCE(A7APID,'')A7APID,COALESCE(A7APTY,'')A7APTY
FROM MXADAD WHERE EXISTS (SELECT * FROM MXADAD AD WHERE AD.A7CONO='{0}' AND AD.A7SERIE='{1}' AND AD.A7FOLIO='{2}' ) AND  A7CONO='{0}' AND A7SERIE='{1}' AND A7FOLIO='{2}' )
UNION (
SELECT '0' AS Exist,'' A7CONO,'' A7SERIE,'' A7FOLIO,'' A7TREG,'' A7GLN,'' A7NAME,'' A7STRT,
'' A7CITY,'' A7ZIPC,'' A7APID,'' A7APTY  FROM sysibm.sysdummy1 ) ";

        public static string SelectMXADII { get; set; } = @"( SELECT '1' AS Exist,A8CONO,A8SERIE,A8FOLIO,A8SEQN,A8SEQA,COALESCE(A8ALTID,'')A8ALTID,COALESCE(A8ALTTY,'')A8ALTTY
FROM MXADII WHERE EXISTS (SELECT * FROM MXADII II WHERE II.A8CONO='{0}' AND II.A8SERIE='{1}' AND II.A8FOLIO='{2}' ) AND A8CONO='{0}' AND A8SERIE='{1}' AND A8FOLIO='{2}' )
UNION (
SELECT '0' AS Exist,'' A8CONO,'' A8SERIE,'' A8FOLIO,0 A8SEQN,0 A8SEQA,'' A8ALTID,'' A8ALTTY FROM sysibm.sysdummy1 )  ";

        public static string SelectMXADIQ { get; set; } = @"( SELECT '1' AS Exist,A9CONO,A9SERIE,A9FOLIO,A9SEQN,A9SEQ,COALESCE(A9ADQT,0)A9ADQT,COALESCE(A9ADTY,'')A9ADTY
FROM MXADIQ WHERE EXISTS (SELECT * FROM MXADIQ IQ WHERE IQ.A9CONO='{0}' AND IQ.A9SERIE='{1}' AND IQ.A9FOLIO='{2}' ) AND A9CONO='{0}' AND A9SERIE='{1}' AND A9FOLIO='{2}' )
UNION (
SELECT '0' AS Exist,'' A9CONO,'' A9SERIE,'' A9FOLIO,0 A9SEQN,0 A9SEQ,0 A9ADQT,'' A9ADTY FROM sysibm.sysdummy1 )  ";

        public static string SelectMXADEA { get; set; } = @"( SELECT '1' AS Exist,AACONO,AASERIE,AAFOLIO,AASEQN,AASEQEA,COALESCE(AALOT,'')AALOT,
--COALESCE(AAPDTE,'0001-01-01')AAPDTE
CASE WHEN (SELECT COUNT(*) FROM MXADEA EA WHERE EA.AAPDTE='0001-01-01' AND EA.AACONO='{0}' AND EA.AASERIE='{1}' AND EA.AAFOLIO='{2}') = 1 THEN ' '
ELSE (SELECT  (cast(AAPDTE as char(200) ccsid 037))  FROM MXADEA EA WHERE  EA.AACONO='{0}' AND EA.AASERIE='{1}' AND EA.AAFOLIO='{2}') END AS AAPDTE
FROM MXADEA WHERE EXISTS (SELECT * FROM MXADEA EA WHERE EA.AACONO='{0}' AND EA.AASERIE='{1}' AND EA.AAFOLIO='{2}' ) AND AACONO='{0}' AND AASERIE='{1}' AND AAFOLIO='{2}' )
UNION (
SELECT '0' AS Exist,'' AACONO,'' AASERIE,'' AAFOLIO,0 AASEQN,0 AASEQEA,'' AALOT,'0001-01-01' AAPDTE   FROM sysibm.sysdummy1 )   ";

        public static string SelectMXADTX { get; set; } = @"( SELECT '1' AS Exist,ABCONO,ABSERIE,ABFOLIO,ABSEQN,ABSEQT,ABTXTY,COALESCE(ABIMPU,'')ABIMPU,COALESCE(ABREFNO,'')ABREFNO,
COALESCE(ABTASA,0)ABTASA,COALESCE(ABIMPO,0)ABIMPO,COALESCE(ABIMPOLC,0)ABIMPOLC
FROM MXADTX WHERE EXISTS (SELECT * FROM MXADTX TX WHERE TX.ABCONO='{0}' AND TX.ABSERIE='{1}' AND TX.ABFOLIO='{2}' ) AND ABCONO='{0}' AND ABSERIE='{1}' AND ABFOLIO='{2}' )
UNION (
SELECT '0' AS Exist,'' ABCONO,'' ABSERIE,'' ABFOLIO,0 ABSEQN,0 ABSEQT,'' ABTXTY,'' ABIMPU,'' ABREFNO,
0 ABTASA,0 ABIMPO,0 ABIMPOLC FROM sysibm.sysdummy1 )  ";

        public static string SelectMXADXC { get; set; } = @"(SELECT '1' AS Exist,AECONO,AESERIE,AEFOLIO,COALESCE(AECOID,'')AECOID
FROM MXADXC WHERE EXISTS (SELECT * FROM MXADXC XC WHERE XC.AECONO='{0}' AND XC.AESERIE='{1}' AND XC.AEFOLIO='{2}' ) AND AECONO='{0}' AND AESERIE='{1}' AND AEFOLIO='{2}' )
UNION (
SELECT '0' AS Exist,'' AECONO,'' AESERIE,'' AEFOLIO,'' AECOID FROM sysibm.sysdummy1 ) ";

    }
    public class ValStrings
    {
        public static string getRFCs { get; set; } = @"SELECT V0RFCE AS rfcCompania, UPPER(V0CLIEN) AS clienteId, V0RFCR AS rfcCliente
                                                       FROM MXEIHD WHERE V0CONO ='{0}' AND V0SERIE = '{1}' AND V0FOLIO = '{2}' ";

        public static string validaRfcCompany { get; set; } = @"IF EXISTS(SELECT * FROM ZMX_Company WHERE rfc = '{0}')
SELECT '1' AS Exist
ELSE
SELECT '0' AS Exist ";

        public static string validaRfcCustomer { get; set; } = @"IF EXISTS (SELECT * FROM ZMX_Customer WHERE rfc = '{0}' AND UPPER(customerId) = '{1}' )
SELECT '1' AS Exist
ELSE
SELECT '0' AS Exist ";

        //Para XA estos dos son los mismos 1
        public static string getItems { get; set; } = @"SELECT V3CNID AS noIdentification,
UPPER(V0CLIEN) AS clienteId,V3SEQN AS sequence, ISNULL(V3UNMSR,'NULL') AS unitId  FROM MXEIDT
LEFT JOIN MXEIHD ON V0CONO =V3CONO AND V0SERIE = V3SERIE AND V0FOLIO = V3FOLIO 
WHERE V3CONO='{0}' AND V3SERIE='{1}' AND V3FOLIO='{2}' ";// Para Validar

        //Para XA estos dos son los mismos 1
        public static string getItemsUpdate { get; set; } = @"SELECT CASE WHEN V3UUT3 IS NULL THEN V3CNID ELSE V3UUT3 END AS noIdentification,
UPPER(V0CLIEN) AS clienteId,V3SEQN AS sequence, ISNULL(V3UNMSR,'NULL') AS unitId FROM MXEIDT
LEFT JOIN MXEIHD ON V0CONO =V3CONO AND V0SERIE = V3SERIE AND V0FOLIO = V3FOLIO 
WHERE V3CONO='{0}' AND V3SERIE='{1}' AND V3FOLIO='{2}' ";//Para actualizar

                                                         //*** No recuerpo xq lo hice pero lo estoy modificando para prinsel OJO MUCHO OJO ***//
                                                         //@"SELECT CASE WHEN V3UUT3 IS NULL THEN V3CNID ELSE V3UUT3 END AS noIdentification, V0CLIEN AS clienteId,V3SEQN AS sequence FROM MXEIDT
                                                         //LEFT JOIN MXEIHD ON V0CONO =V3CONO AND V0SERIE = V3SERIE AND V0FOLIO = V3FOLIO 
                                                         //WHERE V3CONO='{0}' AND V3SERIE='{1}' AND V3FOLIO='{2}' ";
                                                         //***ASI ESTA XA***//
                                                         //SELECT V3CNID AS noIdentification, V0CLIEN AS clienteId,V3SEQN AS sequence FROM MXEIDT
                                                         //LEFT JOIN MXEIHD ON V0CONO =V3CONO AND V0SERIE = V3SERIE AND V0FOLIO = V3FOLIO 
                                                         //WHERE V3CONO='{0}' AND V3SERIE='{1}' AND V3FOLIO='{2}' ";
                                                         //Para verificar si existe en el CS
        public static string validaItems { get; set; } = @"IF EXISTS (SELECT * FROM ZMX_ContractsCustomers WHERE item='{0}' AND customer = '{1}'  ) 
SELECT '1' AS Exist
Else IF EXISTS  (SELECT * FROM ZMX_Items WHERE noIdentification ='{0}')
SELECT '1' AS Exist
ELSE 
SELECT '0' AS Exist  ";

        //Agregado por JL para la validacion de las unidades
        public static string validaum { get; set; } = @"IF EXISTS (SELECT * FROM zmx_relationum WHERE umERP='{0}' ) 
SELECT '1' AS Exist
ELSE 
SELECT '0' AS Exist  ";

        //Parp Update
        public static string validaConceptos { get; set; } = @"IF EXISTS (SELECT * FROM ZMX_concept WHERE noIdentification = '{0}' AND voucherId ='{1}' AND sequence='{2}')
SELECT conceptId, '1' AS Exist FROM ZMX_concept WHERE noIdentification = '{0}' AND voucherId ='{1}' AND sequence='{2}'
ELSE
SELECT NEWID() AS conceptId,'0' AS Exist ";

        public static string getTaxes { get; set; } = @"SELECT DISTINCT V5IMPU AS claveImpuesto FROM MXEITX
WHERE V5CONO = '{0}' AND V5SERIE = '{1}' AND V5FOLIO = '{2}' ";

        public static string validaTaxes { get; set; } = @"IF EXISTS(SELECT * FROM zmx_taxconversion WHERE erpSat = '{0}')
SELECT '1' AS Exist
ELSE 
SELECT '0' AS Exist  ";
    }
    public class Rfc
    {
        public string tipoRfc;
        public string rfc;
        public string cliente;
        public string existe;
    }
    public class Items
    {
        public string item;
        public string existe;
        //Agregado por JL para la validacion de las unidades
        public string unidad;
        public string descripcion;
        public string sequence;
        //
    }
    //Agregado por JL para la validacion de las unidades
    public class um
    {
        public string unidad;
        public string existe;
    }
    public class Taxes
    {
        public string impuesto;
        public string existe;
    }
    public class msg
    {
        public static bool bShowMsg { get; set; } = false;
        public bool bAddenda = false;
        public static string msgcompany { get; set; } = @"El RFC {0} del emisor no coincide con el RFC del common solution; ";
        public static string msgcustomer { get; set; } = @"El RFC {0} o clave del cliente {1} del receptor no se encuentra registrado en common solution; ";
        public static string msgitems { get; set; } = @"El concepto {0} no se encuentra registrado en el common solution; ";
        public static string msgtaxes { get; set; } = @"El impuesto {0} no se encuentra registrado en el common solution;  ";
        //Agregado por JL para la validacion de las unidades
        public static string msgunidades { get; set; } = @"La unidad {0} no se encuentra registrado en el common solution;  ";
    }

}
