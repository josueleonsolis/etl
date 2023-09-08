using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ETLCountryPack;
using System.Configuration;
using System.Xml;

namespace DemoETL
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("----DemoETL-CountryPack ----");
            Console.WriteLine("--------------------------------");
            //====================
            Char delimiter = '@';
            string stringConection = @"Data Source={0} ;Initial Catalog={1};User ID={2};Password={3};";
            string stringConectionIS = @"Data Source={0};Initial Catalog={1}; Integrated Security=SSPI;";
            string stringConnectionXA = @"DataSource={0}; Initial Catalog={1}; UserID={2}; Password={3}; Naming=System; LibraryList={4}; ";
            string stringConnectionIS_XA = @"DataSource={0};Initial Catalog={1}; Integrated Security=SSPI; Naming=System; LibraryList={2}; ";

            string providerSQL = @"System.Data.SqlClient.SqlConnection, System.Data, Version=2.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089";
            string providerXA = @"IBM.Data.DB2.iSeries.iDB2Connection, IBM.Data.DB2.iSeries, Version = 12.0.0.0, Culture = neutral, PublicKeyToken = 9cdb2ebfb1f93a26";
            string Parameter1, Parameter2, Parameter3, Parameter4, Parameter5, Parameter6, Parameter7, Parameter8, Parameter9, Parameter10, Parameter11, Parameter12, Parameter13, Parameter14, Parameter15, Parameter16, Parameter17, Parameter18;
            //=================================================================
            //Parameter1 = Acción a ejecutar(Extraction|Message)
            //Parameter2 = StringConnectionERP
            //Parameter3 = StringConnection CS
            //Parameter4 = (status /) site
            //Parameter5 = (Message /)serie (is null -)
            //Parameter6 = (statusInitial/) / folio
            //Parameter7 = status (ststusEnd /) (is null -)
            //Parameter8 = Message
            //Parameter9 = statusInitial
            //Parameter10 = ststusEnd
            //Parameter11 = (is null -) //Futuros Campos
            //Parameter12 = (is null -) //Futuros Campos
            //Parameter13 =  (is null -)//Futuros Campos

            //{0}=Cono,{1}=Serie,{2}=Folio,{3}=Message,{4}=INLS,{5}=ENDS,{6}=STS
            //=================================================================

            Parameter1 = Convert.ToString(args[0]);
            Parameter2 = Convert.ToString(args[1]);
            Parameter3 = Convert.ToString(args[2]);
            Parameter4 = Convert.ToString(args[3]);
            Parameter5 = Convert.ToString(args[4]);       //Parameter5 = Parameter5.Replace("|","@");
            Parameter6 = Convert.ToString(args[5]).Replace("/"," ");
            //String[] parm7 = args[6].Split('#');
            Parameter7 = Convert.ToString(args[6]);
            Parameter8 = Convert.ToString(args[7]);
            Parameter9 = Convert.ToString(args[8]);
            Parameter10 = Convert.ToString(args[9]);
            Parameter11 = Convert.ToString(args[10]);//UUID Relacionado Sustitucion
            Parameter12 = Convert.ToString(args[11]);     //XML uuid pagos relacionados 
            Parameter13 = Convert.ToString(args[12]);     //Fecha cancelacion
            Parameter14 = Convert.ToString(args[13]);//xml taxes
            if (args.Length > 13)
            {
                Parameter15 = Convert.ToString(args[14]);//xml documentos relacionados
                if (args.Length > 14)
                {
                    Parameter16 = Convert.ToString(args[15]);//Total iva
                    Parameter17 = Convert.ToString(args[16]);//total iva retenido
                    Parameter18 = Convert.ToString(args[17]);//total isr retenido
                }
                else
                {
                    Parameter16 = string.Empty;
                    Parameter17 = string.Empty;
                    Parameter18 = string.Empty;
                }
            }
            else
            {
                Parameter15 = string.Empty;
                Parameter16 = string.Empty;
                Parameter17 = string.Empty;
                Parameter18 = string.Empty;
            }
            //Agregado para manejar los XML
            if (Parameter11 == "SQL")
            {
                stringConnectionXA = stringConnectionXA.Replace("System", Parameter11);
                stringConnectionIS_XA = stringConnectionIS_XA.Replace("System", Parameter11);
            }

            String[] substringsERP = Parameter2.Split(delimiter);
            if (substringsERP[4].ToString() == "0" && substringsERP[5].ToString().ToUpper() != "XA")
                ConnectionData.StringConnectionERP = string.Format(stringConection, substringsERP[0].ToString(), substringsERP[1].ToString(), substringsERP[2].ToString(), substringsERP[3].ToString());
            else if (substringsERP[4].ToString() == "1" && substringsERP[5].ToString().ToUpper() != "XA")
                ConnectionData.StringConnectionERP = string.Format(stringConectionIS, substringsERP[0].ToString(), substringsERP[1].ToString());

            if (substringsERP[4].ToString() == "0" && substringsERP[5].ToString().ToUpper() == "XA")
                ConnectionData.StringConnectionERP = string.Format(stringConnectionXA, substringsERP[0].ToString(), substringsERP[1].ToString(), substringsERP[2].ToString(), substringsERP[3].ToString(), "QGPL"); //substringsERP[6].ToString());
            else if (substringsERP[4].ToString() == "1" && substringsERP[5].ToString().ToUpper() == "XA")
                ConnectionData.StringConnectionERP = string.Format(stringConnectionIS_XA, substringsERP[0].ToString(), substringsERP[1].ToString(), "QGPL"); //substringsERP[6].ToString());


            if (substringsERP[5].ToString().ToUpper() == "XA")
            {
                ConnectionData.ProviderConnectionERP = providerXA;
                ConnectionData.SchemaDefault = substringsERP[6].ToString(); //Guarda el SchemaDefault

                Console.WriteLine(substringsERP[6].ToString());
                Console.WriteLine("esquema");
            }
            else
                ConnectionData.ProviderConnectionERP = providerSQL;

            String[] substringsSC = Parameter3.Split(delimiter);
            if (substringsSC[4].ToString() == "0")
                ConnectionData.StringConnection = string.Format(stringConection, substringsSC[0].ToString(), substringsSC[1].ToString(), substringsSC[2].ToString(), substringsSC[3].ToString());
            else
                ConnectionData.StringConnection = string.Format(stringConectionIS, substringsSC[0].ToString(), substringsSC[1].ToString());

            ConnectionData.ProviderConnection = providerSQL;

            ConnectionData.StringConnectionERP = ConnectionData.StringConnectionERP.Replace("<*>", "@");
            ConnectionData.StringConnectionERP = ConnectionData.StringConnectionERP.Replace("$L*$G", "@");
            ConnectionData.StringConnection = ConnectionData.StringConnection.Replace("<*>", "@");
            ConnectionData.StringConnection = ConnectionData.StringConnection.Replace("$L*$G", "@");

            Console.WriteLine(ConnectionData.StringConnectionERP);
            Console.WriteLine(ConnectionData.StringConnection);
            //=================================================================
            ////////Para Debuguer  //UserID=MXAPLUS; Password=WN7R3K;   ConnectionData.StringConnectionERP = @"DataSource=192.168.17.168; Initial Catalog=S2150caw; UserID=XACFDI; Password=xacfdi05; Naming=System; LibraryList=QGPL; ";
            ////ConnectionData.StringConnectionERP = @"DataSource=192.168.17.168; Initial Catalog=S2150caw; UserID=MXAPLUS; Password=WN7R3K; Naming=System; LibraryList=QGPL; ";
            ////ConnectionData.StringConnection = @"Data Source=192.168.17.58 ;Initial Catalog=Mongoose_App;User ID=sa;Password=Test_CFDI;";
            //////ConnectionData.StringConnectionERP = @"Data Source=189.206.77.126; Initial Catalog=Dietrix_AppDev;User ID=sa;Password=saCIMDEVCLOUD2017;";
            //////ConnectionData.StringConnection = @"Data Source=189.206.77.60 ;Initial Catalog=CountryPack_App;User ID=mongoose;Password=Mongoo$3CLOUD;";
            ////Parameter1 = "Extraction";
            ////Parameter5 = "B";
            ////Parameter4 = "01";
            ////Parameter6 = "000007";
            ////String[] substringsERP = new String[] { "A", "B", "C", "D", "SL", "XA" };
            ////Parameter7 = "60";
            ////Parameter8 = "Corre@electrónico@fue@enviado@satisfactoriamente.#61082E6A-C8C6-433E-A872-3965C6FB869C#2018-03-28@19:21:00#00001000000404477432#PPD";
            ////Parameter9 = "10";
            ////Parameter10 = "50";
            ////ConnectionData.SchemaDefault = "AMFLIBC";
            ////providerSQL = @"System.Data.SqlClient.SqlConnection, System.Data, Version=2.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089";
            ////providerXA = @"IBM.Data.DB2.iSeries.iDB2Connection, IBM.Data.DB2.iSeries, Version = 12.0.0.0, Culture = neutral, PublicKeyToken = 9cdb2ebfb1f93a26";

            ////ConnectionData.ProviderConnection = providerSQL;
            //////ConnectionData.ProviderConnectionERP = providerSQL;
            ////ConnectionData.ProviderConnectionERP = providerXA;
            //=================================================================
         
            ETL objEtl = new ETL();
            if (Parameter1 == "Extraction")
            {
         
                GlobalStrings.industrialNumber = 1;
                Console.WriteLine("Extraction---------------------");
                if (Parameter5 == "-")
                    Parameter5 = "";
                Parameter5 = Parameter5.Replace("|","@");
                GlobalStrings.V0CONO = Parameter4;
                GlobalStrings.V0SERIE = Parameter5;
                GlobalStrings.V0FOLIO = Parameter6;
                if (substringsERP[5].ToString().ToUpper() == "XA")
                {
                    GlobalStrings.ERP = substringsERP[5].ToString().ToUpper();
                    objEtl.PreviousXA(Parameter4, Parameter5, Parameter6);
                }
               
                objEtl.ValidateInvoiceType(Parameter4, Parameter5, Parameter6);
                Console.WriteLine("FIN Extraction---------------------");
            }
            else if (Parameter1 == "Message")
            {
             
                Console.WriteLine("Message---------------------");
              
                if (Parameter5 == "-")
                    Parameter5 = "";
                GlobalStrings.V0CONO = Parameter4;
                GlobalStrings.V0SERIE = Parameter5;
                GlobalStrings.V0FOLIO = Parameter6;
              
                if (!string.IsNullOrEmpty(Parameter11))
                    GlobalStrings.V9CFDIREL = Parameter11;
                
                //if (!string.IsNullOrEmpty(Parameter12))
                //    GlobalStrings.SiteERP = Parameter12;

                string msg = Parameter8.Replace("@", " ");
                Console.WriteLine(Parameter7);
                Console.WriteLine(msg);
                Console.WriteLine(Parameter9);
                Console.WriteLine(Parameter10);
                if (substringsERP[5].ToString().ToUpper() == "XA")
                {
                    ETL j = new ETL();
                    GlobalStrings.ERP = substringsERP[5].ToString().ToUpper();
                    objEtl.PreviousXA(Parameter4, Parameter5, Parameter6); Console.WriteLine("PreviousXA");
                    if (int.Parse(Parameter7) <= 10) //Bug de XA no regresar estados 10 para no recibir registros duplicados.
                    {
                        Parameter7 = "20";
                    }
                 
                    j.getpathCFDI(Parameter5);
                    objEtl.VoucherLog("ERP", int.Parse(Parameter7), msg, int.Parse(Parameter9), int.Parse(Parameter10)); Console.WriteLine("VoucherLog");
                }
                else
                {
                    ETL j = new ETL();
                    j.getpathCFDI(Parameter5);
                    var XML = Parameter13.Trim().Length == 1 ? Parameter13.Replace("-", "") : Parameter13;
                    if (!string.IsNullOrEmpty(XML))
                    {
                        Console.WriteLine("xml:"+Parameter13);
                        Console.WriteLine("Proceso de actualizacion documentos relacionados");
                        //string xml = Parameter13.Replace("$L", "<");
                        //var xml2 = xml.Replace("$G", ">");
                        byte[] myBase64ret = Convert.FromBase64String(XML);
                        string xml2 = System.Text.Encoding.UTF8.GetString(myBase64ret);
                        Console.WriteLine("ARMAR XML");
                        objEtl.UpdateDocRelated(xml2, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO, GlobalStrings.V0CONO);
                        Console.WriteLine("Fin proceso de actualizacion documentos relacionados");
                    }

                    if (GlobalStrings.SyncTableTaxSL)
                    {
                        //Seccion de informacion de impuestos para sincronizar a sl
                        var XMLImpuestosBase64 = Parameter14.Trim().Length == 1 ? Parameter14.Replace("-", "") : Parameter14;
                        if (!string.IsNullOrEmpty(XMLImpuestosBase64))
                        {
                            Console.WriteLine("Proceso de registro de impuestos");
                            //Convertir texto base 64 a string
                            byte[] myBase64ret = Convert.FromBase64String(XMLImpuestosBase64);
                            string xmlImpuestos = System.Text.Encoding.UTF8.GetString(myBase64ret);
                            objEtl.ProcessTaxses(xmlImpuestos);
                            Console.WriteLine("Fin Proceso de registro de impuestos");
                        }
                    }
                    if (GlobalStrings.DocumentCFDIRelated)
                    {
                        var XMLImpuestosBase64 = Parameter15.Trim().Length == 1 ? Parameter15.Replace("-", "") : Parameter15;
                        if (!string.IsNullOrEmpty(XMLImpuestosBase64))
                        {
                            Console.WriteLine("Proceso de registro de documentos relacionados a facturas");
                            //Convertir texto base 64 a string
                            byte[] myBase64ret = Convert.FromBase64String(XMLImpuestosBase64);
                            string xmlImpuestos = System.Text.Encoding.UTF8.GetString(myBase64ret);
                            objEtl.ProcessDocumentoCFDIDocRelated(xmlImpuestos);
                            Console.WriteLine("Fin Proceso de documentos relacionados a facturas");
                        }
                    }
                  
                   
                    
                        Console.WriteLine("GlobalStrings.V9CFDIREL" + GlobalStrings.V9CFDIREL);
                    objEtl.UpdateMXEIRQ("ERP", int.Parse(Parameter7), msg, int.Parse(Parameter9), int.Parse(Parameter10), GlobalStrings.V9CFDIREL);
                    //Actualizar HD
                    Console.WriteLine("UpdateHD");
                    var Fecha = Parameter12.Trim().Length == 1 ? Parameter12.Replace("-", "") : Parameter12;


                    if (!GlobalStrings.UpdateAditionalDataHD)
                        objEtl.UpdateMXEIHD("ERP", int.Parse(Parameter7), msg, int.Parse(Parameter9), int.Parse(Parameter10), Fecha);
                    else
                        objEtl.UpdateMXEIHDCUSTOM("ERP", int.Parse(Parameter7), msg, int.Parse(Parameter9), int.Parse(Parameter10), Fecha, Parameter16, Parameter17, Parameter18);


                    // objEtl.VoucherLog("ERP", int.Parse(Parameter7), msg, int.Parse(Parameter9), int.Parse(Parameter10)); Console.WriteLine("VoucherLog");
                }
                Console.WriteLine("Message FIN---------------------");
            }
            else if (Parameter1 == "Connection")
            {
                Console.WriteLine("Connection---------------------");
                Console.WriteLine(Parameter4);
                Console.WriteLine(substringsERP[5].ToString());
                if (objEtl.TestConnection(Parameter4, substringsERP[5].ToString()))
                    Console.WriteLine("ETL:ConexionExitosa");
                else
                    Console.WriteLine("ETL:ConexionFallida");
            }

          
            Console.WriteLine("--------------------------------");
            Console.WriteLine("Fin del DemoETL-CountryPack");
            //Console.ReadKey();
        }
    }
}
