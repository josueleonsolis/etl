using System;
using System.Collections.Generic;
using System.Linq;
using System.Configuration;
//------------------
using Rhino.Etl.Core;
using Rhino.Etl.Core.Operations;
using Rhino.Etl.Core.ConventionOperations;
using System.Xml;
using System.IO;
using System.Xml.Linq;
using System.Xml.Serialization;

namespace ETLCountryPack
{
    //JoinOperation
    public class JoinEmisor : JoinOperation
    {
        protected override Row MergeRows(Row leftRow, Row rightRow)
        {
            Row row = leftRow.Clone();

            row["version"] = rightRow["version"];
            row["lugarExpedicion"] = rightRow["lugarExpedicion"];
            row["SiteRef"] = rightRow["SiteRef"];
            return row;
        }
        protected override void SetupJoinConditions()
        {
            LeftJoin.Left("rfcEmisor").Right("rfcEmisor");
        }

    }
    public class JoinCustomer : JoinOperation
    {
        protected override Row MergeRows(Row leftRow, Row rightRow)
        {
            //Row row = leftRow.Clone();
            //row["formaPago"] = rightRow["formaPago"];
            //row["metodoPago"] = rightRow["metodoPago"];
            //row["rfcReceptor"] = rightRow["rfcReceptor"];
            Row row = rightRow.Clone();

            //Agregado por JL para resolver incidencia de prinsel cuando hay mas de un rfc igual
            row["cliente"] = leftRow["cliente"];
            //---------end---
            row["rfcEmisor"] = leftRow["rfcEmisor"];
            return row;
        }
        protected override void SetupJoinConditions()
        {
            LeftJoin.Left("cliente").Right("cliente");
        }

    }
    public class JoinCurrency : JoinOperation
    {
        protected override Row MergeRows(Row leftRow, Row rightRow)
        {
            Row row = leftRow.Clone();
            if (rightRow.Count != 0)
            {
                row["currency"] = rightRow["currency"];
            }
            return row;
        }
        protected override void SetupJoinConditions()
        {
            LeftJoin.Left("currency").Right("currencyERP");
        }

    }
    public class JoinNodoEmisor : JoinOperation
    {
        protected override Row MergeRows(Row leftRow, Row rightRow)
        {
            Row row = rightRow.Clone();

            row["UFT1"] = leftRow["UFT1"];
            row["UFD1"] = leftRow["UFD1"];
            return row;
        }
        protected override void SetupJoinConditions()
        {
            LeftJoin.Left("rfc").Right("rfc");
        }

    }
   
    public class JoinNodoReceptor1 : JoinOperation
    {
        protected override Row MergeRows(Row leftRow, Row rightRow)
        {
            Row row = leftRow.Clone();

            row["shipto"] = rightRow["shipto"];
            return row;
        }
        protected override void SetupJoinConditions()
        {
            LeftJoin.Left("rfc").Right("rfc");
        }

    }
    public class JoinNodoReceptor2 : JoinOperation
    {
        protected override Row MergeRows(Row leftRow, Row rightRow)
        {
            Row row = rightRow.Clone();
            row["UFT1"] = leftRow["UFT1"];
            row["UFD1"] = leftRow["UFD1"];
            row["address"] = leftRow["address"];
            row["shipto"] = leftRow["shipto"];
            row["rfc"] = leftRow["rfc"];
            return row;
        }
        protected override void SetupJoinConditions()
        {
            LeftJoin.Left("rfc").Right("rfc");
        }

    }

    //Agreagado por JL para el manejo de la configuracion de calculo de importes en conceptos
    public class JoinNodoConceptos : JoinOperation
    {
        private List<Conceptos> list;
        public JoinNodoConceptos(List<Conceptos> ListaConceptos)
        {
            this.list = ListaConceptos;
        }
        protected override Row MergeRows(Row leftRow, Row rightRow)
        {
            //SELECT V5IMPU AS impuestoERP,V5UUT1 AS UFT1, V5UUD1 AS UFD1, V5UUA1 AS UFAMT1, V5SEQN AS sequence  FROM MXEITX
            //--XA--//
            //Recupera el tipo del valor de la variable

            Row row = rightRow.Clone();
            row["UFT1"] = leftRow["UFT1"];
            row["UFD1"] = leftRow["UFD1"];
            row["UFAMT1"] = leftRow["UFAMT1"];
            row["total"] = leftRow["total"].ToString();

            //newid = (Guid)leftRow["taxConceptId"];
            String type = leftRow["taxConceptId"].GetType().Name;
            if (type.ToUpper() == "STRING")
            {
                if ((string)leftRow["taxConceptId"] == "00000000-0000-0000-0000-000000000000")
                {
                    row["taxConceptId"] = Guid.NewGuid();
                }
            }
            else
                row["taxConceptId"] = leftRow["taxConceptId"];

            int sequence = int.Parse(leftRow["sequence"].ToString());


            List<Conceptos> result = list.FindAll(a => a.linea == sequence);

            foreach (Conceptos ImpuestoConcepto in result)
            {
                
                //Console.WriteLine(ImpuestoConcepto.item);
                //Console.WriteLine(ImpuestoConcepto.conceptoId);
                //Console.WriteLine((Guid)row["taxConceptId"]);
                row["conceptId"] = ImpuestoConcepto.conceptoId;
                if (GlobalStrings.UseConfigDecimals)
                {
                    row["base"] = Math.Abs(Decimal.Round(ImpuestoConcepto.dbase, GlobalStrings.objVoucherDec.Concept_trasl_base));
                    row["total"] = Math.Abs(Decimal.Round(Decimal.Round(ImpuestoConcepto.dbase, GlobalStrings.objVoucherDec.Concept_trasl_base) * Decimal.Round((decimal)rightRow["tasaOquote"], GlobalStrings.objVoucherDec.Concept_trasl_tasaocuota), GlobalStrings.objVoucherDec.Concept_reten_import));
                }
                else
                {
                    row["base"] = Math.Abs(ImpuestoConcepto.dbase);
                    row["total"] = Math.Abs(ImpuestoConcepto.dbase * (decimal)rightRow["tasaOquote"]);
                }
            }
            return row;
        }
        protected override void SetupJoinConditions()
        {
            InnerJoin.Left("impuestoERP").Right("impuestoERP");
        }
    }
    //public class Impuestos : AbstractOperation
    //{
    //    public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
    //    {

    //        foreach (Row row in rows)
    //        {
    //            Console.WriteLine("ENTRE");
    //            row["base"] = Math.Round((decimal)row["base"], GlobalStrings.objVoucherDec.Concept_trasl_base);
    //            Console.WriteLine((decimal)row["base"]);
    //            Console.WriteLine(row["taxConceptId"]);
    //            if (int.Parse(row["Calculate"].ToString()) == 1)
    //            {
    //                if ((decimal)row["base"] > 0)
    //                    row["total"] = Math.Round((decimal)row["base"] * (decimal)row["tasaOquote"], GlobalStrings.objVoucherDec.Concept_trasl_base);
    //            }
    //            yield return row;
    //        }
    //    }
    //}
    /// <summary>
    /// 
    /// </summary>

    public class JoinNodoImpuestos : JoinOperation
    {
        private List<Conceptos> list;
        public JoinNodoImpuestos(List<Conceptos> ListaImpuestos)
        {
            this.list = ListaImpuestos;
        }
        protected override Row MergeRows(Row leftRow, Row rightRow)
        {
            //SELECT V5IMPU AS impuestoERP,V5UUT1 AS UFT1, V5UUD1 AS UFD1, V5UUA1 AS UFAMT1, V5SEQN AS sequence  FROM MXEITX
            //--XA--//
            //Recupera el tipo del valor de la variable          

            Row row = rightRow.Clone();
            row["UFT1"] = leftRow["UFT1"];
            row["UFD1"] = leftRow["UFD1"];
            row["UFAMT1"] = leftRow["UFAMT1"];
            row["total"] = leftRow["total"];
            row["base"] = leftRow["base"];
            //newid = (Guid)leftRow["taxConceptId"];
            String type = leftRow["taxConceptId"].GetType().Name;
            if (type.ToUpper() == "STRING")
            {
                if ((string)leftRow["taxConceptId"] == "00000000-0000-0000-0000-000000000000")
                {
                    row["taxConceptId"] = Guid.NewGuid();
                }
            }
            else
                row["taxConceptId"] = leftRow["taxConceptId"];

            int sequence = int.Parse(leftRow["sequence"].ToString());


            List<Conceptos> result = list.FindAll(a => a.linea == sequence);

            foreach (Conceptos ImpuestoConcepto in result)
            {
                //Console.WriteLine("JoinNodoImpuestos--------------------------------");
                //Console.WriteLine(ImpuestoConcepto.item);
                //Console.WriteLine(ImpuestoConcepto.conceptoId);
                //Console.WriteLine((Guid)row["taxConceptId"]);
                row["conceptId"] = ImpuestoConcepto.conceptoId;
                if (GlobalStrings.UseConfigDecimals)
                {
                    row["base"] = Math.Abs(Decimal.Round(ImpuestoConcepto.dbase, GlobalStrings.objVoucherDec.Concept_trasl_base));
                    row["total"] = Math.Abs(Decimal.Round(Decimal.Round(ImpuestoConcepto.dbase, GlobalStrings.objVoucherDec.Concept_trasl_base) * Decimal.Round((decimal)rightRow["tasaOquote"], GlobalStrings.objVoucherDec.Concept_trasl_tasaocuota), GlobalStrings.objVoucherDec.Concept_reten_import));
                }
                else
                {
                    row["base"] = Math.Abs(ImpuestoConcepto.dbase);
                    row["total"] = Math.Abs(ImpuestoConcepto.dbase * (decimal)rightRow["tasaOquote"]);
                }

            }

            return row;
        }

        protected override void SetupJoinConditions()
        {
            InnerJoin.Left("impuestoERP").Right("impuestoERP");
        }
        //public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        //{
        //    Console.WriteLine("ENTRE:"+rows.ToList().Count);
        //    foreach (Row row in rows)
        //    {
        //        Console.WriteLine("ENTRE");
        //        row["base"] = Math.Round((decimal)row["base"], GlobalStrings.objVoucherDec.Concept_trasl_base);


        //        //if (int.Parse(row["Calculate"].ToString()) == 1)
        //        //{
        //        //    if ((decimal)row["base"] > 0)
        //        //        row["total"] = Math.Round((decimal)row["base"] * (decimal)row["tasaOquote"], GlobalStrings.objVoucherDec.Concept_trasl_base);
        //        //}
        //        yield return row;
        //    }
        //}
    }

    //Operations
    public class LoadData : ConventionOutputCommandOperation
    {
        public LoadData(string query) : base(ConnectionData.ConexionSettingCFDI)
        {
            Command = query;
        }

    }
    public class LoadDataERP : ConventionOutputCommandOperation
     {
          public LoadDataERP(string query) : base(ConnectionData.ConexionSettingERP)
        {
          
            Command = query;
         }

}

public class ExtractData : ConventionInputCommandOperation
    {
        public ExtractData(string query) : base(ConnectionData.ConexionSettingERP)
        {
            Timeout = 90000000;
            // JL - 25052020 - Validacion Multisitio
            
            Command = (GlobalStrings.ERP != "XA") ? (GlobalStrings.UseMultisite) ? string.Format(GlobalStrings.SetSite, GlobalStrings.V0CONO) + ' ' + query : query : query;
        }

    }
    public class GetCompanyData : AbstractOperation
    {
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {

            foreach (Row row in rows)
            {

                GlobalStrings.rfcEmisor = (string)row["rfcEmisor"];
                GlobalStrings.version = (string)row["version"];

                //GlobalStrings.companyId = (string)row["comprobanteId"];
                GlobalStrings.lugarExpedicion = (string)row["lugarExpedicion"];
              //  GlobalStrings.SiteRef = (string)row["SiteRef"];
                //----
                GlobalStrings.formaPago = (string)row["formaPago"];
                GlobalStrings.metodoPago = (string)row["metodoPago"];
                GlobalStrings.rfcReceptor = (string)row["rfcReceptor"];
                GlobalStrings.cliente = (string)row["cliente"];              
                yield return row;
            }
        }
    }
    //Agregado por JL - 23102019 para el procesamiento de los detalles del complemento de vehiculos
    public class GetComplementCVDetData : AbstractOperation
    {
       
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {
            GlobalStrings.ListDetCV  = new List<CompleVehiDet>();
            foreach (Row row in rows)
            {
                CompleVehiDet obj = new CompleVehiDet();
                obj.Amount = (string)row["AMOUNT"];
                obj.CVDetalleID = (string)row["CVDETALLEID"];
                obj.Descr = (string)row["DESCRI"];
                obj.Folio = (string)row["FOLIO"];
                obj.Impo = (string)row["IMPO"];
                obj.NoIdent = (string)row["NOIDENT"];
                obj.PUnit = (string)row["PUNIT"];
                obj.Seque = (string)row["SEQUEN"];
                obj.Serie = (string)row["SERIE"];
                obj.Unit = (string)row["UNIT"];
                obj.VoucherID = (string)row["VOUCHERID"];
                GlobalStrings.ListDetCV.Add(obj);
                yield return row;
            }
        }
    }
    //
    public class GetIdComprobante : AbstractOperation
    {
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {

            foreach (Row row in rows)
            {
                //Recupera el tipo del valor de la variable
                String type = row["voucherId"].GetType().Name;
                if (type.ToUpper() == "STRING")
                {
                    if ((string)row["voucherId"] == "00000000-0000-0000-0000-000000000000")
                    {
                        row["voucherId"] = Guid.NewGuid();
                    }
                }
                GlobalStrings.comprobanteId = (Guid)row["voucherId"];

                if (!string.IsNullOrEmpty((string)row["voucherType"].ToString()))
                GlobalStrings.TypeCFDI = row["voucherType"].ToString().ToUpper();
                //Console.WriteLine(GlobalStrings.comprobante33Id);
                //------------------------Prueba
                //Para meter cambio de la moneda
                //ExNihiloGetCodigoImpuesto GetCodigoImpuesto = new ExNihiloGetCodigoImpuesto(InputData, Concepto);
                //GetCodigoImpuesto.Execute();
                //-------------------------
                yield return row;
            }
        }
    }

    public class MachConceptoKits : AbstractOperation
    {

        private string item;
        private string unit;
        private string unitIdERP;
        private string descripcion;
        //Agregado por JL  para los conceptos faltantes
        //private List<Conceptos> listNewConcept;
        //public MachConcepto(List<Conceptos> listaNewConcept)
        //{
        //    this.listNewConcept = listaNewConcept;
        //}
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {


            foreach (Row row in rows)
            {
                item = (string)row["noIdentification"].ToString().Trim();
                descripcion = (string)row["description"];
               unitIdERP = (string)row["unit"].ToString().Trim();


                ExNihiloGetMachConcepto getMachConcepto = new ExNihiloGetMachConcepto(item);
                getMachConcepto.UseTransaction = false;
                getMachConcepto.Execute();
                if (getMachConcepto.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in getMachConcepto.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
                if (getMachConcepto.ObjgetMachConcepto.getprodServId() == null)
                    row["prodServId"] = "";
                else
                    row["prodServId"] = getMachConcepto.ObjgetMachConcepto.getprodServId();
                //Se toma la descripcion de las tablas intermedias (MXEIDT) si es NULL se toma del CountryPack
                if (descripcion == "NULL")
                {
                    if (getMachConcepto.ObjgetMachConcepto.getdescription() == null)
                        row["description"] = "";
                    else
                        row["description"] = getMachConcepto.ObjgetMachConcepto.getdescription();
                }
                //Se comenta para mandar al xml la clave de la unidad del ERP
                //if (getMachConcepto.ObjgetMachConcepto.getunit() == null)
                //    row["unit"] = "";
                //else
                //    row["unit"] = getMachConcepto.ObjgetMachConcepto.getunit();
                //Se toma la clave UM de las tablas intermedias (MXEIDT) para enviar al xml la clave de la unidad del ERP
                row["unit"] = unitIdERP;
                //Se toma la clave UM de las tablas intermedias (MXEIDT) si es NULL se toma del CountryPack
                if (unitIdERP == "NULL")
                {
                    if (getMachConcepto.ObjgetMachConcepto.getunitId() == null)
                        row["unitId"] = "";
                    else
                        unitIdERP = getMachConcepto.ObjgetMachConcepto.getunitId().Trim();
                }

                //----------------------------Aqui estoy Programando
                ExNihiloGetclaveUnidad GetclaveUnidad = new ExNihiloGetclaveUnidad(unitIdERP);
                GetclaveUnidad.UseTransaction = false;
                GetclaveUnidad.Execute();

                if (GetclaveUnidad.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in GetclaveUnidad.GetAllErrors())
                    {
                        GlobalStrings.intRollback = 1;
                        GlobalStrings.msgNotExistUM = "Verifica que las unidades de medida de la factura esten registradas en CFDI3.3";
                        Console.WriteLine(ex.Message);

                    }
                }

              
                //row["total"] = Math.Round((decimal)row["total"], GlobalStrings.objVoucherDec.Concept_Imp_import);

                yield return row;

            }
        }
    }
    public class MachConcepto : AbstractOperation
    {

        private string item;
        private string unitId;
        private string unitIdERP;
        private string descripcion;
        //Agregado por JL  para los conceptos faltantes
        //private List<Conceptos> listNewConcept;
        //public MachConcepto(List<Conceptos> listaNewConcept)
        //{
        //    this.listNewConcept = listaNewConcept;
        //}
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {


            foreach (Row row in rows)
            {
                item = (string)row["noIdentification"].ToString().Trim();
                descripcion = (string)row["description"];
                unitIdERP = (string)row["unitId"].ToString().Trim();


                ExNihiloGetMachConcepto getMachConcepto = new ExNihiloGetMachConcepto(item);
                getMachConcepto.UseTransaction = false;
                getMachConcepto.Execute();
                if (getMachConcepto.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in getMachConcepto.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
                if (getMachConcepto.ObjgetMachConcepto.getprodServId() == null)
                    row["prodServId"] = "";
                else
                    row["prodServId"] = getMachConcepto.ObjgetMachConcepto.getprodServId();
                //Se toma la descripcion de las tablas intermedias (MXEIDT) si es NULL se toma del CountryPack
                if (descripcion == "NULL")
                {
                    if (getMachConcepto.ObjgetMachConcepto.getdescription() == null)
                        row["description"] = "";
                    else
                        row["description"] = getMachConcepto.ObjgetMachConcepto.getdescription();
                }
                //Se comenta para mandar al xml la clave de la unidad del ERP
                //if (getMachConcepto.ObjgetMachConcepto.getunit() == null)
                //    row["unit"] = "";
                //else
                //    row["unit"] = getMachConcepto.ObjgetMachConcepto.getunit();
                //Se toma la clave UM de las tablas intermedias (MXEIDT) para enviar al xml la clave de la unidad del ERP
                row["unit"] = unitIdERP;
                //Se toma la clave UM de las tablas intermedias (MXEIDT) si es NULL se toma del CountryPack
                if (unitIdERP == "NULL")
                {
                    if (getMachConcepto.ObjgetMachConcepto.getunitId() == null)
                        row["unitId"] = "";
                    else
                        unitIdERP = getMachConcepto.ObjgetMachConcepto.getunitId().Trim();
                }
               
                //----------------------------Aqui estoy Programando
                ExNihiloGetclaveUnidad GetclaveUnidad = new ExNihiloGetclaveUnidad(unitIdERP);
                GetclaveUnidad.UseTransaction = false;
                GetclaveUnidad.Execute();
               
                if (GetclaveUnidad.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in GetclaveUnidad.GetAllErrors())
                    {
                        GlobalStrings.intRollback = 1;
                        GlobalStrings.msgNotExistUM = "Verifica que las unidades de medida de la factura esten registradas en CFDI3.3";
                        Console.WriteLine(ex.Message);
                       
                    }
                }
              
                if (GlobalStrings.UseConfigDecimals)
                {
                    row["unitvalue"] = Math.Round((decimal)row["unitvalue"], GlobalStrings.objVoucherDec.Concept_PriceUnit);
                    row["quantity"] = Math.Round((decimal)row["quantity"], GlobalStrings.objVoucherDec.Concept_Amount);
                    row["discount"] = Math.Round((decimal)row["discount"], GlobalStrings.objVoucherDec.Concept_Discount);

                }
                else
                {
                    row["unitvalue"] = (decimal)row["unitvalue"];
                    row["quantity"] = (decimal)row["quantity"];
                    row["discount"] = (decimal)row["discount"];
                }
              
                row["unitId"] = GetclaveUnidad.Objgetclaveunidad.getclaveUnidad().Trim();
                decimal factorConversion = GetclaveUnidad.Objgetclaveunidad.getfactorConversion();
                //if (factorConversion == 1)
                //{
                //    row["total"] = (decimal)row["quantity"] * (decimal)row["unitvalue"];
                //}
            
                if (factorConversion != 1 & factorConversion != 0)//Es una conversión de unidades
                {
                    row["unit"] = GetclaveUnidad.Objgetclaveunidad.getnombreUnidad().Trim();
                    row["quantity"] = (decimal)row["quantity"] * factorConversion;
                    row["unitvalue"] = (decimal)row["unitvalue"] / factorConversion;
                    if (GlobalStrings.RecalculateImports)
                    {
                        if (GlobalStrings.UseConfigDecimals)
                            row["total"] = Decimal.Round(Decimal.Round((decimal)row["quantity"], GlobalStrings.objVoucherDec.Concept_Amount) * Math.Round((decimal)row["unitvalue"], GlobalStrings.objVoucherDec.Concept_PriceUnit), GlobalStrings.objVoucherDec.Concept_Imp_import);
                        else
                            row["total"] = (decimal)row["quantity"] * (decimal)row["unitvalue"];
                    }
                }

                //Agregado por JL para el manejo de los recalculos de los importes 
              
                if (GlobalStrings.ListConfiguraciones != null)
                {                   
                    if (GlobalStrings.ListConfiguraciones.FirstOrDefault().CalculateXML == 1)
                    {
                        if (GlobalStrings.RecalculateImports)
                        {
                            if (GlobalStrings.UseConfigDecimals)
                            {

                                row["total"] = Math.Round(Math.Round((decimal)row["quantity"], GlobalStrings.objVoucherDec.Concept_Amount) * Math.Round((decimal)row["unitvalue"], GlobalStrings.objVoucherDec.Concept_PriceUnit), GlobalStrings.objVoucherDec.Concept_Imp_import);
                            }
                            else
                            {

                                row["total"] = Math.Abs((decimal)row["quantity"] * (decimal)row["unitvalue"]);

                            }
                        }
                    }
                    else
                    {
                        if (GlobalStrings.RecalculateImports)
                        {
                            if (GlobalStrings.UseConfigDecimals)
                            {
                                row["total"] = Math.Round(Math.Round((decimal)row["quantity"], GlobalStrings.objVoucherDec.Concept_Amount) * Math.Round((decimal)row["unitvalue"], GlobalStrings.objVoucherDec.Concept_PriceUnit), GlobalStrings.objVoucherDec.Concept_Imp_import);
                            }
                             else
                                {                           
                                row["total"] = Math.Abs((decimal)row["quantity"] * (decimal)row["unitvalue"]);
                                }
                        }
                    }

                }
                else
                {
                    if (GlobalStrings.RecalculateImports)
                    {

                        //Si no logra recuperar configuraciones funciona de manera normal
                        if (GlobalStrings.UseConfigDecimals)
                        {
                            row["total"] = Math.Round(Math.Round((decimal)row["quantity"], GlobalStrings.objVoucherDec.Concept_Amount) * Math.Round((decimal)row["unitvalue"], GlobalStrings.objVoucherDec.Concept_PriceUnit), GlobalStrings.objVoucherDec.Concept_Imp_import);
                        }
                        else
                        {
                            row["total"] = Math.Abs((decimal)row["quantity"] * (decimal)row["unitvalue"]);
                        }
                    }
                }
                if (GlobalStrings.RecalculateImports)
                {
                    row["total"] = (decimal)row["total"];
                }
                //row["total"] = Math.Round((decimal)row["total"], GlobalStrings.objVoucherDec.Concept_Imp_import);
               
                yield return row;

            }
        }
    }
       
    public class GetMachConcepto : AbstractOperation
    {
        private string prodServId;
        //private string noIdentification;
        private string description;
        private string unit;
        private string unitId;
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {
            foreach (Row row in rows)
            {
                prodServId = (string)row["prodServId"];
                //noIdentification = (string)row["noIdentification"];
                description = (string)row["description"];
                unit = (string)row["unit"];
                unitId = (string)row["unitId"];
                
                yield return row;
            }
        }
        public string getprodServId()
        {
            return this.prodServId;
        }
        public string getdescription()
        {
            return this.description;
        }
        public string getunit()
        {
            return this.unit;
        }
        public string getunitId()
        {
            return this.unitId;
        }
    }
    public class GetclaveUnidad : AbstractOperation
    {
        private string claveUnidad;
        private string nombreUnidad;
        private decimal factorConversion;
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {

            foreach (Row row in rows)
            {
                claveUnidad = (string)row["claveUnidad"];
                factorConversion = (decimal)row["factor"];
                nombreUnidad = (string)row["nombreUnidad"];
                yield return row;
            }
        }
        public string getclaveUnidad()
        {
            return this.claveUnidad;
        }
        public decimal getfactorConversion()
        {
            return this.factorConversion;
        }
        public string getnombreUnidad()
        {
            return this.nombreUnidad;
        }
    }

    //Agregado por JL para el uso del cfdi
    public class GetMXEIHDUSECFDI : AbstractOperation
    {
       
        public GetMXEIHDUSECFDI()
        {
           
        }
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {
            foreach (Row row in rows)
            {
                if (row["V0USGE"] != null)
                    GlobalStrings.UseCFDI = row["V0USGE"].ToString();
                yield return row;
            }
        }
    }
    //Agregado por JL para el manejo calculo de impuestos
    public class GetConfiguraciones : AbstractOperation
    {
        private List<Configuraciones> list;
        public GetConfiguraciones()
        {
            this.list = new List<Configuraciones>();
        }
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {
            foreach (Row row in rows)
            {
                GlobalStrings.ListConfiguraciones = new List<Configuraciones>();
                GlobalStrings.ListConfiguraciones.Add(new Configuraciones() { CalculateXML = int.Parse(row["CalculateXML"].ToString()) });
                yield return row;
            }
        }
    }
    //Agregado por JL para el manejo de los decimales
    public class GetVoucherDecimals : AbstractOperation
    {
        private VoucherDecimals list;
        public GetVoucherDecimals()
        {
            this.list = new VoucherDecimals();
        }
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {
            VoucherDecimals OBJVou = new VoucherDecimals();
            foreach (Row row in rows)
            {
              
                if (row["Concept_Amount"] !=null)
                {
                  
                    if (int.Parse(row["Concept_Amount"].ToString()) > 0)
                        OBJVou.Concept_Amount = int.Parse(row["Concept_Amount"].ToString());
                    else
                        OBJVou.Concept_Amount = 2;
                }
                else
                {  OBJVou.Concept_Amount = 2; }
                   

                if (row["Concept_Discount"] != null)
                {
                  
                    if (int.Parse(row["Concept_Discount"].ToString()) > 0)
                        OBJVou.Concept_Discount = int.Parse(row["Concept_Discount"].ToString());
                    else
                        OBJVou.Concept_Discount = 2;
                }
                else { OBJVou.Concept_Discount = 2; }                   

                
                if (row["Concept_Imp_import"] != null)
                {
                    if (int.Parse(row["Concept_Imp_import"].ToString()) > 0)
                        OBJVou.Concept_Imp_import = int.Parse(row["Concept_Imp_import"].ToString());
                    else
                        OBJVou.Concept_Imp_import = 2;
                }
                else
                    OBJVou.Concept_Imp_import = 2;

                if (row["Concept_part_amount"] != null)
                {
                    if (int.Parse(row["Concept_part_amount"].ToString()) > 0)
                        OBJVou.Concept_part_amount = int.Parse(row["Concept_part_amount"].ToString());
                    else
                        OBJVou.Concept_part_amount = 2;
                }
                else
                    OBJVou.Concept_part_amount = 2;

                if (row["Concept_part_import"] != null)
                {
                    if (int.Parse(row["Concept_part_import"].ToString()) > 0)
                        OBJVou.Concept_part_import = int.Parse(row["Concept_part_import"].ToString());
                    else
                        OBJVou.Concept_part_import = 2;
                }
                else
                    OBJVou.Concept_part_import = 2;

                if (row["Concept_part_unitvalue"] != null)
                {
                    if (int.Parse(row["Concept_part_unitvalue"].ToString()) > 0)
                        OBJVou.Concept_part_unitvalue = int.Parse(row["Concept_part_unitvalue"].ToString());
                    else
                        OBJVou.Concept_part_unitvalue = 2;
                }
                else
                    OBJVou.Concept_part_unitvalue = 2;

                if (row["Concept_PriceUnit"] != null)
                {
                    if (int.Parse(row["Concept_PriceUnit"].ToString()) > 0)
                        OBJVou.Concept_PriceUnit = int.Parse(row["Concept_PriceUnit"].ToString());
                    else
                        OBJVou.Concept_PriceUnit = 2;
                }
                else
                    OBJVou.Concept_PriceUnit = 2;

                if (row["Concept_reten_base"] != null)
                {
                    if (int.Parse(row["Concept_reten_base"].ToString()) > 0)
                        OBJVou.Concept_reten_base = int.Parse(row["Concept_reten_base"].ToString());
                    else
                        OBJVou.Concept_reten_base = 2;
                }
                else
                    OBJVou.Concept_reten_base = 2;

                if (row["Concept_reten_tasaocuota"] != null)
                {
                    if (int.Parse(row["Concept_reten_tasaocuota"].ToString()) > 0)
                        OBJVou.Concept_reten_tasaocuota = int.Parse(row["Concept_reten_tasaocuota"].ToString());
                    else
                        OBJVou.Concept_reten_tasaocuota = 2;
                }
                else
                    OBJVou.Concept_reten_tasaocuota = 2;

                if (row["Concept_trasl_base"] != null)
                {
                    if (int.Parse(row["Concept_trasl_base"].ToString()) > 0)
                        OBJVou.Concept_trasl_base = int.Parse(row["Concept_trasl_base"].ToString());
                    else
                        OBJVou.Concept_trasl_base = 2;
                }
                else
                    OBJVou.Concept_trasl_base = 2;

                if (row["Concept_trasl_tasaocuota"] != null)
                {
                    if (int.Parse(row["Concept_trasl_tasaocuota"].ToString()) > 0)
                        OBJVou.Concept_trasl_tasaocuota = int.Parse(row["Concept_trasl_tasaocuota"].ToString());
                    else
                        OBJVou.Concept_trasl_tasaocuota = 2;
                }
                else
                    OBJVou.Concept_trasl_tasaocuota = 2;

                if (row["Concept_reten_import"] != null)
                {
                    if (int.Parse(row["Concept_reten_import"].ToString()) > 0)
                        OBJVou.Concept_reten_import = int.Parse(row["Concept_reten_import"].ToString());
                    else
                        OBJVou.Concept_reten_import = 2;
                }
                else
                    OBJVou.Concept_reten_import = 2;

                if (row["Concept_trasl_import"] != null)
                {
                    if (int.Parse(row["Concept_trasl_import"].ToString()) > 0)
                        OBJVou.Concept_trasl_import = int.Parse(row["Concept_trasl_import"].ToString());
                    else
                        OBJVou.Concept_trasl_import = 2;
                }
                else
                    OBJVou.Concept_trasl_import = 2;


                GlobalStrings.objVoucherDec = new VoucherDecimals();
                GlobalStrings.objVoucherDec = OBJVou;
                yield return row;
            }
        }
    }
    //Agregado por JL para la validacion de la existencia de las columnas
    public class ValisteExistColumn : AbstractOperation
    {
       
        public ValisteExistColumn()
        {
            
        }
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {
            foreach (Row row in rows)
            {
                GlobalStrings.ValidateCol = row["Existe"].ToString();
                yield return row;
            }
        }
    }
    //
    //Agregado por JL-23102019 para validar la existencia de las tablas
    public class ValisteExistTable : AbstractOperation
    {
        private string ExitsT;
        public ValisteExistTable()
        {
            ExitsT = "";
        }
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {
            foreach (Row row in rows)
            {
                ExitsT = row["Existe"].ToString();
                yield return row;
            }
        }
        public string getIfExistsTable()
        {
            return this.ExitsT;
        }
    }
    //
    public class GetIdConcepto : AbstractOperation
    {
        private List<Conceptos> list;
        public GetIdConcepto(List<Conceptos> ListaConcepto)
        {
            this.list = ListaConcepto;
        }
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {

            foreach (Row row in rows)
            {

                ////Conceptos con = new Conceptos();
                ////con.conceptoId = (Guid)row["conceptId"];
                ////con.item = (string)row["noIdentification"];
                ////con.linea = int.Parse(row["sequence"].ToString());
                ////String t = row["sequence"].GetType().Name;
                ////Console.WriteLine("HOLA FELIPE: -- " + t);
                ////this.list.Add(con); //, conceptoId = (Guid)row["ID"] });
                //--XA--//
                //Recupera el tipo del valor de la variable
                String type = row["conceptId"].GetType().Name;
                if (type.ToUpper() == "STRING")
                {
                    if ((string)row["conceptId"] == "00000000-0000-0000-0000-000000000000")
                    {
                        row["conceptId"] = Guid.NewGuid();                      
                    }
                    else
                    {                     
                        row["conceptId"] = Guid.Parse(row["conceptId"].ToString());
                    }
                }
             
                decimal calculo = 0.0m;
                if (GlobalStrings.UseConfigDecimals)
                {
                    calculo = Decimal.Round(Decimal.Round((decimal)row["total"], GlobalStrings.objVoucherDec.Concept_trasl_base) - Math.Round((decimal)row["discount"], GlobalStrings.objVoucherDec.Concept_Discount), GlobalStrings.objVoucherDec.Concept_trasl_base);
                }
                else
                {
                    calculo = Math.Abs((decimal)row["total"] - (decimal)row["discount"]);
                }


                this.list.Add(new Conceptos() { conceptoId = (Guid)row["conceptId"], item = (string)row["noIdentification"].ToString().Trim(), linea = int.Parse(row["sequence"].ToString()), dbase = calculo });
                //this.list.Add(new Conceptos() { conceptoId = (Guid)row["conceptId"], item = (string)row["noIdentification"], linea = int.Parse(row["sequence"].ToString()), dbase = (Math.Round((decimal)row["total"], cantdecimalesImporte) - Math.Round((decimal)row["discount"],cantdecimalesDesc)) }  );
                //this.list.Add(new Conceptos() { conceptoId = (Guid)row["conceptId"], item = (string)row["noIdentification"], linea = int.Parse(row["sequence"].ToString()), dbase = ((decimal)row["total"]) });
                yield return row;
            }
        }
    }
    public class FormatPedimento : AbstractOperation
    {
        private string pedimento;
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {
            foreach (Row row in rows)
            {
                //Console.WriteLine((string)row["pedimentNumber"]);
                pedimento = formaPedimento((string)row["pedimentNumber"]);
                //Console.WriteLine(pedimento);
                row["pedimentNumber"] = pedimento;
                yield return row;
            }
        }
        public String formaPedimento(string pedimento)
        {
            pedimento = pedimento.Replace(" ", "");

            if (pedimento.Length == 15)
            {

                try
                {
                    // pedimento = string.Format("{0:##  ##  ####  #######}", long.Parse(pedimento));
                    pedimento = string.Format("{0:##  ##  ####  #######}", pedimento);
                }
                catch (Exception e)
                {
                    return pedimento;
                }

                return pedimento;
            }
            else
            {
                return pedimento;
            }
        }
    }
    public class ExistsRegistros : AbstractOperation 
    {
        public string Exists;
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {
            foreach (Row row in rows)
            {
                Exists = (string)row["Exist"];
                yield return row;
            }
        }
        public string getIfExistsRegistros()
        {
         
            return this.Exists;
        }
    }
    public class StatusVoucher : AbstractOperation
    {
        public string StatusId;
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {
            foreach (Row row in rows)
            {
                StatusId = (string)row["Status"];
                if (StatusId == "UPDATE" || StatusId == "TIMBRADA")
                {
                    //GlobalStrings.SiteRef = (string)row["SiteRef"];
                    GlobalStrings.comprobanteId = (Guid)row["comprobanteId"];
                }
                yield return row;
            }
        }
        public string getStatusVoucher()
        {
            return this.StatusId;
        }

    }
    public class ValidaVoucherLog : AbstractOperation
    {
        public string ExistRegistro;
        private string queryInsert;
        private string queryUpdate;
        private string query;
        private string sistema;
        public ValidaVoucherLog(string Sistema,string Insert,string Update)
        {
            this.queryInsert = Insert;
            this.queryUpdate = Update;
            this.sistema = Sistema;
        }
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {
            foreach (Row row in rows)
            {
                ExistRegistro = (string)row["Exist"];
                if (ExistRegistro == "0")
                    query = queryInsert; //Insert
                else
                    query = queryUpdate; //Update
                Console.WriteLine("ValidaVoucherLog---------------");
                ExNihiloWriteVoucherLog WriteVoucherLog = new ExNihiloWriteVoucherLog(sistema,query);
                WriteVoucherLog.UseTransaction = false;
                WriteVoucherLog.Execute();
                if (WriteVoucherLog.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in WriteVoucherLog.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
                yield return row;
            }
        }
        public string getIfExistRegistro()
        {
            return this.ExistRegistro;
        }
    }
    public class GetInvoiceType : AbstractOperation
    {
        public string invoceType;
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {
            
            foreach (Row row in rows)
            {              
                invoceType = (string)row["TipoComprobante"];
                
                //if (StatusId == "UPDATE")
                //    GlobalStrings.comprobanteId = (Guid)row["comprobanteId"];
                yield return row;
            }           
        }
        public string getInvoiceType()
        {
            return this.invoceType;
        }
    }
    public class GetSchema : AbstractOperation
    {
        public string schema;
        public string timestamp;
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {
            foreach (Row row in rows)
            {
                schema = (string)row["SCHEMA"];
                //timestamp = (row["TIMESTAMP"].ToString()).TrimEnd();
                timestamp = ((string)row["TIMESTAMP"]).TrimEnd();
                yield return row;
            }
        }
        public string getSchemaXA()
        {
            return this.schema;
        }
        public string getTimeStamp()
        {
            return this.timestamp;
        }
    }
    //Process
    public class ExNihiloGetCompanyData : EtlProcess
    {
        protected override void Initialize()
        {
            string InputDataERP = string.Format(GlobalStrings.SelectrfcEmisorERP, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
            Register(new JoinEmisor()
                .Left(new JoinCustomer()//JoinEmisor()
                    .Left(new ConventionInputCommandOperation(ConnectionData.ConexionSettingERP)
                    {

                        // JL - 25052020 - Validacion Multisitio
                        Command = (GlobalStrings.ERP != "XA") ? (GlobalStrings.UseMultisite) ? string.Format(GlobalStrings.SetSite, GlobalStrings.V0CONO) + ' ' + InputDataERP : InputDataERP : InputDataERP,
                       // Command = string.Format(GlobalStrings.SetSite, GlobalStrings.V0CONO) +' '+InputDataERP,
                        Timeout = 9000000
                    })
                    .Right(new ConventionInputCommandOperation(ConnectionData.ConexionSettingCFDI)
                    {
                        Command = GlobalStrings.SelectClienteIdCountryPack,
                        Timeout = 9000000
                    }))
                .Right(new ConventionInputCommandOperation(ConnectionData.ConexionSettingCFDI)
                {
                    Command = GlobalStrings.SelectrfcEmisorCountryPack,
                    Timeout = 9000000
                })
                );
            Register(new GetCompanyData());
        }
    }
    public class ExNihiloNodoComprobante : EtlProcess
    {
        private string querySelect;
        private string queryInsert;
        //private string querySelect2;
        public ExNihiloNodoComprobante(string InputData, string OutputData)
        {
            this.querySelect = InputData;
            this.queryInsert = OutputData;
        }
        protected override void Initialize()
        {
            Register(new JoinCurrency()
                .Left(new ExtractData(querySelect)
                    )
                 .Right(new ConventionInputCommandOperation(ConnectionData.ConexionSettingCFDI)
                 {                     
                     Command = GlobalStrings.SelectMonedaCountryPack,
                     Timeout = 9000000
                 })
                );
            Register(new GetIdComprobante());
            Register(new LoadData(queryInsert));
           
        }
    }
    public class ExNihiloNodoEmisor : EtlProcess
    {
        private string querySelect1;
        private string querySelect2;
        private string queryInsert;

        public ExNihiloNodoEmisor(string InputData1, string InputData2, string OutputData)
        {
            this.querySelect1 = InputData1;
            this.querySelect2 = InputData2;
            this.queryInsert = OutputData;
        }
        protected override void Initialize()
        {
            Register(new JoinNodoEmisor()
                .Left(new ExtractData(querySelect1)
                    )
                .Right(new ConventionInputCommandOperation(ConnectionData.ConexionSettingCFDI)
                {
                    Command = querySelect2,
                    Timeout = 9000000
                }));
            Register(new LoadData(queryInsert));
        }
    }

    //Agregadi por JL para el manejo de impuestos multisitio
    public class ExNihiloTaxesMultisite : EtlProcess
    {
        private string querySelect1;
        private string querySelect2;
        private string queryInsert;

        public ExNihiloTaxesMultisite(string InputData1, string InputData2, string OutputData)
        {
            this.querySelect1 = InputData1;
            this.querySelect2 = InputData2;
            this.queryInsert = OutputData;
        }
        protected override void Initialize()
        {
            Register(new JoinNodoEmisor()
                .Left(new ExtractData(querySelect1)
                    )
                .Right(new ConventionInputCommandOperation(ConnectionData.ConexionSettingCFDI)
                {
                    Command = querySelect2,
                    Timeout = 9000000
                }));
            Register(new LoadData(queryInsert));
        }
    }
    //Agregado por JL para buscar usecfdi
    public class ExNihiloMXEIHDUSECFDI : EtlProcess
    {
        private string querySelect1;
        public ExNihiloMXEIHDUSECFDI(string InputData1)
        {
            this.querySelect1 = InputData1;
           
        }
        protected override void Initialize()
        {
            Register(new ConventionInputCommandOperation(ConnectionData.ConexionSettingERP)
            {
                Command = querySelect1,
                Timeout = 9000000
            });
            Register(new GetMXEIHDUSECFDI());
        }
    }
    public class ExNihiloNodoReceptor : EtlProcess
    {
        private string querySelect1;
        private string querySelect2;
        private string querySelect3;
        private string queryInsert;

        public ExNihiloNodoReceptor(string InputData1, string InputData2, string InputData3, string OutputData)
        {
            this.querySelect1 = InputData1;
            this.querySelect2 = InputData2;
            this.querySelect3 = InputData3;
            this.queryInsert = OutputData;
        }
        protected override void Initialize()
        {
            Register(new JoinNodoReceptor2()
                .Left(
                    new JoinNodoReceptor1()
                    .Left(new ExtractData(querySelect1))
                    .Right(new ExtractData(querySelect2))
                    )
                .Right(new ConventionInputCommandOperation(ConnectionData.ConexionSettingCFDI)
                {
                    Command = querySelect3,
                    Timeout = 9000000
                })

                );
            Register(new LoadData(queryInsert));
        }
    }
    public class ExNihiloNodoConcepto : EtlProcess
    {
        private string querySelect;
        private string queryInsert;
        private List<Conceptos> list;
        //Agregado por JL para registrar items faltantes en factura
        private List<Conceptos> listNewConept;
        //private string querySelect2;
        public ExNihiloNodoConcepto(string InputData, string OutputData, List<Conceptos> ListaConcepto)
        {
            this.querySelect = InputData;
            this.queryInsert = OutputData;
            this.list = ListaConcepto;
           
        }
        protected override void Initialize()
        {
            //Console.WriteLine("antes de conceptos");
            //Console.WriteLine(querySelect);
            Register(new ExtractData(querySelect));
            //Console.WriteLine("despues de conceptos");
            //Console.WriteLine("match");
            Register(new MachConcepto());
            //Console.WriteLine("fin de match");
            //Console.WriteLine("unidad");
            Register(new GetIdConcepto(list));
            //Console.WriteLine("fin de unidad");
            Register(new LoadData(queryInsert));
        }
        public List<Conceptos> Getlista()
        {
            return this.list;
        }
    }


    public class ExNihiloNodoKits : EtlProcess
    {
        private string querySelect;
        private string queryInsert;
      
        public ExNihiloNodoKits(string InputData, string OutputData)
        {
            this.querySelect = InputData;
            this.queryInsert = OutputData;        

        }
        protected override void Initialize()
        {
            
            Register(new ExtractData(querySelect));

            Register(new MachConceptoKits());
           
          
            
            Register(new LoadData(queryInsert));
        }
      
    }

    public class ExNihiloPedimentosConceptos : EtlProcess
    {
        private string querySelect;
        private string queryInsert;
        public ExNihiloPedimentosConceptos(string InputData, string OutputData)
        {
            this.querySelect = InputData;
            this.queryInsert = OutputData;
        }
        protected override void Initialize()
        {
            Register(new ExtractData(querySelect));
            Register(new FormatPedimento());
            Register(new LoadData(queryInsert));
        }
    }
    //AGregado por jl para el manejo de la configuracion de calcul de importe
    public class ExNihiloNodoConfiguracion : EtlProcess
    {
        private string querySelect;

        private List<Configuraciones> list;
        //private string querySelect2;
        public ExNihiloNodoConfiguracion(string InputData)
        {
            this.querySelect = InputData;
            // this.queryInsert = OutputData;

        }
        protected override void Initialize()
        {
            Register(new ConventionInputCommandOperation(ConnectionData.ConexionSettingCFDI)
            {
                Command = querySelect,
                Timeout = 9000000
            });
            Register(new GetConfiguraciones());
        }
    }
    //AGregado por jl para el manejo de la recuperacion de los decimales
    public class ExNihiloRecuperarVoucherDecimal : EtlProcess
    {
        private string querySelect;
       
        //private string querySelect2;
        public ExNihiloRecuperarVoucherDecimal(string InputData)
        {
            this.querySelect = InputData;
            // this.queryInsert = OutputData;

        }
        protected override void Initialize()
        {
            Register(new ConventionInputCommandOperation(ConnectionData.ConexionSettingCFDI)
            {
                Command = querySelect,
                Timeout = 9000000
            });
            Register(new GetVoucherDecimals());
        }
    }
    //AGregado por jl para el manejo de la configuracion de calcul de importe
    public class ExNihiloValidarExistenciaColumna: EtlProcess
    {
        private string querySelect;  
        public ExNihiloValidarExistenciaColumna(string InputData)
        {
            this.querySelect = InputData;           
        }
        protected override void Initialize()
        {
            Register(new ConventionInputCommandOperation(ConnectionData.ConexionSettingCFDI)
            {
                Command = querySelect,
                Timeout = 9000000
            });
            Register(new ValisteExistColumn());
        }
    }
    public class ExNihiloValidarExistenciaColumnaSL : EtlProcess
    {
        private string querySelect;
        public ExNihiloValidarExistenciaColumnaSL(string InputData)
        {
            this.querySelect = InputData;
        }
        protected override void Initialize()
        {
            Register(new ConventionInputCommandOperation(ConnectionData.ConexionSettingERP)
            {
                Command = querySelect,
                Timeout = 9000000
            });
            Register(new ValisteExistColumn());
        }
    }
    //
    //Agregado por JL -23102019 para la validacion de la existencia de las tablas
    public class ExNihiloValidarExistenciaTablaSL : EtlProcess
    {        
        public ValisteExistTable ObjgetExists;
        private string querySelect;
        public ExNihiloValidarExistenciaTablaSL(string InputData)
        {
            this.querySelect = InputData;
        }
        protected override void Initialize()
        {
            Register(new ConventionInputCommandOperation(ConnectionData.ConexionSettingERP)
            {
                // JL - 25052020 - Validacion Multisitio
                Command = (GlobalStrings.ERP != "XA") ? (GlobalStrings.UseMultisite) ? string.Format(GlobalStrings.SetSite, GlobalStrings.V0CONO) + ' ' + querySelect : querySelect : querySelect,
                // Command = string.Format(GlobalStrings.SetSite, GlobalStrings.V0CONO) + ' ' + querySelect,
                Timeout = 9000000
            });
            this.ObjgetExists = new ValisteExistTable();
            Register(ObjgetExists);
        }
    }
    public class ExNihiloValidarExistenciaTablaCS : EtlProcess
    {
        public ValisteExistTable ObjgetExists;
        private string querySelect;
        public ExNihiloValidarExistenciaTablaCS(string InputData)
        {
            this.querySelect = InputData;
        }
        protected override void Initialize()
        {
            Register(new ConventionInputCommandOperation(ConnectionData.ConexionSettingCFDI)
            {
                Command = querySelect,
                Timeout = 9000000
            });
            this.ObjgetExists = new ValisteExistTable();
            Register(ObjgetExists);
        }
    }
    //

    public class ExNihiloGetMachConcepto : EtlProcess
    {
        private string querySelect;
        //private string queryInsert;
        //private string querySelect2;
        public GetMachConcepto ObjgetMachConcepto;
        public ExNihiloGetMachConcepto(string item)
        {
            //this.querySelect1 = string.Format(GlobalStrings.SelectConceptoERP, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
            this.querySelect = string.Format(GlobalStrings.SelectConceptoCountryPack, item, GlobalStrings.cliente);
        }
        protected override void Initialize()
        {            
            Register(new ConventionInputCommandOperation(ConnectionData.ConexionSettingCFDI)
            { 
                Command = querySelect,
                 Timeout = 9000000
            }
                    );
            this.ObjgetMachConcepto = new GetMachConcepto();
            Register(ObjgetMachConcepto);
        }
    }
    public class ExNihiloGetclaveUnidad : EtlProcess
    {
        private string querySelect;
        public GetclaveUnidad Objgetclaveunidad;
        public ExNihiloGetclaveUnidad(string unidad)
        {
            this.querySelect = string.Format(GlobalStrings.SelecClaveUnidad, unidad);
        }
        protected override void Initialize()
        {
           
            Register(new ConventionInputCommandOperation(ConnectionData.ConexionSettingCFDI)
            {
                Command = querySelect,
                Timeout = 9000000
            });
            this.Objgetclaveunidad = new GetclaveUnidad();
            Register(Objgetclaveunidad);
        }
    }
    public class ExNihiloNodoImpuestos : EtlProcess
    {
        private string querySelect;
        private string queryInsert;
        private List<Conceptos> list;
        public ExNihiloNodoImpuestos(string InputData, string OutputData, List<Conceptos> ListaImpuestos)
        {
            this.querySelect = InputData;
            this.queryInsert = OutputData;
            this.list = ListaImpuestos;
        }
        protected override void Initialize()
        {
            Register(new JoinNodoImpuestos(list)
                    .Left(new ExtractData(querySelect))
                    .Right(new ConventionInputCommandOperation(ConnectionData.ConexionSettingCFDI)
                    {
                        Command = GlobalStrings.SelectImpuestosCountryPack,
                        Timeout = 9000000
                    })
                );
           
            Register(new LoadData(queryInsert));
        }
    }
    public class ExNihiloExists : EtlProcess
    {
        private string querySelect;
        //private string queryInsert;
        public ExistsRegistros ObjgetExists;
        //private string ExistAddenda;
        public ExNihiloExists(string InputData)
        {
            this.querySelect = InputData;
        }
        protected override void Initialize()
        {
            Register(new ExtractData(querySelect));
            this.ObjgetExists = new ExistsRegistros();
            Register(ObjgetExists);
        }
    }
    //Agregado por JL para validar registros en CS
    public class ExNihiloExistsCS : EtlProcess
    {
        private string querySelect;
        //private string queryInsert;
        public ExistsRegistros ObjgetExists;
        //private string ExistAddenda;
        public ExNihiloExistsCS(string InputData)
        {
            this.querySelect = InputData;
        }
        protected override void Initialize()
        {
            
            Register(new ConventionInputCommandOperation(ConnectionData.ConexionSettingCFDI)
            {

                Timeout = 9000000,
                Command = querySelect
                //Command = querySelect
            });
         
            this.ObjgetExists = new ExistsRegistros();
            Register(ObjgetExists);
        }
    }
    
    public class ExNihiloNodoAddenda : EtlProcess
    {
        private string querySelect;
        private string queryInsert;
        //private ExisteAddenda ObjgetAddenda;
        //private string ExistAddenda;
        public ExNihiloNodoAddenda(string InputData, string OutputData)
        {
            this.querySelect = InputData;
            this.queryInsert = OutputData;
        }
        protected override void Initialize()
        {
            Register(new ExtractData(querySelect));
            //this.ObjgetAddenda = new ExisteAddenda();
            //Register(ObjgetAddenda);
            //Console.WriteLine("Addenda");
            //Console.WriteLine(ObjgetAddenda.ExistAddenda); 
            //if (ObjgetAddenda.ExistAddenda == "1")
            Register(new LoadData(queryInsert));
        }
    }
    public class ExNihiloExec_SP : EtlProcess
    {
        private string querySelect;
        private string queryInsert;
        public ExNihiloExec_SP(string InputData,string OutputData)
        {
            this.querySelect = InputData;
            this.queryInsert = OutputData;
        }
        protected override void Initialize()
        {
            //Cambiado por JL para resolver problema de tiempo de espera al ejecutar el VoucherGenSP          
            Register(new ConventionInputCommandOperation(ConnectionData.ConexionSettingCFDI)
            {
              
                Timeout = 9000000,
                Command = queryInsert
                //Command = querySelect
            });
            //Register(new LoadData(queryInsert));
        }
    }
    public class ExNihiloVoucherLog : EtlProcess
    {
        private string querySelect;
        private string queryInsert;
        private string queryUpdate;
       //agregado por jl para la insercion en hd cuando no haya registros en rq para xa
        private string queryUpdatehd;
        //
        // private string queryUpdate2;
        private string sistema;
        //Modificado por jl para la insercion en hd cuando no haya registros en rq para xa
        public ExNihiloVoucherLog(string Sistema, string InputData, string OutputDataInsert, string OutputDataUpdateRQ, string OutputDataHD)
        {
            this.querySelect = InputData;
            this.queryInsert = OutputDataInsert;
            //Modificado por jl para la insercion en hd cuando no haya registros en rq para xa
            this.queryUpdate = OutputDataUpdateRQ;           
            this.queryUpdatehd = OutputDataHD;
            //
            //this.queryUpdate2 = OutputDataUpdate2;
            this.sistema = Sistema;
        }
        protected override void Initialize()
        {
            if ((sistema == "CountryPack") || (sistema == "ERP" && GlobalStrings.ERP != "XA"))
            {
               
                Register(new ConventionInputCommandOperation(sistema=="ERP"?ConnectionData.ConexionSettingERP:ConnectionData.ConexionSettingCFDI)
                {
                    Command = querySelect,
                    Timeout = 9000000,
                });
                Register(new ValidaVoucherLog(sistema, queryInsert, queryUpdate));
            }
            else
                //Modificado por jl para la insercion en hd cuando no haya registros en rq para xa
                writeXA(queryUpdatehd, queryUpdate, querySelect);
                
            //string sql = String.Format("insert into zmx_voucherlog (comprobantestatusid, errorMensaje,comprobanteId,siteRef,fechacreacion) values(0,'---','{0}','COUNTRYP',getdate())", GlobalStrings.comprobanteId);
            //Register(new ExtractData("select 1"));
            //Register(new LoadData(sql));
        }
        //Modificado por jl para la insercion en hd cuando no haya registros en rq para xa
        public void writeXA(string OutputDataHD, string OutputDataRQ, string inputData)
        {
            Message.Message objEtl = new Message.Message(inputData, OutputDataRQ, OutputDataHD);
        }
    }
    public class ExNihiloWriteVoucherLog : EtlProcess
    {
        private string querySelect;
        private string queryInsert;
        private string sistema;
        public ExNihiloWriteVoucherLog(string Sistema,string OutputData)
        {
            //this.querySelect = InputData;
            this.queryInsert = OutputData;
            this.sistema = Sistema;
            if (sistema == "ERP")
                querySelect = (GlobalStrings.ERP != "XA") ? (GlobalStrings.UseMultisite) ? string.Format(GlobalStrings.SetSite, GlobalStrings.V0CONO) + ' ' + GlobalStrings.InicializaRegistroERP : GlobalStrings.InicializaRegistroERP : GlobalStrings.InicializaRegistroERP;
            //querySelect = (GlobalStrings.ERP != "XA") ? string.Format(GlobalStrings.SetSite, GlobalStrings.V0CONO) + ' ' + GlobalStrings.InicializaRegistroERP : GlobalStrings.InicializaRegistroERP;
            else
                querySelect = GlobalStrings.InicializaRegistroCountryPack;
           
        }
        protected override void Initialize()
        {           
            Register(new ConventionInputCommandOperation(sistema=="ERP" ? ConnectionData.ConexionSettingERP : ConnectionData.ConexionSettingCFDI)
            {
                Command = querySelect,
                Timeout = 9000000,
            });
            Register(new ConventionOutputCommandOperation(sistema == "ERP" ? ConnectionData.ConexionSettingERP : ConnectionData.ConexionSettingCFDI)
            {
                Command = queryInsert
            });
            //Register(new LoadData(queryInsert));

        }
    }
    public class ExNihiloVerificaStatusVoucher : EtlProcess
    {
        public StatusVoucher ObjgetStatus; 
        private string querySelect;
        public ExNihiloVerificaStatusVoucher(string InputData)
        {
            this.querySelect = InputData;
        }
        protected override void Initialize()
        {
            Register(new ConventionInputCommandOperation(ConnectionData.ConexionSettingCFDI)
            {
                Command = querySelect,
                Timeout = 9000000,
            });
            this.ObjgetStatus = new StatusVoucher();
            Register(ObjgetStatus);
        }
    }
    public class ExNihiloRollback : EtlProcess
    {
        private string querySelect;
        private string queryInsert;
        public ExNihiloRollback(string InputData, string OutputData)
        {
            this.querySelect = InputData;
            this.queryInsert = OutputData;
        }
        protected override void Initialize()
        {
            Register(new ConventionInputCommandOperation(ConnectionData.ConexionSettingCFDI)
            {
                Command = querySelect,
                Timeout = 9000000,
            });
            Register(new LoadData(queryInsert));
        }
    }
    public class ExNihiloInvoiceType : EtlProcess
    {
        public GetInvoiceType ObjgetType;
        private string querySelect;
        public ExNihiloInvoiceType(string InputData)
        {
            this.querySelect = InputData;
        }
        protected override void Initialize()
        {
            Register(new ConventionInputCommandOperation(ConnectionData.ConexionSettingERP)
            {
                Command = (GlobalStrings.ERP != "XA") ? (GlobalStrings.UseMultisite) ? string.Format(GlobalStrings.SetSite, GlobalStrings.V0CONO) + ' ' + querySelect : querySelect : querySelect,
               
            //Command = (GlobalStrings.ERP != "XA") ? string.Format(GlobalStrings.SetSite, GlobalStrings.V0CONO) + ' ' + querySelect : querySelect,                
            Timeout = 9000000,
            });

           
            this.ObjgetType = new GetInvoiceType();
            Register(ObjgetType);
        }
    }
    public class ExNihiloSetSite : EtlProcess
    {
      
        private string querySelect;
        public ExNihiloSetSite(string InputData)
        {
            this.querySelect = InputData;
            Console.WriteLine((GlobalStrings.ERP != "XA") ? (GlobalStrings.UseMultisite) ? string.Format(GlobalStrings.SetSite, GlobalStrings.V0CONO) + ' ' + querySelect : querySelect : querySelect);
        }
        protected override void Initialize()
        {         
               
            Register(new ConventionInputCommandOperation(ConnectionData.ConexionSettingERP)
            {
                Command = (GlobalStrings.ERP != "XA") ? (GlobalStrings.UseMultisite) ? string.Format(GlobalStrings.SetSite, GlobalStrings.V0CONO) + ' ' + querySelect : querySelect : querySelect,
               
                // Command =  (GlobalStrings.ERP != "XA") ? string.Format(GlobalStrings.SetSite, GlobalStrings.V0CONO) + ' ' + querySelect : querySelect,
                Timeout = 9000000,
            });
            
        }
    }
    public class ExNihiloGenericExtraction : EtlProcess
    {
        private string querySelect;
        private string queryInsert;
        public ExNihiloGenericExtraction(string InputData, string OutputData)
        {
            this.querySelect = InputData;
            this.queryInsert = OutputData;
        }
        protected override void Initialize()
        {          
            Register(new ExtractData((GlobalStrings.ERP != "XA") ? (GlobalStrings.UseMultisite) ? string.Format(GlobalStrings.SetSite, GlobalStrings.V0CONO) + ' ' + querySelect : querySelect : querySelect));          
            Register(new LoadData(queryInsert));
        }

    }
    public class ExNihiloGenericExtractionERP : EtlProcess
    {
        private string querySelect;
        private string queryInsert;
        public ExNihiloGenericExtractionERP(string InputData, string OutputData)
        {
            this.querySelect = InputData;
            this.queryInsert = OutputData;
        }
        protected override void Initialize()
        {
           
            Register(new ExtractData(querySelect));
            Register(new LoadDataERP((GlobalStrings.ERP != "XA") ? (GlobalStrings.UseMultisite) ? string.Format(GlobalStrings.SetSite, GlobalStrings.V0CONO) + ' ' + querySelect : querySelect : querySelect));
        }

    }


    public class ExNihiloGenericExtractionERP2 : EtlProcess
    {
        private string querySelect;
        private string queryInsert;
        public ExNihiloGenericExtractionERP2(string InputData, string OutputData)
        {
            this.querySelect = InputData;
            this.queryInsert = OutputData;
        }
        protected override void Initialize()
        {

            Register(new ExtractData(querySelect));
            Register(new LoadDataERP((GlobalStrings.ERP != "XA") ? (GlobalStrings.UseMultisite) ? string.Format(GlobalStrings.SetSite, GlobalStrings.V0CONO) + ' ' + queryInsert : queryInsert : queryInsert));
        }

    }

    public class ExNihiloTestConnection : EtlProcess
    {
        private string querySelect;
        private string NameConnection;
        public ExNihiloTestConnection(string InputData, string  BDConnection)
        {
            this.querySelect = InputData;
            this.NameConnection = BDConnection;
        }
        protected override void Initialize()
        {
            Register(new ConventionInputCommandOperation(NameConnection=="ERP"? ConnectionData.ConexionSettingERP: ConnectionData.ConexionSettingCFDI)
            {
                Command = querySelect,
                Timeout = 9000000,
            });
        }
    }
    public class ExNihiloGetSchema : EtlProcess
    {
        private string querySelect;
        public GetSchema ObjgetSchema;
        public ExNihiloGetSchema(string InputData)
        {
            this.querySelect = InputData;
        }
        protected override void Initialize()
        {
            
            Register(new ExtractData(querySelect));
            this.ObjgetSchema = new GetSchema();
            Register(ObjgetSchema);
        }
    }
    //***Clase Principal***//
    public class ETL
    {
        public ETL()
        {
            // Configuration customConfig = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);
            // customConfig.ConnectionStrings.ConnectionStrings.Clear();
            //Usado para crear en config las cadenas de conexion, se comenta por que marca error de archivo inaccesible
            //crearconexiones(customConfig);
            // ConfigurationManager.RefreshSection("connectionStrings");

            crearconexionesnew();
        }
        void AddConexionesnew(string NameConnetion, string StringConnection, string ProviderConnection,Configuration config)
        {
            //Buscar cadena de conexion para desencriptar
            ////var Cadena = StringConnection.Split(';');
            ////var Pass = Cadena[3];

            //Agrega la cadena de conexion si no existe
            ConnectionStringSettings settings = new ConnectionStringSettings(NameConnetion, StringConnection, ProviderConnection);

           

           // config.ConnectionStrings.ConnectionStrings.Add(settings);
            if(NameConnetion=="ERP")
                ConnectionData.ConexionSettingERP = settings;
            else
               ConnectionData.ConexionSettingCFDI = settings;
        }
        //Metodos para crear la conexión
        //void AddConexiones(string NameConnection, string StringConnection, string ProviderConnection, Configuration config)
        //{
        //    //abrimos la configuración de nuestro proyecto
        //    config = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);
        //    config.ConnectionStrings.ConnectionStrings.Add(new ConnectionStringSettings
        //    {
        //        Name = NameConnection,
        //        ConnectionString = StringConnection,
        //        ProviderName = ProviderConnection
        //    });
        //    config.Save(ConfigurationSaveMode.Full);

        //}
        void UpdateConexionERP(Configuration config, string schema)
        {
            string schemas;
            schemas = "QGPL," + schema;        
            ConnectionData.ConexionSettingERP.ConnectionString = ConnectionData.ConexionSettingERP.ConnectionString.Replace("QGPL", schemas);
         
            //ConnectionStringSettingsCollection settings = ConfigurationManager.ConnectionStrings;
            //if (config != null)
            //{
            //    foreach (ConnectionStringSettings cs in settings)
            //    {
            //        if (cs.Name == "ERP")
            //        {
            //            ConnectionData.StringConnectionERP = ConnectionData.StringConnectionERP.Replace("QGPL",schemas);
            //            //config = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);
            //            //hacemos la modificacion de la cadena de conexion (ServerDb es el atributo que tengo en app.config) 
            //            config.ConnectionStrings.ConnectionStrings["ERP"].ConnectionString = ConnectionData.StringConnectionERP;
            //            config.ConnectionStrings.ConnectionStrings["ERP"].ProviderName = ConnectionData.ProviderConnectionERP;
            //            //Cambiamos el modo de guardado
            //            config.Save(ConfigurationSaveMode.Modified, true);
            //        }
            //    }
            //}
        }
        void crearconexionesnew()
        {
            //Configuration config = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);
            //ConnectionStringSettingsCollection settings = ConfigurationManager.ConnectionStrings;
            //bool ExistCountryPack = false;
            //bool ExistERP = false;
            //if (config != null)
            //{
            //    foreach (ConnectionStringSettings cs in settings)
            //    {
            //        if (cs.Name == "CountryPack")
            //        {
            //            ExistCountryPack = true;
            //            //hacemos la modificacion de la cadena de conexion (ServerDb es el atributo que tengo en app.config) 
            //            config.ConnectionStrings.ConnectionStrings["CountryPack"].ConnectionString = ConnectionData.StringConnection;
            //            config.ConnectionStrings.ConnectionStrings["CountryPack"].ProviderName = ConnectionData.ProviderConnection;
                       
            //        }
            //        if (cs.Name == "ERP")
            //        {
            //            ExistERP = true;
            //            //hacemos la modificacion de la cadena de conexion (ServerDb es el atributo que tengo en app.config) 
            //            config.ConnectionStrings.ConnectionStrings["ERP"].ConnectionString = ConnectionData.StringConnectionERP;
            //            config.ConnectionStrings.ConnectionStrings["ERP"].ProviderName = ConnectionData.ProviderConnectionERP;                     
                       
            //        }
            //    }
            //}
            //config.ConnectionStrings.ConnectionStrings.Clear();
           // if (!ExistCountryPack)
                AddConexionesnew("CountryPack", ConnectionData.StringConnection, ConnectionData.ProviderConnection, null);
           // if (!ExistERP)
                AddConexionesnew("ERP", ConnectionData.StringConnectionERP, ConnectionData.ProviderConnectionERP, null);

            //config.Save(ConfigurationSaveMode.Modified, true);
            //ConfigurationManager.RefreshSection("connectionStrings");
        }
        //void crearconexiones(Configuration config)
        //{
        //    bool ExistCountryPack = false;
        //    bool ExistERP = false;
        //    // string a = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None).FilePath;
        //    //MessageBox.Show(a);
        //    ConnectionStringSettingsCollection settings = ConfigurationManager.ConnectionStrings;
           
        //    if (config != null)
        //    {
        //       foreach (ConnectionStringSettings cs in settings)
        //        {
        //            if (cs.Name == "CountryPack")
        //            {
        //                ExistCountryPack = true;
        //                config = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);
        //                //hacemos la modificacion de la cadena de conexion (ServerDb es el atributo que tengo en app.config) 
        //                config.ConnectionStrings.ConnectionStrings["CountryPack"].ConnectionString = ConnectionData.StringConnection;
        //                config.ConnectionStrings.ConnectionStrings["CountryPack"].ProviderName = ConnectionData.ProviderConnection;
        //                //Cambiamos el modo de guardado
        //                config.Save(ConfigurationSaveMode.Modified, true);
        //            }
        //            if (cs.Name == "ERP")
        //            {
        //                ExistERP = true;
        //                config = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);
        //                //hacemos la modificacion de la cadena de conexion (ServerDb es el atributo que tengo en app.config) 
        //                config.ConnectionStrings.ConnectionStrings["ERP"].ConnectionString = ConnectionData.StringConnectionERP;
        //                config.ConnectionStrings.ConnectionStrings["ERP"].ProviderName = ConnectionData.ProviderConnectionERP;
        //                //Cambiamos el modo de guardado
        //                config.Save(ConfigurationSaveMode.Modified, true);
        //            }
        //        }
        //    }
        //    //config.ConnectionStrings.ConnectionStrings.Clear();
        //    if (!ExistCountryPack)
        //        AddConexiones("CountryPack", ConnectionData.StringConnection, ConnectionData.ProviderConnection, config);
        //    if (!ExistERP)
        //        AddConexiones("ERP", ConnectionData.StringConnectionERP, ConnectionData.ProviderConnectionERP, config);
        //}
        void GetDataCompany()
        {

            ExNihiloGetCompanyData CompanyData = new ExNihiloGetCompanyData();
            CompanyData.UseTransaction = false;
            CompanyData.Execute();
            if (CompanyData.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in CompanyData.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                }
            }
        }

        //Agregado por JL para
       public void UpdateDocRelated(string rows, string serie,string folio,string siteerp)
        {
          
            
            if (!string.IsNullOrEmpty(rows))
            {
                //Cargar xml 
                XmlSerializer serializer = new XmlSerializer(typeof(List<PX>), new XmlRootAttribute("ListDocRel"));
                StringReader stringReader = new StringReader(rows);
                List<PX> docrelated = (List<PX>)serializer.Deserialize(stringReader);
                //Buscar el site
               
                foreach (var row in docrelated)
                {
                    //Console.WriteLine(row.UUID);
                    //Armar cadena para realizar la actualizacion de la informacion
                    var stringUpdatePX = string.Format(GlobalStrings.UPDATEMXEIPX, row.SerieDocRel, row.FolioDocRel, row.Moneda, row.TipoCambio, row.MetodoPago, row.NumParcialidad, row.SaldoAnterior, row.MontoPagado, row.NuevoSaldo, siteerp, row.UUID,row.SeriePago,row.FolioPago);
                    UPDATEMXEIPXY(stringUpdatePX);
                }
            }
            //Actualizar MXEIPY
            //var stringUpdatePY = string.Format(GlobalStrings.UPDATEMXEIPY, serie, folio, siteerp);
            //UPDATEMXEIPXY(stringUpdatePY);
            //
        }
        //
        public void ProcessTaxses(string registro)
        {


            if (!string.IsNullOrEmpty(registro))
            {
                //Cargar xml 
                XmlSerializer serializer = new XmlSerializer(typeof(List<Taxs>), new XmlRootAttribute("ListDocTax"));
                StringReader stringReader = new StringReader(registro);
                List<Taxs> docrelated = (List<Taxs>)serializer.Deserialize(stringReader);
                //Buscar el site              
                foreach (var row in docrelated)
                {
                    //Console.WriteLine(row.UUID);
                    //Armar cadena para realizar la actualizacion de la informacion
                    var stringInsertTaxMultiSL = string.Format(GlobalStrings.InsertTaxesMultiSL, row.SiteRef, row.Base, row.Tax, row.TypeFactor, row.TasaOquote, row.Total, row.TaxConceptMultisiteID, row.UUID_DocRelated, row.Type_Tax, (string.IsNullOrEmpty(row.UFT1) ? "NULL" : row.UFT1), (string.IsNullOrEmpty(row.UFD1) ? "NULL" : row.UFD1) , (string.IsNullOrEmpty(row.UFAMT1) ? "NULL" : row.UFAMT1), row.Serie, row.Folio, row.TotalInvoice, row.CurrencyInvoice);                  
                    InsertTaxMultiSL(stringInsertTaxMultiSL);
                }
            }
           
        }

        public void ProcessDocumentoCFDIDocRelated(string registro)
        {


            if (!string.IsNullOrEmpty(registro))
            {
                //Cargar xml 
                XmlSerializer serializer = new XmlSerializer(typeof(List<CFDIDocRelated>), new XmlRootAttribute("ListDocRelated"));
                StringReader stringReader = new StringReader(registro);
                List<CFDIDocRelated> docrelated = (List<CFDIDocRelated>)serializer.Deserialize(stringReader);
                //Buscar el site              
                foreach (var row in docrelated)
                {
                    //Console.WriteLine(row.UUID);
                    //Armar cadena para realizar la actualizacion de la informacion
                    var stringInsertTaxMultiSL = string.Format(GlobalStrings.InsertDocumentDocRelated, row.VJCONO, row.VJFOLIO, row.VJSERIE, row.VJSEQN, row.VJISERIE, row.VJIFOLIO, row.VJRELTYPE);
                    InsertDocumentCFDIDocRelated(stringInsertTaxMultiSL);
                }
            }

        }
        void GetSite()
        {

            ExNihiloGetSitio CompanyData = new ExNihiloGetSitio(GlobalStrings.RecuperarSitio);            
            CompanyData.UseTransaction = false;
            CompanyData.Execute();
            if (CompanyData.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in CompanyData.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                }
            }
        }
        void NodoComprobante(string OutputData)
        {
            Console.WriteLine("Inicia extraccion nodo comprobante");
            string InputData = string.Empty;
            if (GlobalStrings.ERP != "XA")
            {
                if (GlobalStrings.UseMultisite)
                    InputData = string.Format(GlobalStrings.SelectComprobanteSL, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                else

                    InputData = string.Format(GlobalStrings.SelectComprobanteSLNoMulti, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
            }
            else
            {
                if(!GlobalStrings.GetSubTotalXA)
                InputData = string.Format(GlobalStringsXA.SelectComprobante0, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO, GlobalStrings.metodoPago, GlobalStrings.formaPago);
                else
                InputData = string.Format(GlobalStringsXA.SelectComprobante, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO, GlobalStrings.metodoPago, GlobalStrings.formaPago);
            }

         //   string OutputData = string.Format(GlobalStrings.InsertVoucher, GlobalStrings.version, GlobalStrings.formaPago, GlobalStrings.lugarExpedicion, GlobalStrings.metodoPago, GlobalStrings.SiteRef);

            ExNihiloNodoComprobante NodoComprobante = new ExNihiloNodoComprobante(InputData, OutputData);
            NodoComprobante.UseTransaction = false;
            NodoComprobante.Execute();
            if (NodoComprobante.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in NodoComprobante.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                }
            }
            Console.WriteLine("Finaliza extraccion nodo comprobante");
        }
        void NodoEmisor(string OutputData)
        {
            Console.WriteLine("Inicia extraccion nodo emisor");
            string InputData1 = string.Format(GlobalStrings.SelectEmisorErp, GlobalStrings.rfcEmisor, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
            string InputData2 = string.Format(GlobalStrings.SelectEmisorCountryPack, GlobalStrings.rfcEmisor);
            //string OutputData = string.Format(GlobalStrings.InsertEmisor, GlobalStrings.comprobanteId, GlobalStrings.SiteRef);

            ExNihiloNodoEmisor NodoEmisor = new ExNihiloNodoEmisor(InputData1, InputData2, OutputData);
            NodoEmisor.UseTransaction = false;
            NodoEmisor.Execute();
            if (NodoEmisor.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in NodoEmisor.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                }
            }
            Console.WriteLine("Finaliza extraccion nodo emisor");
        }
        void NodoReceptor(string OutputData)
        {

            if (GlobalStrings.ERP != "XA")
            {
                Console.WriteLine("ValidarExistenciaColumna USECFDI");

                GlobalStrings.ValidateCol = "0";
                string InputDataCol = string.Format(GlobalStrings.ValidarExistenciaColumna, "V0USGE", "MXEIHD");
                ExNihiloValidarExistenciaColumnaSL ExitsColumn = new ExNihiloValidarExistenciaColumnaSL(InputDataCol);
                ExitsColumn.UseTransaction = false;
                ExitsColumn.Execute();
                if (ExitsColumn.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in ExitsColumn.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                        VoucherLog("CountryPack", 5, ex.Message, 0, 10);
                        //Rollback();
                    }
                }
                if (GlobalStrings.ValidateCol == "1")
                {
                    GlobalStrings.ValidateCol = "0";
                    //Extraer valor de la columna
                    var InputDataHD = string.Format(GlobalStrings.SelectComprobanteV0USGE, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                    ExNihiloMXEIHDUSECFDI V0USGEReceptor = new ExNihiloMXEIHDUSECFDI(InputDataHD);
                    V0USGEReceptor.UseTransaction = false;
                    V0USGEReceptor.Execute();
                    if (V0USGEReceptor.GetAllErrors().Count() > 0)
                    {
                        foreach (Exception ex in V0USGEReceptor.GetAllErrors())
                        {
                            Console.WriteLine(ex.Message);
                        }
                    }
                    Console.WriteLine("Finaliza extraccion del uso del cfdi");

                }
            }
           
            Console.WriteLine("Inicia extraccion nodo receptor");
            string InputData1 = string.Format(GlobalStrings.SelectReceptorAddressErp, GlobalStrings.rfcReceptor, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
            string InputData2 = string.Format(GlobalStrings.SelectReceptorShiptopErp, GlobalStrings.rfcReceptor, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
            //string InputData3 = string.Format(GlobalStrings.SelectReceptorCountryPack, GlobalStrings.rfcReceptor);
            //PRUEBA FELIPE
            string InputData3 = string.Empty;
            var useCFDI = ((GlobalStrings.TypeCFDI == "I") ? "ISNULL(cfdiUse,'')" : "ISNULL(cfdiUseEgreso,'')").ToString();
            if (GlobalStrings.ERP != "XA")
            {             
              
                InputData3 = string.IsNullOrEmpty(GlobalStrings.UseCFDI) ? string.Format(GlobalStrings.SelectReceptorCountryPack, GlobalStrings.rfcReceptor, GlobalStrings.cliente, useCFDI) : string.Format(GlobalStrings.SelectReceptorCountryPackUSE, GlobalStrings.rfcReceptor, GlobalStrings.cliente, GlobalStrings.UseCFDI);
                
            }
            else
                InputData3 = string.Format(GlobalStrings.SelectReceptorCountryPack, GlobalStrings.rfcReceptor, GlobalStrings.cliente, useCFDI);
           
            //string OutputData = string.Format(GlobalStrings.InsertReceptor, GlobalStrings.comprobanteId, GlobalStrings.SiteRef);
            ExNihiloNodoReceptor NodoReceptor = new ExNihiloNodoReceptor(InputData1, InputData2, InputData3, OutputData);
            NodoReceptor.UseTransaction = false;
            NodoReceptor.Execute();
            if (NodoReceptor.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in NodoReceptor.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                }
            }
            Console.WriteLine("Finaliza extraccion nodo receptor");
        }
        public static List<Conceptos> NodoConceptos(string InputData,string OutputData)
        {
            //string InputData = string.Format(GlobalStrings.SelectNodoConcepto, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
            //string OutputData = string.Format(GlobalStrings.InsertNodoConcepto, GlobalStrings.comprobanteId, GlobalStrings.SiteRef);
            Console.WriteLine("Inicia extraccion nodo conceptos");
            List<Conceptos> Conceptos = new List<Conceptos>();
            ExNihiloNodoConcepto NodoConcepto = new ExNihiloNodoConcepto(InputData, OutputData, Conceptos);
            NodoConcepto.UseTransaction = false;
            NodoConcepto.Execute();
            if (NodoConcepto.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in NodoConcepto.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                }
            }
            Conceptos = NodoConcepto.Getlista();
            Console.WriteLine("Finaliza extraccion nodo conceptos");
            return Conceptos;
        }
        //Agregado por jl para el manejo de la configuracion calculo de importe
        public void NodoConfiguraciones(string InputData)
        {

            List<Configuraciones> configuraciones = new List<Configuraciones>();
            ExNihiloNodoConfiguracion Nodoconfiguraciones = new ExNihiloNodoConfiguracion(InputData);
            Nodoconfiguraciones.UseTransaction = false;
            Nodoconfiguraciones.Execute();
            if (Nodoconfiguraciones.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in Nodoconfiguraciones.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                }
            }
        }
        //Agregado por JL para la recuperacion de los decimales
        public void RecuperarVoucherDecimals()
        {

            VoucherDecimals voudec = new VoucherDecimals();
            ExNihiloRecuperarVoucherDecimal Nodoconfiguraciones = new ExNihiloRecuperarVoucherDecimal(GlobalStrings.SelectVoucherDecimals);
            Nodoconfiguraciones.UseTransaction = false;
            Nodoconfiguraciones.Execute();
            if (Nodoconfiguraciones.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in Nodoconfiguraciones.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                }
            }
        }
        void PedimentosConceptos(List<Conceptos> listConceptos)
        {
            Console.WriteLine("Inicio de extraccion de pedimentos");
            string sExistsPedimentos = "0";
            string InputData1 = string.Format(GlobalStrings.SelectExistsPedimentos, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);

            ExNihiloExists ExistsPedimentos = new ExNihiloExists(InputData1);
            ExistsPedimentos.UseTransaction = false;
            ExistsPedimentos.Execute();
            if (ExistsPedimentos.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in ExistsPedimentos.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                    sExistsPedimentos = "0";
                }
            }   
            if (ExistsPedimentos.ObjgetExists.getIfExistsRegistros() != null)
                sExistsPedimentos = ExistsPedimentos.ObjgetExists.getIfExistsRegistros();
            
            if (sExistsPedimentos == "1")
            {
                foreach (Conceptos Concepto in listConceptos)
                {
                    string InputData2 = GlobalStrings.InicializaRegistroERP;
                    string OutputData = string.Format(GlobalStrings.DeletePedimentos, Concepto.conceptoId);
                    //Eliminamos Pedimentos
                    ExNihiloGenericExtraction DeletePedimentos = new ExNihiloGenericExtraction(InputData2, OutputData);
                    DeletePedimentos.UseTransaction = false;
                    DeletePedimentos.Execute();
                    if (DeletePedimentos.GetAllErrors().Count() > 0)
                    {
                        foreach (Exception ex in DeletePedimentos.GetAllErrors())
                        {
                            Console.WriteLine(ex.Message);
                        }
                    }
                    //Insertamos Pedimentos
                    InputData2 = string.Format(GlobalStrings.SelectPedimentos, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO, Concepto.linea );
                    OutputData = string.Format(GlobalStrings.InsertPedimentos, Concepto.conceptoId,GlobalStrings.Parm_Site);                   
                    ExNihiloPedimentosConceptos InsertPedimentos = new ExNihiloPedimentosConceptos(InputData2,OutputData);
                    InsertPedimentos.UseTransaction = false;
                    InsertPedimentos.Execute();
                    if (InsertPedimentos.GetAllErrors().Count() > 0)
                    {
                        foreach (Exception ex in InsertPedimentos.GetAllErrors())
                        {
                            Console.WriteLine(ex.Message);
                        }
                    }
                }
            }
            Console.WriteLine("Fin de extraccion de pedimentos");
        }
        //Agregado por JL para elimnar conceptos
        void LimpiaNodoConcepto(string OutputData)
        {
            Console.WriteLine("Inicia proceso de eliminacion de conceptos");
            string InputData = GlobalStrings.InicializaRegistroERP;
            ExNihiloGenericExtraction DeleteConcepto = new ExNihiloGenericExtraction(InputData, OutputData);
            DeleteConcepto.UseTransaction = false;
            DeleteConcepto.Execute();
            if (DeleteConcepto.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in DeleteConcepto.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                }
            }
            Console.WriteLine("Finaliza proceso de eliminacion de conceptos");
        }
        //
        void LimpiaNodoImpuesto(string OutputData)
        {
            Console.WriteLine("Inicia limpiar nodo impuesto");
            string InputData = GlobalStrings.InicializaRegistroERP;
            ExNihiloGenericExtraction DeleteImpuestos = new ExNihiloGenericExtraction(InputData, OutputData);
            DeleteImpuestos.UseTransaction = false;
            DeleteImpuestos.Execute();
            if (DeleteImpuestos.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in DeleteImpuestos.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                }
            }
            Console.WriteLine("Finaliza limpieza nodo impuesto");
        }

        void LimpiaNodoKits(string OutputData)
        {
            Console.WriteLine("Inicia limpiar nodo Kits");
            string InputData = GlobalStrings.InicializaRegistroERP;
            ExNihiloGenericExtraction DeleteKits = new ExNihiloGenericExtraction(InputData, OutputData);
            DeleteKits.UseTransaction = false;
            DeleteKits.Execute();
            if (DeleteKits.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in DeleteKits.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                }
            }
            Console.WriteLine("Finaliza limpieza nodo Kits");
        }
        //Agregado por JL -2812019 para eliminar px y py
        void LimpiaPXPY(string OutputData)
        {           
            string InputData = GlobalStrings.InicializaRegistroERP;
            ExNihiloGenericExtraction DeleteImpuestos = new ExNihiloGenericExtraction(InputData, OutputData);
            DeleteImpuestos.UseTransaction = false;
            DeleteImpuestos.Execute();
            if (DeleteImpuestos.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in DeleteImpuestos.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                }
            }
        }
        void LimpiaImpuestosMulti(string OutputData)
        {
            string InputData = GlobalStrings.InicializaRegistroERP;            
            ExNihiloGenericExtraction DeleteImpuestos = new ExNihiloGenericExtraction(InputData, OutputData);
            DeleteImpuestos.UseTransaction = false;
            DeleteImpuestos.Execute();
            if (DeleteImpuestos.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in DeleteImpuestos.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                }
            }
        }
        //Agregado por JL para actualizar los registros de MXEIPY
        void UPDATEMXEIPXY(string OutputData)
        {
           
            string InputData = GlobalStrings.InicializaRegistroERP;
            ExNihiloGenericExtractionERP UpdateMXEIPY = new ExNihiloGenericExtractionERP(InputData, OutputData);
            UpdateMXEIPY.UseTransaction = false;
            UpdateMXEIPY.Execute();
            if (UpdateMXEIPY.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in UpdateMXEIPY.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                }
            }
        }
        //Agregado por JL para insertar impuestos
        void InsertTaxMultiSL(string OutputData)
        {

            string InputData = GlobalStrings.InicializaRegistroERP;
            ExNihiloGenericExtractionERP2 InsertMultiTaxSL = new ExNihiloGenericExtractionERP2(InputData, OutputData);
            InsertMultiTaxSL.UseTransaction = false;
            InsertMultiTaxSL.Execute();
            if (InsertMultiTaxSL.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in InsertMultiTaxSL.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                }
            }
        }

        void InsertDocumentCFDIDocRelated(string OutputData)
        {

            string InputData = GlobalStrings.InicializaRegistroERP;
            ExNihiloGenericExtractionERP2 InsertDocCfdiRelated = new ExNihiloGenericExtractionERP2(InputData, OutputData);
            InsertDocCfdiRelated.UseTransaction = false;
            InsertDocCfdiRelated.Execute();
            if (InsertDocCfdiRelated.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in InsertDocCfdiRelated.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                }
            }
        }
        void NodoImpuestos(List<Conceptos> Impuestos)
        {

            Console.WriteLine("Inicia extraccion nodo concepto");
            string InputData = string.Format(GlobalStrings.SelectImpuestosERP, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
            string OutputData = string.Format(GlobalStrings.InsertNodoImpuestos, GlobalStrings.SiteRef);


            ExNihiloNodoImpuestos NodoImpuestos = new ExNihiloNodoImpuestos(InputData, OutputData, Impuestos);
            NodoImpuestos.UseTransaction = false;
            NodoImpuestos.Execute();
            if (NodoImpuestos.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in NodoImpuestos.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                }
            }
            Console.WriteLine("Finaliza extraccion nodo concepto");
        }
        void CfdiRelacionados(string OutputData)
        {
            Console.WriteLine("Inicia extraccion documentos relacionados");
            string InputData = string.Empty;
            if (GlobalStrings.ERP != "XA")           
                InputData = string.Format(GlobalStrings.SelectCfdiRelacionados, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);            
            else
                InputData = string.Format(GlobalStringsXA.SelectCfdiRelacionados, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
            //string OutputData = string.Format(GlobalStrings.InsertCfdiRelacionados, GlobalStrings.comprobanteId);
           
            ExNihiloGenericExtraction Comentarios = new ExNihiloGenericExtraction(InputData, OutputData);
            Comentarios.UseTransaction = false;
            Comentarios.Execute();
            if (Comentarios.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in Comentarios.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                }
            }
            Console.WriteLine("Finaliza extraccion documentos relacionados");
        }
        void POS(string OutputData) //**Punto de Venta en SL**\\||//**Para Recuperar Shipto en XA**\\  
        {
            Console.WriteLine("Inicia extraccion punto de venta");
            string sVerificaPOS = "0";
            string InputData1 = string.Format(GlobalStrings.SelectVerificaPOS, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
            //Console.WriteLine(InputData1);
            ExNihiloExists VerificaPOS = new ExNihiloExists(InputData1);
            VerificaPOS.UseTransaction = false;
            VerificaPOS.Execute();
            if (VerificaPOS.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in VerificaPOS.GetAllErrors())
                {
                    Console.WriteLine("ERROR");
                    Console.WriteLine(ex.Message);
                    sVerificaPOS = "0"; 
                }
            }   /// if (getMachConcepto.ObjgetMachConcepto.getunitId() == null)
            if (VerificaPOS.ObjgetExists.getIfExistsRegistros() != null)
                sVerificaPOS = "1";//VerificaPOS.ObjgetExists.getIfExistsRegistros();
            if (sVerificaPOS == "1")
            {

                string InputData2 = string.Empty;
                if (GlobalStrings.ERP != "XA")                
                    InputData2 = string.Format(GlobalStrings.SelectPOS, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                else
                    InputData2 = string.Format(GlobalStrings.SelectPOSXA, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                // string OutputData = string.Format(GlobalStrings.InsertAddenda, GlobalStrings.comprobanteId, GlobalStrings.SiteRef);
              
                ExNihiloGenericExtraction WritePos = new ExNihiloGenericExtraction(InputData2, OutputData);
                WritePos.UseTransaction = false;
                WritePos.Execute();
                if (WritePos.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in WritePos.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
            }
            Console.WriteLine("Finaliza extraccion punto venta");

        }
        void NodoAddendas(string OutputData)
        {
            Console.WriteLine("Inicia proceso extraccion addenda");
            string sExistAddenda = "0";
            string InputData1 = string.Format(GlobalStrings.SelectExistsAddenda, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);

            ExNihiloExists ExistAddenda = new ExNihiloExists(InputData1);
            ExistAddenda.UseTransaction = false;
            ExistAddenda.Execute();
            if (ExistAddenda.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in ExistAddenda.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                    sExistAddenda = "0";
                }
            }   /// if (getMachConcepto.ObjgetMachConcepto.getunitId() == null)
            if (ExistAddenda.ObjgetExists.getIfExistsRegistros() != null)
                sExistAddenda = ExistAddenda.ObjgetExists.getIfExistsRegistros();
            if (sExistAddenda == "1")
            {
                string InputData2 = string.Format(GlobalStrings.SelectAddenda, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
              
                ExNihiloNodoAddenda NodoAddenda = new ExNihiloNodoAddenda(InputData2, OutputData);
                NodoAddenda.UseTransaction = false;
                NodoAddenda.Execute();
                if (NodoAddenda.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in NodoAddenda.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                    }
                }

                Console.WriteLine("Fin proceso extraccion addenda");
            }
        }
        void Comentarios()
        {
            Console.WriteLine("Inicia proceso de extracción comentarios");
            string InputData = GlobalStrings.InicializaRegistroERP;
            string OutputData = string.Format(GlobalStrings.DeleteComments, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
            //---------------------------
            ExNihiloGenericExtraction DeleteComentarios = new ExNihiloGenericExtraction(InputData, OutputData);
            DeleteComentarios.UseTransaction = false;
            DeleteComentarios.Execute();
            if (DeleteComentarios.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in DeleteComentarios.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                }
            }
            //---------------------------
            InputData = string.Format(GlobalStrings.SelectComments, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
            OutputData = string.Format(GlobalStrings.InsertComments, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO,GlobalStrings.Parm_Site);
            ExNihiloGenericExtraction Comentarios = new ExNihiloGenericExtraction(InputData, OutputData);
            Comentarios.UseTransaction = false;
            Comentarios.Execute();
            if (Comentarios.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in Comentarios.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                }
            }
            Console.WriteLine("Finaliza proceso de extracción comentarios");
        }

        void UpdateVoucherUFS()
        {
            Console.WriteLine("Inicia proceso de actualizacion ufs");
            string InputData = string.Format(GlobalStrings.SelectVoucherUFS, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
            string OutputData = string.Format(GlobalStrings.UpdateVoucherUFS, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
            //---------------------------
            ExNihiloGenericExtraction UpdateVoucher = new ExNihiloGenericExtraction(InputData, OutputData);
            UpdateVoucher.UseTransaction = false;
            UpdateVoucher.Execute();
            if (UpdateVoucher.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in UpdateVoucher.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                }
            }
            //---------------------------
           
            Console.WriteLine("Finaliza proceso de actualizacion ufs");
        }
        //Complementos
        void Detallista()
        {
            Console.WriteLine("Inicia extraccion complemento detallista");
            string sExistDetallista = "0";
            string InputData = String.Empty;
            string OutputData = String.Empty;
            InputData = string.Format(GlobalStrings.SelectExistsDetallista, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);

            ExNihiloExists ExistAddenda = new ExNihiloExists(InputData);
            ExistAddenda.UseTransaction = false;
            ExistAddenda.Execute();
            if (ExistAddenda.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in ExistAddenda.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                    sExistDetallista = "0";
                }
            }
            if (ExistAddenda.ObjgetExists.getIfExistsRegistros() != null)
                sExistDetallista = ExistAddenda.ObjgetExists.getIfExistsRegistros();
            if (sExistDetallista == "1")
            {
                InputData = string.Format(GlobalStrings.SelectMXADHD, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                OutputData = string.Format(GlobalStrings.InsertMXADHD, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                ExNihiloGenericExtraction TablaMXADHD = new ExNihiloGenericExtraction(InputData, OutputData);
                TablaMXADHD.UseTransaction = false;
                TablaMXADHD.Execute();
                if (TablaMXADHD.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in TablaMXADHD.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
                InputData = string.Format(GlobalStrings.SelectMXADSI, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                OutputData = string.Format(GlobalStrings.InsertMXADSI, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                ExNihiloGenericExtraction TablaMXADSI = new ExNihiloGenericExtraction(InputData, OutputData);
                TablaMXADSI.UseTransaction = false;
                TablaMXADSI.Execute();
                if (TablaMXADSI.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in TablaMXADSI.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
                InputData = string.Format(GlobalStrings.SelectMXADCU, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                OutputData = string.Format(GlobalStrings.InsertMXADCU, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                ExNihiloGenericExtraction TablaMXADCU = new ExNihiloGenericExtraction(InputData, OutputData);
                TablaMXADCU.UseTransaction = false;
                TablaMXADCU.Execute();
                if (TablaMXADCU.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in TablaMXADCU.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
                InputData = string.Format(GlobalStrings.SelectMXADPD, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                OutputData = string.Format(GlobalStrings.InsertMXADPD, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                ExNihiloGenericExtraction TablaMXADPD = new ExNihiloGenericExtraction(InputData, OutputData);
                TablaMXADPD.UseTransaction = false;
                TablaMXADPD.Execute();
                if (TablaMXADPD.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in TablaMXADPD.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
            
                InputData = string.Format(GlobalStrings.SelectMXADCG, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                OutputData = string.Format(GlobalStrings.InsertMXADCG, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                ExNihiloGenericExtraction TablaMXADCG = new ExNihiloGenericExtraction(InputData, OutputData);
                TablaMXADCG.UseTransaction = false;
                TablaMXADCG.Execute();
                if (TablaMXADCG.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in TablaMXADCG.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
               
                InputData = string.Format(GlobalStrings.SelectMXADDT, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                OutputData = string.Format(GlobalStrings.InsertMXADDT, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                ExNihiloGenericExtraction TablaMXADDT = new ExNihiloGenericExtraction(InputData, OutputData);
                TablaMXADDT.UseTransaction = false;
                TablaMXADDT.Execute();
                if (TablaMXADDT.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in TablaMXADDT.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
               
                InputData = string.Format(GlobalStrings.SelectMXADRF, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                OutputData = string.Format(GlobalStrings.InsertMXADRF, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                ExNihiloGenericExtraction TablaMXADRF = new ExNihiloGenericExtraction(InputData, OutputData);
                TablaMXADRF.UseTransaction = false;
                TablaMXADRF.Execute();
                if (TablaMXADRF.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in TablaMXADRF.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
               
                InputData = string.Format(GlobalStrings.SelectMXADAD, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                OutputData = string.Format(GlobalStrings.InsertMXADAD, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                ExNihiloGenericExtraction TablaMXADAD = new ExNihiloGenericExtraction(InputData, OutputData);
                TablaMXADAD.UseTransaction = false;
                TablaMXADAD.Execute();
                if (TablaMXADAD.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in TablaMXADAD.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
               
                InputData = string.Format(GlobalStrings.SelectMXADII, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                OutputData = string.Format(GlobalStrings.InsertMXADII, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                ExNihiloGenericExtraction TablaMXADII = new ExNihiloGenericExtraction(InputData, OutputData);
                TablaMXADII.UseTransaction = false;
                TablaMXADII.Execute();
                if (TablaMXADII.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in TablaMXADII.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
                
                InputData = string.Format(GlobalStrings.SelectMXADIQ, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                OutputData = string.Format(GlobalStrings.InsertMXADIQ, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                ExNihiloGenericExtraction TablaMXADIQ = new ExNihiloGenericExtraction(InputData, OutputData);
                TablaMXADIQ.UseTransaction = false;
                TablaMXADIQ.Execute();
                if (TablaMXADIQ.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in TablaMXADIQ.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
               
                InputData = string.Format(GlobalStrings.SelectMXADEA, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                OutputData = string.Format(GlobalStrings.InsertMXADEA, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                ExNihiloGenericExtraction TablaMXADEA = new ExNihiloGenericExtraction(InputData, OutputData);
                TablaMXADEA.UseTransaction = false;
                TablaMXADEA.Execute();
                if (TablaMXADEA.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in TablaMXADEA.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
               
                InputData = string.Format(GlobalStrings.SelectMXADTX, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                OutputData = string.Format(GlobalStrings.InsertMXADTX, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                ExNihiloGenericExtraction TablaMXADTX = new ExNihiloGenericExtraction(InputData, OutputData);
                TablaMXADTX.UseTransaction = false;
                TablaMXADTX.Execute();
                if (TablaMXADTX.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in TablaMXADTX.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
                
                InputData = string.Format(GlobalStrings.SelectMXADXC, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                OutputData = string.Format(GlobalStrings.InsertMXADXC, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                ExNihiloGenericExtraction TablaMXADXC = new ExNihiloGenericExtraction(InputData, OutputData);
                TablaMXADXC.UseTransaction = false;
                TablaMXADXC.Execute();
                if (TablaMXADXC.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in TablaMXADXC.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
               
            }
            Console.WriteLine("Finaliza extraccion complemento detallista");
        }
        //Agregado por JL-23102019 para la validacion de la existencia de la tablas en el ERP
        string ValidateExistTableERP(string InputData)
        {
            var exitsTable = "0";
            ExNihiloValidarExistenciaTablaSL ValidateExitTabla= new ExNihiloValidarExistenciaTablaSL(InputData);
            ValidateExitTabla.UseTransaction = false;
            ValidateExitTabla.Execute();
            if (ValidateExitTabla.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in ValidateExitTabla.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                }
            }
            if (ValidateExitTabla.ObjgetExists.getIfExistsTable() != null)
                exitsTable = ValidateExitTabla.ObjgetExists.getIfExistsTable();
            return exitsTable;


        }

        //Agregado por JL-23102019 para la validacion de la existencia de la tablas en CS
        string ValidateExistTableCS(string InputData)
        {
            var exitsTable = "0";
            ExNihiloValidarExistenciaTablaCS ValidateExitTabla = new ExNihiloValidarExistenciaTablaCS(InputData);
            ValidateExitTabla.UseTransaction = false;
            ValidateExitTabla.Execute();
            if (ValidateExitTabla.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in ValidateExitTabla.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                }
            }
            if (ValidateExitTabla.ObjgetExists.getIfExistsTable() != null)
                exitsTable = ValidateExitTabla.ObjgetExists.getIfExistsTable();
            return exitsTable;


        }
        //Agregado por JL - 23102019 para realizar el proceso de insercion del complemento de vehiculos
        void ComplementoVehiculoHD(string OutputData, string OutputData2)
        {
            string sExistReg = "0";
            string InputData = String.Empty;          
            InputData = string.Format(GlobalStrings.SelectExistsReg,"MXEICV","SERIE",GlobalStrings.V0SERIE,"FOLIO", GlobalStrings.V0FOLIO, "CONO", GlobalStrings.V0CONO);            
         
            ExNihiloExists ExistRegCV = new ExNihiloExists(InputData);
            ExistRegCV.UseTransaction = false;
            ExistRegCV.Execute();
            if (ExistRegCV.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in ExistRegCV.GetAllErrors())
                {
                    Console.WriteLine("erro registros");
                    Console.WriteLine(ex.Message);
                    sExistReg = "0";
                }
            }
            if (ExistRegCV.ObjgetExists.getIfExistsRegistros() != null)
                sExistReg = ExistRegCV.ObjgetExists.getIfExistsRegistros();
       
            if (sExistReg == "1")
            {
                InputData = GlobalStrings.InicializaRegistroERP;

                ExNihiloGenericExtraction DeleteRegCV = new ExNihiloGenericExtraction(InputData, OutputData);
                DeleteRegCV.UseTransaction = false;
                DeleteRegCV.Execute();
                if (DeleteRegCV.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in DeleteRegCV.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
             
                InputData = string.Format(GlobalStrings.SelectComplementVehiHD, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO, GlobalStrings.V0CONO);
                ExNihiloGenericExtraction InsertCV = new ExNihiloGenericExtraction(InputData, OutputData2);
                InsertCV.UseTransaction = false;
                InsertCV.Execute();
                if (InsertCV.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in InsertCV.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
            }
        }
        void ComplementoVehiculoComplemento(string OutputData, string OutputData2)
        {
            string sExistReg = "0";
            string InputData = String.Empty;
            InputData = string.Format(GlobalStrings.SelectExistsReg, "MXEICVCP", "SERIE", GlobalStrings.V0SERIE, "FOLIO", GlobalStrings.V0FOLIO,"CONO",GlobalStrings.V0CONO);
            ExNihiloExists ExistRegCVCP = new ExNihiloExists(InputData);
            ExistRegCVCP.UseTransaction = false;
            ExistRegCVCP.Execute();
            if (ExistRegCVCP.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in ExistRegCVCP.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                    sExistReg = "0";
                }
            }
            if (ExistRegCVCP.ObjgetExists.getIfExistsRegistros() != null)
                sExistReg = ExistRegCVCP.ObjgetExists.getIfExistsRegistros();

            if (sExistReg == "1")
            {
                InputData = GlobalStrings.InicializaRegistroERP;
                ExNihiloGenericExtraction DeleteRegCVCP = new ExNihiloGenericExtraction(InputData, OutputData);
                DeleteRegCVCP.UseTransaction = false;
                DeleteRegCVCP.Execute();
                if (DeleteRegCVCP.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in DeleteRegCVCP.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
                InputData = string.Format(GlobalStrings.SelectComplementVehComp, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO,GlobalStrings.V0CONO);
                ExNihiloGenericExtraction InsertCVCP = new ExNihiloGenericExtraction(InputData, OutputData2);
                InsertCVCP.UseTransaction = false;
                InsertCVCP.Execute();
                if (InsertCVCP.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in InsertCVCP.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
            }
        }
        void ComplementoVehiculoDT(string OutputData, string OutputData2)
        {
            string sExistReg = "0";
            string InputData = String.Empty;
            InputData = string.Format(GlobalStrings.SelectExistsReg, "MXEICVD", "SERIE", GlobalStrings.V0SERIE, "FOLIO", GlobalStrings.V0FOLIO, "CONO", GlobalStrings.V0CONO);
            ExNihiloExists ExistRegCVDT = new ExNihiloExists(InputData);
            ExistRegCVDT.UseTransaction = false;
            ExistRegCVDT.Execute();
            if (ExistRegCVDT.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in ExistRegCVDT.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                    sExistReg = "0";
                }
            }
            if (ExistRegCVDT.ObjgetExists.getIfExistsRegistros() != null)
                sExistReg = ExistRegCVDT.ObjgetExists.getIfExistsRegistros();
         
            if (sExistReg == "1")
            {
                InputData = GlobalStrings.InicializaRegistroERP;
                ExNihiloGenericExtraction DeleteRegCVDT = new ExNihiloGenericExtraction(InputData, OutputData);
                DeleteRegCVDT.UseTransaction = false;
                DeleteRegCVDT.Execute();
                if (DeleteRegCVDT.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in DeleteRegCVDT.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
                InputData = string.Format(GlobalStrings.SelectComplementVehiDT, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO, GlobalStrings.V0CONO);
                ExNihiloGenericExtraction InsertCVDT = new ExNihiloGenericExtraction(InputData, OutputData2);
                InsertCVDT.UseTransaction = false;
                InsertCVDT.Execute();
                if (InsertCVDT.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in InsertCVDT.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
            }
        }
        void ComplementoVehiculoDTAD(string OutputData, string OutputData2)
        {
            string sExistReg = "0";
            string InputData = String.Empty;
            InputData = string.Format(GlobalStrings.SelectExistsReg, "MXEICVDA", "SERIE", GlobalStrings.V0SERIE, "FOLIO", GlobalStrings.V0FOLIO, "CONO", GlobalStrings.V0CONO);
            ExNihiloExists ExistRegCVDTDA = new ExNihiloExists(InputData);
            ExistRegCVDTDA.UseTransaction = false;
            ExistRegCVDTDA.Execute();
            if (ExistRegCVDTDA.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in ExistRegCVDTDA.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                    sExistReg = "0";
                }
            }
            if (ExistRegCVDTDA.ObjgetExists.getIfExistsRegistros() != null)
                sExistReg = ExistRegCVDTDA.ObjgetExists.getIfExistsRegistros();
           
            if (sExistReg == "1")
            {
                InputData = GlobalStrings.InicializaRegistroERP;
                ExNihiloGenericExtraction DeleteRegCVDTDA = new ExNihiloGenericExtraction(InputData, OutputData);
                DeleteRegCVDTDA.UseTransaction = false;
                DeleteRegCVDTDA.Execute();
                if (DeleteRegCVDTDA.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in DeleteRegCVDTDA.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                    }
                }             
            

                InputData = string.Format(GlobalStrings.SelectComplementVehiDTAD, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO, GlobalStrings.V0CONO);
                ExNihiloGenericExtraction InsertCVDT = new ExNihiloGenericExtraction(InputData, OutputData2);
                InsertCVDT.UseTransaction = false;
                InsertCVDT.Execute();
                if (InsertCVDT.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in InsertCVDT.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
            }
        }
        void Complemento(string OutputData, string OutputData2)
        {       
                
            string  InputData = GlobalStrings.InicializaRegistroERP;
            ExNihiloGenericExtraction DeleteComplement = new ExNihiloGenericExtraction(InputData, OutputData);
            DeleteComplement.UseTransaction = false;
            DeleteComplement.Execute();
                if (DeleteComplement.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in DeleteComplement.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                    }
                }


                
            ExNihiloGenericExtraction InsertComplement = new ExNihiloGenericExtraction(InputData, OutputData2);
            InsertComplement.UseTransaction = false;
            InsertComplement.Execute();
                if (InsertComplement.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in InsertComplement.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
            
        }
        //
        void ImpuestosLocales(string OutputData,string OutputData2)
        {
            Console.WriteLine("Inicia extraccion complemento impuestos locales");
            string sExistImpuestosLocales = "0";
            string InputData = String.Empty;
            //string OutputData = String.Empty;
            //GlobalStrings.SelectExistsDetallista="";
            InputData = string.Format(GlobalStrings.SelectExistsImpuestosLocales, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
            ExNihiloExists ExistImpuestosLocales = new ExNihiloExists(InputData);
            ExistImpuestosLocales.UseTransaction = false;
            ExistImpuestosLocales.Execute();
            if (ExistImpuestosLocales.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in ExistImpuestosLocales.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                    sExistImpuestosLocales = "0";
                }
            }
            if (ExistImpuestosLocales.ObjgetExists.getIfExistsRegistros() != null)
                sExistImpuestosLocales = ExistImpuestosLocales.ObjgetExists.getIfExistsRegistros();
            if (sExistImpuestosLocales == "1")
            {
                InputData = GlobalStrings.InicializaRegistroERP;
                ExNihiloGenericExtraction DeleteImpuestosLocales = new ExNihiloGenericExtraction(InputData, OutputData);
                DeleteImpuestosLocales.UseTransaction = false;
                DeleteImpuestosLocales.Execute();
                if (DeleteImpuestosLocales.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in DeleteImpuestosLocales.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
                InputData = string.Format(GlobalStrings.SelectImpuestosLocales, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                ExNihiloGenericExtraction ImpuestosLocales = new ExNihiloGenericExtraction(InputData, OutputData2);
                ImpuestosLocales.UseTransaction = false;
                ImpuestosLocales.Execute();
                if (ImpuestosLocales.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in ImpuestosLocales.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
            }
            Console.WriteLine("Finaliza extraccion complemento impuestos locales");
        }
        //Agregado JL para actualizar HD
        public void UpdateMXEIHD(string Sistema, int StatusId, string Message, int Initial, int End, string DateCancel)
        {
            string[] list = Message.Split('#');

            if (list.Length > 1)
            {
                
                var OutputDataUpdate = String.Format(GlobalStrings.UpdateVoucherMXEIHDSL, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO, list[0].ToString(), Initial, End, StatusId, list[1].ToString(), list[2].ToString(), list[3].ToString(), list[4].ToString(), DateCancel);
                ExNihiloSetSite exi = new ExNihiloSetSite(OutputDataUpdate);
                Console.WriteLine(OutputDataUpdate);
                exi.UseTransaction = false;
                exi.Execute();
                if (exi.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in exi.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
            }
        }


        public void UpdateMXEIHDCUSTOM(string Sistema, int StatusId, string Message, int Initial, int End, string DateCancel,string TotalIva, string TotalIvaRetenido, string TotalIsrRetenido)
        {
            string[] list = Message.Split('#');

            if (list.Length > 1)
            {

                var OutputDataUpdate = String.Format(GlobalStrings.UpdateVoucherMXEIHDSLFIELDCUSTOM, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO, list[0].ToString(), Initial, End, StatusId, list[1].ToString(), list[2].ToString(), list[3].ToString(), list[4].ToString(), DateCancel, TotalIva, TotalIvaRetenido, TotalIsrRetenido);
                ExNihiloSetSite exi = new ExNihiloSetSite(OutputDataUpdate);
                Console.WriteLine(OutputDataUpdate);
                exi.UseTransaction = false;
                exi.Execute();
                if (exi.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in exi.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
            }
        }

        public void UpdateMXEIRQ(string Sistema, int StatusId, string Message, int Initial, int End,string CFDIRel)
        {
            string[] list = Message.Split('#');
          
            if (list.Length > 1)
            {
                
                var OutputDataUpdate = String.Format(GlobalStrings.UpdateVoucherMXEIRQUUID, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO, list[0].ToString(), Initial, End, StatusId, list[1].ToString(), list[2].ToString(), list[3].ToString(), list[4].ToString(), CFDIRel);
              
                ExNihiloSetSite exi = new ExNihiloSetSite(OutputDataUpdate);
               
                exi.UseTransaction = false;
                exi.Execute();
                if (exi.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in exi.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
            }
        }
        public void VoucherLog(string Sistema, int StatusId, string Message, int Initial, int End) 
        {
            string InputData = String.Empty;
            string OutputDataInsert = String.Empty;
            string OutputDataUpdate = String.Empty;
            //Agregado por jl para la inserion hd cuando no hay info en rq para xa
            string OutputDataRQ = String.Empty;
            //

            //string OutputDataUpdate2 = String.Empty;
            if (Sistema == "ERP")
            {
                string[] list = Message.Split('#');

                if (list.Length > 1)
                {
                    /* OutputDataUpdate = String.Format(GlobalStrings.UpdateVoucherMXEIRQUUID, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO, list[0].ToString(), Initial, End, StatusId, list[1].ToString(), list[2].ToString(), list[3].ToString(), list[4].ToString(), GlobalStrings.V9CFDIREL);
                     //OutputDataUpdate2 = String.Format(GlobalStringsXA.UpdateVoucherMXEIHDUUID, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO, list[1].ToString(), list[4].ToString());*/

                    //Agregado por jl para la inserion hd cuando no hay info en rq para xa
                    if (GlobalStrings.ERP != "XA")
                    {                       
                        OutputDataUpdate = String.Format(GlobalStrings.UpdateVoucherMXEIRQUUID, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO, list[0].ToString(), Initial, End, StatusId, list[1].ToString(), list[2].ToString(), list[3].ToString(), list[4].ToString(), GlobalStrings.V9CFDIREL, GlobalStrings.SiteERP);
                    }
                    else
                    {
                        //Actualizar en HD
                        //Se aplica la validacion de la configuracion
                        if (GlobalStrings.Config=="1")
                        {

                            Console.WriteLine("Antes del update");                      
                        InputData = String.Format(GlobalStrings.ValidaVoucherMXEIRQ, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                        OutputDataUpdate = String.Format(GlobalStrings.UpdateVoucherMXEIHD, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO, list[0].ToString(), Initial, End, StatusId, list[1].ToString(), list[2].ToString(), list[3].ToString(), list[4].ToString(), GlobalStrings.SiteERP);
                        OutputDataRQ = String.Format(GlobalStrings.UpdateVoucherMXEIRQUUID, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO, list[0].ToString(), Initial, End, StatusId, list[1].ToString(), list[2].ToString(), list[3].ToString(), list[4].ToString(), GlobalStrings.SiteERP);
                        }
                        else
                        {                            
                            OutputDataUpdate = String.Format(GlobalStrings.UpdateVoucherMXEIRQUUID, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO, list[0].ToString(), Initial, End, StatusId, list[1].ToString(), list[2].ToString(), list[3].ToString(), list[4].ToString(), GlobalStrings.SiteERP);
                        }

                    }
                    //

                }
                else
                    OutputDataUpdate = String.Format(GlobalStrings.UpdateVoucherMXEIRQ, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO, Message, Initial, End, StatusId, GlobalStrings.SiteERP);

                
                InputData = String.Format(GlobalStrings.ValidaVoucherMXEIRQ, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                    //{0}=Cono,{1}=Serie,{2}=Folio,{3}=Message,{4}=INLS,{5}=ENDS,{6}=STS
                OutputDataInsert = String.Format(GlobalStrings.InsertVoucherMXEIRQ, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO, Message, Initial, End, StatusId);
                //OutputDataUpdate = String.Format(GlobalStrings.UpdateVoucherMXEIRQ, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO, Message, Initial, End, StatusId);


                //Agregado por jl para la inserion hd cuando no hay info en rq para xa
                if (GlobalStrings.ERP == "XA")
                    OutputDataRQ = "";
                else
                {
                    if (list.Length > 1)
                        OutputDataRQ = String.Format(GlobalStrings.UpdateVoucherMXEIRQUUID, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO, list[0].ToString(), Initial, End, StatusId, list[1].ToString(), list[2].ToString(), list[3].ToString(), list[4].ToString(), GlobalStrings.V9CFDIREL, GlobalStrings.SiteERP);
                    else
                        OutputDataRQ = String.Format(GlobalStrings.UpdateVoucherMXEIRQ, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO, Message, Initial, End, StatusId, GlobalStrings.SiteERP);
                }
                //OutputDataRQ= String.Format(GlobalStrings.UpdateVoucherMXEIRQ, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO, Message, Initial, End, StatusId);
             
                //

            }
            else
            {
                //Agregado por jl para la inserion hd cuando no hay info en rq para xa              
                InputData = String.Format(GlobalStrings.ValidaVoucherLog, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO,GlobalStrings.Parm_Site);                
                OutputDataInsert = String.Format(GlobalStrings.InsertVoucherLog, GlobalStrings.Parm_Site, StatusId, Message, GlobalStrings.comprobanteId, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                OutputDataUpdate = String.Format(GlobalStrings.UpdateVoucherLog, GlobalStrings.Parm_Site, StatusId, Message, GlobalStrings.comprobanteId, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                OutputDataRQ = String.Format(GlobalStrings.UpdateVoucherLog, GlobalStrings.Parm_Site, StatusId, Message, GlobalStrings.comprobanteId, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                //
            }

            //Console.WriteLine(OutputDataUpdate);
            //Console.WriteLine(OutputDataUpdate2);         
            ExNihiloVoucherLog VoucherLog = new ExNihiloVoucherLog(Sistema,InputData, OutputDataInsert, OutputDataRQ,OutputDataUpdate);
            VoucherLog.UseTransaction = false;
            VoucherLog.Execute();
            if (VoucherLog.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in VoucherLog.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                }
            }
        }
        string VerificaStatusVoucher()
        {
            string status = String.Empty;
            string InputData = string.Format(GlobalStrings.VerificaStatusVoucher, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO,GlobalStrings.Parm_Site);          
            ExNihiloVerificaStatusVoucher StatusVoucher = new ExNihiloVerificaStatusVoucher(InputData);
            StatusVoucher.UseTransaction = false;
            StatusVoucher.Execute();
            if (StatusVoucher.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in StatusVoucher.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                }
            }
            if (StatusVoucher.ObjgetStatus.getStatusVoucher() != null)
                status = StatusVoucher.ObjgetStatus.getStatusVoucher();
            return status;
        }
        void Rollback(List<Conceptos> listConceptos)
        {
            string sconceptId = "'00000000-0000-0000-0000-000000000000'";
            foreach (Conceptos Conceptos  in listConceptos)
            {
                sconceptId = sconceptId + ",'"+Conceptos.conceptoId+"'";
            }
            string OutputData = string.Format(GlobalStrings.Rollback, GlobalStrings.comprobanteId, sconceptId);
            ExNihiloRollback Rollback = new ExNihiloRollback(GlobalStrings.InicializaRegistroCountryPack, OutputData);
            Rollback.UseTransaction = false;
            Rollback.Execute();
            if (Rollback.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in Rollback.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                }
            }

        }
        void EXEC_SP(string OutputData)
        {
            //string OutputData = @"Exec ZMX_VoucherGenSP  '" + GlobalStrings.V0SERIE + "',  '" + GlobalStrings.V0FOLIO + "',null ";
            ExNihiloExec_SP exec = new ExNihiloExec_SP(GlobalStrings.InicializaRegistroCountryPack, OutputData);
            exec.UseTransaction = false;
            exec.Execute();
            if (exec.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in exec.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                }
            }
        }
        void InvoiceExtraction()//(string cono, string serie, string folio)
        {
            string sNodoComprobante = "", sNodoEmisor ="", sNodoReceptor="", sNodoConceptosInput = "", sNodoConceptosOutput = "", 
                   sCfdiRelacionados="", sNodoAddendas="",sPos="", sDeleteImpuestos="" ,sDeleteImpuestosLocales="", sInsertImpuestosLocales="" , sEXEC_SP="";
            
            
            //Validar la existencia de las columna MLSITE en cs y obtener su valor para determinar si hat una busqueda multisitio
            //JL 21012019 --- Recuperacion de site para asignar en las consultas CS y utilizar los indices de sql          

            
           
            Console.WriteLine("Inicia recuperacion del sitio");
            GetSite();
            Console.WriteLine("Finaliza recuperacion del sitio");
            
            List<Conceptos> listUpdateConcepts = new List<Conceptos>();
            List<Conceptos> listConceptos = new List<Conceptos>();
            listConceptos.Clear();
            listUpdateConcepts.Clear();
            //Agregado por jl para el desarrollo de los conceptos faltantes, lista que almacena los conceptos no agregados la primera ves que se extraiga la factura
            List<Items> listNewConceptos = new List<Items>();
            listNewConceptos.Clear();

            //


            //Agregado a por jl para el calculo de importe
            Console.WriteLine("Inicia recuperacion de configuraciones");
            if (GlobalStrings.ListConfiguraciones != null)
                GlobalStrings.ListConfiguraciones.Clear();       
            NodoConfiguraciones(GlobalStrings.SelectConfiguracion);
            Console.WriteLine("Finaliza recuperacion de configuraciones");
            //////////////
            //Agreagado por JL para el manejo de los decimales
            //
            //Agregado por JL para recuperar decimales
            Console.WriteLine("Inicia consulta a voucher decimals");
            //Configuracion agregada para usar configuración de los decimales.
            if (GlobalStrings.UseConfigDecimals)
                RecuperarVoucherDecimals();
            Console.WriteLine("Finaliza consulta a voucher decimals");
            //
            Console.WriteLine("Inicia consulta de status del voucher");           
            string status = VerificaStatusVoucher();
            Console.WriteLine("Finaliza consulta de status del voucher");            
            if (status == "INSERT")
            {
                Console.WriteLine("Inicia proceso de insercion del comprobante");

                Console.WriteLine("Inicia proceso de validaciones");
                validaciones();
                Console.WriteLine("Finaliza proceso de validaciones");
                try
                {
                    if (msg.bShowMsg != true)
                    {
                        Console.WriteLine("Inicia extraccion datos compañia");
                        GetDataCompany();
                        Console.WriteLine("Finaliza extraccion datos compañia");
                        if (GlobalStrings.ERP == "XA")
                            sNodoComprobante = string.Format(GlobalStrings.InsertVoucherXA, GlobalStrings.version, GlobalStrings.formaPago, GlobalStrings.lugarExpedicion, GlobalStrings.metodoPago, GlobalStrings.SiteRef, GlobalStrings.V0CONO, GlobalStrings.pathCFDI);
                        else
                        { //Validar columnas
                            if (GlobalStrings.UseMultisite)
                            {
                                GlobalStrings.ValidateCol = "0";
                                string InputDataCol = string.Format(GlobalStrings.ValidarExistenciaColumna, "MLSITE", "ZMX_Voucher");
                                ExNihiloValidarExistenciaColumna ExitsColumn = new ExNihiloValidarExistenciaColumna(InputDataCol);
                                ExitsColumn.UseTransaction = false;
                                ExitsColumn.Execute();
                                if (ExitsColumn.GetAllErrors().Count() > 0)
                                {
                                    foreach (Exception ex in ExitsColumn.GetAllErrors())
                                    {
                                        Console.WriteLine(ex.Message);
                                        VoucherLog("CountryPack", 5, ex.Message, 0, 10);
                                        //Rollback();
                                    }
                                }
                                if (GlobalStrings.ValidateCol == "1")
                                    sNodoComprobante = string.Format(GlobalStrings.InsertVoucherWithColumn, GlobalStrings.version, GlobalStrings.formaPago, GlobalStrings.lugarExpedicion, GlobalStrings.metodoPago, GlobalStrings.SiteRef, GlobalStrings.V0CONO, GlobalStrings.pathCFDI);
                                else
                                    sNodoComprobante = string.Format(GlobalStrings.InsertVoucher, GlobalStrings.version, GlobalStrings.formaPago, GlobalStrings.lugarExpedicion, GlobalStrings.metodoPago, GlobalStrings.SiteRef, GlobalStrings.V0CONO, GlobalStrings.pathCFDI);
                            }
                            else
                            {
                                GlobalStrings.ValidateCol = "0";
                                sNodoComprobante = string.Format(GlobalStrings.InsertVoucher, GlobalStrings.version, GlobalStrings.formaPago, GlobalStrings.lugarExpedicion, GlobalStrings.metodoPago, GlobalStrings.SiteRef, GlobalStrings.V0CONO, GlobalStrings.pathCFDI);
                            }
                        }
                            
                        //Extraccion nodo comprobante
                        NodoComprobante(sNodoComprobante);
                        //

                        //Extraccion nodo emisor
                        sNodoEmisor = string.Format(GlobalStrings.InsertEmisor, GlobalStrings.comprobanteId, GlobalStrings.SiteRef);
                        NodoEmisor(sNodoEmisor);
                        //
                       
                        //Nodo Receptor
                        sNodoReceptor = string.Format(GlobalStrings.InsertReceptor, GlobalStrings.comprobanteId, GlobalStrings.SiteRef);
                        NodoReceptor(sNodoReceptor);
                        //

                        //Extraccion nodo conceptos                       
                        sNodoConceptosInput = string.Format(GlobalStrings.SelectNodoConcepto, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                        sNodoConceptosOutput = string.Format(GlobalStrings.InsertNodoConcepto, GlobalStrings.comprobanteId, GlobalStrings.SiteRef);
                           
                        listConceptos = NodoConceptos(sNodoConceptosInput, sNodoConceptosOutput);
                        //

                        //Extraccion impuestos
                        sDeleteImpuestos = string.Format(GlobalStrings.DeleteImpuestos, GlobalStrings.comprobanteId);
                        LimpiaNodoImpuesto(sDeleteImpuestos);
                        NodoImpuestos(listConceptos);
                        //Extraccion impuestos


                        if (GlobalStrings.ERP != "XA")
                        {
                            ///Validacion de tablas parte
                            var sValidateTableKITS = string.Format(GlobalStrings.ValidateExistTable, "MXEIPTS");

                            if (ValidateExistTableERP(sValidateTableKITS) == "1")
                            {
                                //Validar si hay registros relacionados a la factura
                                var sKitsInput = string.Format(GlobalStrings.SelectMXEIKits, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                                var sKitsOutput = string.Format(GlobalStrings.InsertKitsParts, GlobalStrings.SiteRef, GlobalStrings.comprobanteId);
                                Console.WriteLine("Inicia extraccion nodo kits");
                                sDeleteImpuestos = string.Format(GlobalStrings.DeleteKits, GlobalStrings.comprobanteId);
                                LimpiaNodoKits(sDeleteImpuestos);
                                //Eliminar kits si hay reimpresion
                                ExNihiloNodoKits NodoKits = new ExNihiloNodoKits(sKitsInput, sKitsOutput);
                                NodoKits.UseTransaction = false;
                                NodoKits.Execute();
                                if (NodoKits.GetAllErrors().Count() > 0)
                                {
                                    foreach (Exception ex in NodoKits.GetAllErrors())
                                    {
                                        Console.WriteLine(ex.Message);
                                    }
                                }

                                Console.WriteLine("Finaliza extraccion nodo kits");

                            }
                        }
                        else
                        {
                             if (GlobalStrings.ExtraerKitsXA)
                             {
                                 //Validar si hay registros relacionados a la factura
                                 var sKitsInput = string.Format(GlobalStrings.SelectMXEIKits, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                                 var sKitsOutput = string.Format(GlobalStrings.InsertKitsParts, GlobalStrings.SiteRef, GlobalStrings.comprobanteId);
                                 Console.WriteLine("Inicia extraccion nodo kits");
                                 sDeleteImpuestos = string.Format(GlobalStrings.DeleteKits, GlobalStrings.comprobanteId);
                                 LimpiaNodoKits(sDeleteImpuestos);
                                 //Eliminar kits si hay reimpresion
                                 ExNihiloNodoKits NodoKits = new ExNihiloNodoKits(sKitsInput, sKitsOutput);
                                 NodoKits.UseTransaction = false;
                                 NodoKits.Execute();
                                 if (NodoKits.GetAllErrors().Count() > 0)
                                 {
                                     foreach (Exception ex in NodoKits.GetAllErrors())
                                     {
                                         Console.WriteLine(ex.Message);
                                     }
                                 }

                                 Console.WriteLine("Finaliza extraccion nodo kits");
                             }
                        }
                        ///

                       
                        //CXA

                        //Extraccion pedimentos
                     //   if(GlobalStrings.ERP != "XA")
                            PedimentosConceptos(listConceptos);     
                        //                
                       
                        //Extraccion Addendas
                        sNodoAddendas = string.Format(GlobalStrings.InsertAddenda, GlobalStrings.comprobanteId, GlobalStrings.SiteRef);
                        NodoAddendas(sNodoAddendas);
                        //

                        //Extraccion comentarios
                        Comentarios();
                        //
                        //Extraccion documentos relacionado
                        if (GlobalStrings.ERP != "XA")
                        { 
                            GlobalStrings.ValidateCol = "0";
                        var InputDataCol1 = string.Format(GlobalStrings.ValidarExistenciaColumna, "amount", "ZMX_CfdiRelated");
                       var ExitsColumn1 = new ExNihiloValidarExistenciaColumna(InputDataCol1);
                        ExitsColumn1.UseTransaction = false;
                        ExitsColumn1.Execute();
                        if (ExitsColumn1.GetAllErrors().Count() > 0)
                        {
                            foreach (Exception ex in ExitsColumn1.GetAllErrors())
                            {
                                Console.WriteLine(ex.Message);
                                VoucherLog("CountryPack", 5, ex.Message, 0, 10);
                                //Rollback();
                            }
                        }
                            if (GlobalStrings.ValidateCol == "1")
                            {
                                GlobalStrings.ValidateCol = "0";
                                ///Validar si existe columna del lado sl
                                var InputDataColSL = string.Format(GlobalStrings.ValidarExistenciaColumna, "VJIIMP", "MXEIRC");
                                var ExitsColumnSL = new ExNihiloValidarExistenciaColumnaSL(InputDataColSL);
                                ExitsColumnSL.UseTransaction = false;
                                ExitsColumnSL.Execute();
                                if (ExitsColumnSL.GetAllErrors().Count() > 0)
                                {
                                    foreach (Exception ex in ExitsColumnSL.GetAllErrors())
                                    {
                                        Console.WriteLine(ex.Message);
                                        VoucherLog("CountryPack", 5, ex.Message, 0, 10);
                                        //Rollback();
                                    }
                                }
                                if (GlobalStrings.ValidateCol == "1")
                                {
                                    //Validar si existe la columna del lado sl typerelation
                                    string ColCS = ",AMOUNT";
                                    string ColSL = ",isnull(@VJIIMP,0)";
                                    GlobalStrings.ValidateCol = "0";
                                    var InputDataCol2 = string.Format(GlobalStrings.ValidarExistenciaColumna, "VJRELTYPE", "MXEIRC");
                                    var ExitsColumn2 = new ExNihiloValidarExistenciaColumnaSL(InputDataCol2);
                                    ExitsColumn2.UseTransaction = false;
                                    ExitsColumn2.Execute();
                                    if (ExitsColumn2.GetAllErrors().Count() > 0)
                                    {
                                        foreach (Exception ex in ExitsColumn2.GetAllErrors())
                                        {
                                            Console.WriteLine(ex.Message);
                                            VoucherLog("CountryPack", 5, ex.Message, 0, 10);
                                            //Rollback();
                                        }
                                    }
                                    if (GlobalStrings.ValidateCol == "1")
                                    {
                                        ColCS += ",relationType";
                                        ColSL += ",@VJRELTYPE";
                                        GlobalStrings.ValidateCol = "0";
                                    }
                                    sCfdiRelacionados = string.Format(GlobalStrings.InsertCfdiRelacionadosAmount, GlobalStrings.comprobanteId, GlobalStrings.Parm_Site, ColCS, ColSL);
                                }
                                else
                                {
                                    string ColCS = string.Empty;
                                    string ColSL = string.Empty;
                                    var InputDataCol2 = string.Format(GlobalStrings.ValidarExistenciaColumna, "VJRELTYPE", "MXEIRC");
                                    var ExitsColumn2 = new ExNihiloValidarExistenciaColumnaSL(InputDataCol2);
                                    ExitsColumn2.UseTransaction = false;
                                    ExitsColumn2.Execute();
                                    if (ExitsColumn2.GetAllErrors().Count() > 0)
                                    {
                                        foreach (Exception ex in ExitsColumn2.GetAllErrors())
                                        {
                                            Console.WriteLine(ex.Message);
                                            VoucherLog("CountryPack", 5, ex.Message, 0, 10);
                                            //Rollback();
                                        }
                                    }
                                    if (GlobalStrings.ValidateCol == "1")
                                    {
                                        ColCS += ",relationType";
                                        ColSL += ",@VJRELTYPE";
                                        GlobalStrings.ValidateCol = "0";
                                        sCfdiRelacionados = string.Format(GlobalStrings.InsertCfdiRelacionadosAmount, GlobalStrings.comprobanteId, GlobalStrings.Parm_Site, ColCS, ColSL);
                                    }
                                    else
                                    sCfdiRelacionados = string.Format(GlobalStrings.InsertCfdiRelacionados, GlobalStrings.comprobanteId, GlobalStrings.Parm_Site);                                   
                                }                               
                                CfdiRelacionados(sCfdiRelacionados);
                            }
                            else
                            {
                                //Validar si existe la columna de tipo de relacion en la intermedia
                                string ColCS = "";
                                string ColSL = "";
                                var InputDataCol2 = string.Format(GlobalStrings.ValidarExistenciaColumna, "VJRELTYPE", "MXEIRC");
                                var ExitsColumn2 = new ExNihiloValidarExistenciaColumnaSL(InputDataCol2);
                                ExitsColumn2.UseTransaction = false;
                                ExitsColumn2.Execute();
                                if (ExitsColumn2.GetAllErrors().Count() > 0)
                                {
                                    foreach (Exception ex in ExitsColumn2.GetAllErrors())
                                    {
                                        Console.WriteLine(ex.Message);
                                        VoucherLog("CountryPack", 5, ex.Message, 0, 10);
                                        //Rollback();
                                    }
                                }
                                if (GlobalStrings.ValidateCol == "1")
                                {
                                    ColCS += ",relationType";
                                    ColSL += ",@VJRELTYPE";
                                    GlobalStrings.ValidateCol = "0";
                                    sCfdiRelacionados = string.Format(GlobalStrings.InsertCfdiRelacionadosAmount, GlobalStrings.comprobanteId, GlobalStrings.Parm_Site, ColCS, ColSL);
                                }
                                else
                                    sCfdiRelacionados = string.Format(GlobalStrings.InsertCfdiRelacionadosAmount, GlobalStrings.comprobanteId, GlobalStrings.Parm_Site, "");
                            }
                        
                            CfdiRelacionados(sCfdiRelacionados);
                        }
                        else
                        {
                            sCfdiRelacionados = string.Format(GlobalStrings.InsertCfdiRelacionados, GlobalStrings.comprobanteId, GlobalStrings.Parm_Site);
                            CfdiRelacionados(sCfdiRelacionados);
                        }

                        //Extraccion punto de venta
                        //sPos = string.Format(GlobalStrings.InsertPOS, GlobalStrings.comprobanteId);
                        //POS(sPos); //utilizado para en XA para obbtener Shipto
                        //

                        if (GlobalStrings.ERP != "XA")
                        {
                            sPos = string.Format(GlobalStrings.InsertPOS, GlobalStrings.comprobanteId);
                            POS(sPos); //utilizado para en XA para obbtener Shipto
                        }
                        else
                        {
                            sPos = string.Format(GlobalStrings.InsertPOSXA, GlobalStrings.comprobanteId);
                            POS(sPos); //utilizado para en XA para obbtener Shipto
                        }
                        if (GlobalStrings.ERP != "XA")
                        {
                            //Agregado por JL - 24102019 para validar la existencia de las tablas
                            var sValidateTableERPCompDet = string.Format(GlobalStrings.ValidateExistTable, "MXADHD");
                            if (ValidateExistTableERP(sValidateTableERPCompDet) == "1")
                            {
                                Detallista();                               
                            }
                        }
                        else
                        {
                            Detallista();                            
                        }
                        //----------------------------------------------------
                        //Agregado por JL - 24102019 para validar la existencia de las tablas
                        if (GlobalStrings.ERP != "XA")
                        {
                            var sValidateTableERPImpueLoc = string.Format(GlobalStrings.ValidateExistTable, "MXEIXL");

                            if (ValidateExistTableERP(sValidateTableERPImpueLoc) == "1")
                            {
                                sDeleteImpuestosLocales = string.Format(GlobalStrings.DeleteImpuestosLocales, GlobalStrings.comprobanteId);
                                sInsertImpuestosLocales = string.Format(GlobalStrings.InsertImpuestosLocales, GlobalStrings.comprobanteId);
                                ImpuestosLocales(sDeleteImpuestosLocales, sInsertImpuestosLocales);                                
                            }
                        }
                        else
                        {
                            sDeleteImpuestosLocales = string.Format(GlobalStrings.DeleteImpuestosLocales, GlobalStrings.comprobanteId);
                            sInsertImpuestosLocales = string.Format(GlobalStrings.InsertImpuestosLocales, GlobalStrings.comprobanteId);
                            ImpuestosLocales(sDeleteImpuestosLocales, sInsertImpuestosLocales);                           
                        }
                        //                      
                        if (GlobalStrings.ERP != "XA")
                        {
                            //Agregado por JL Para el manejo del complemento de vehiculos
                            //Validar existencia de la tabla del complemenento de vehiculos
                            var sValidateTableERP = string.Format(GlobalStrings.ValidateExistTable, "MXEICV");
                            var sValidateTableCS = string.Format(GlobalStrings.ValidateExistTable, "ZMX_MXEICV");
                            if (ValidateExistTableERP(sValidateTableERP) == "1" && ValidateExistTableCS(sValidateTableCS) == "1")
                            {
                                //Proceso para inserta el encabezado del complemento de Vehiculos
                                Console.WriteLine("Inicia extraccion complemento Vehiculo");

                                var sInsertDHCV = string.Format(GlobalStrings.InsertComplementVehiHD, GlobalStrings.comprobanteId);
                                var sDeleteDHCV = string.Format(GlobalStrings.DeleteComplementVehiHD, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                                ComplementoVehiculoHD(sDeleteDHCV, sInsertDHCV);
                                Console.WriteLine("Complemento vehiculos hd");
                                //
                                //Proceso para insertar el detalle del complemento de Vehiculos   
                                var sInsertDTCV = string.Format(GlobalStrings.InsertComplementVehiDT, GlobalStrings.comprobanteId);
                                var sDeleteDTCV = string.Format(GlobalStrings.DeleteComplementVehiDT, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                                ComplementoVehiculoDT(sDeleteDTCV, sInsertDTCV);
                                Console.WriteLine("Complemento vehiculos dt");
                                //
                                //Proceso para insertar la info del detalle de la aduana del complemento
                                var sInsertDTADCV = string.Format(GlobalStrings.InsertComplementVehiDTAD, GlobalStrings.comprobanteId);
                                var sDeleteDTADCV = string.Format(GlobalStrings.DeleteComplementVehiDTAD, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                                ComplementoVehiculoDTAD(sDeleteDTADCV, sInsertDTADCV);
                                Console.WriteLine("Complemento vehiculos dtad");

                                Console.WriteLine("Finaliza extraccion complemento vehiculos");
                            }
                        }

                        //
                        Console.WriteLine("Inicia registro  en voucherLog countrypack");
                        VoucherLog("CountryPack", 10, "Extracción finalizada exitosamente", 0, 10);
                        Console.WriteLine("Finaliza registro  en voucherLog countrypack");

                        //CXA
                        if (GlobalStrings.ERP != "XA")
                        {
                            Console.WriteLine("Inicia registro en voucherLog erp");
                            VoucherLog("ERP", 10, "Extracción finalizada exitosamente", 0, 10);
                            Console.WriteLine("Finaliza registro en voucherLog erp");

                        }
                     
                       
                        if (GlobalStrings.intRollback == 1)
                        {
                            Console.WriteLine("Inicia proceso de rollback");
                            Rollback(listConceptos);
                            VoucherLog("CountryPack", 5, GlobalStrings.msgNotExistUM, 0, 10);
                            Console.WriteLine("Finaliza proceso de rollback");
                        }
                        else
                        {
                            Console.WriteLine("Inicia llamado a ZMX_VoucherGenSP");
                            sEXEC_SP = @"Exec ZMX_VoucherGenSP  '" + GlobalStrings.V0SERIE + "',  '" + GlobalStrings.V0FOLIO + "',null ";
                            EXEC_SP(sEXEC_SP);
                            Console.WriteLine("Finaliza llamado a ZMX_VoucherGenSP");
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                    Console.WriteLine("Inicia proceso de rollback");
                    Rollback(listConceptos);
                    VoucherLog("CountryPack", 5, ex.Message, 0, 10);
                    Console.WriteLine("Finaliza proceso de rollback");
                }
                Console.WriteLine("Finaliza proceso de insercion del comprobante");
            }
            if (status == "UPDATE")
            {
                Console.WriteLine("Inicia proceso de actualizacion del comprobante");
              
                //Agregado a por jl para el calculo de importe
                Console.WriteLine("Inicia recuperacion de configuraciones");
                GlobalStrings.ListConfiguraciones.Clear();               
                NodoConfiguraciones(GlobalStrings.SelectConfiguracion);
                Console.WriteLine("Finaliza recuperacion de configuraciones");
                /////////////


                //Agregado por JL para los conceptos faltantes               
                listUpdateConcepts = validarItems(status, out listNewConceptos);//Valida y obtiene la lista de Conceptos              

                GlobalStrings.comprobanteIdUpdate = GlobalStrings.comprobanteId;
                Console.WriteLine("Inicia proceso de validaciones");
                validaciones();
                Console.WriteLine("Finaliza proceso de validaciones");
                try
                {
                    if (msg.bShowMsg != true)
                    {
                        Console.WriteLine("Inicia extaccion de compañia");
                        GetDataCompany();
                        Console.WriteLine("Finaliza extaccion de compañia");
                        if (GlobalStrings.ERP == "XA")
                            sNodoComprobante = string.Format(GlobalStrings.UpdateVoucher, GlobalStrings.version, GlobalStrings.formaPago, GlobalStrings.lugarExpedicion, GlobalStrings.metodoPago, GlobalStrings.SiteRef, GlobalStrings.comprobanteId, GlobalStrings.V0CONO, GlobalStrings.pathCFDI);
                        else
                        {
                            if (GlobalStrings.UseMultisite)
                            {
                                GlobalStrings.ValidateCol = "0";
                                string InputDataCol = string.Format(GlobalStrings.ValidarExistenciaColumna, "MLSITE", "ZMX_Voucher");
                                ExNihiloValidarExistenciaColumna ExitsColumn = new ExNihiloValidarExistenciaColumna(InputDataCol);
                                ExitsColumn.UseTransaction = false;
                                ExitsColumn.Execute();
                                if (ExitsColumn.GetAllErrors().Count() > 0)
                                {
                                    foreach (Exception ex in ExitsColumn.GetAllErrors())
                                    {
                                        Console.WriteLine(ex.Message);
                                        VoucherLog("CountryPack", 5, ex.Message, 0, 10);
                                        //Rollback();
                                    }
                                }
                                if (GlobalStrings.ValidateCol == "1")
                                    sNodoComprobante = string.Format(GlobalStrings.UpdateVoucherWithColumn, GlobalStrings.version, GlobalStrings.formaPago, GlobalStrings.lugarExpedicion, GlobalStrings.metodoPago, GlobalStrings.SiteRef, GlobalStrings.comprobanteId, GlobalStrings.V0CONO, GlobalStrings.pathCFDI);
                                else
                                    sNodoComprobante = string.Format(GlobalStrings.UpdateVoucher, GlobalStrings.version, GlobalStrings.formaPago, GlobalStrings.lugarExpedicion, GlobalStrings.metodoPago, GlobalStrings.SiteRef, GlobalStrings.comprobanteId, GlobalStrings.V0CONO, GlobalStrings.pathCFDI);
                            }
                            else
                            {
                                GlobalStrings.ValidateCol = "0";
                                sNodoComprobante = string.Format(GlobalStrings.UpdateVoucher, GlobalStrings.version, GlobalStrings.formaPago, GlobalStrings.lugarExpedicion, GlobalStrings.metodoPago, GlobalStrings.SiteRef, GlobalStrings.comprobanteId, GlobalStrings.V0CONO, GlobalStrings.pathCFDI);
                            }
                        }
                        //Extraccion Comprobante
                        NodoComprobante(sNodoComprobante);
                        //
                        //Extraccion Emisor
                        sNodoEmisor = string.Format(GlobalStrings.UpdateEmisor, GlobalStrings.comprobanteIdUpdate, GlobalStrings.SiteRef);
                        NodoEmisor(sNodoEmisor);
                        //
                        //Extraccion receptor
                        sNodoReceptor = string.Format(GlobalStrings.UpdateReceptor, GlobalStrings.comprobanteIdUpdate, GlobalStrings.SiteRef);
                        NodoReceptor(sNodoReceptor);
                        //

                        sDeleteImpuestos = string.Format(GlobalStrings.DeleteKits, GlobalStrings.comprobanteIdUpdate);
                        LimpiaNodoKits(sDeleteImpuestos);

                        //Eliminar conceptos antes de actualizar                        
                        string deleteconcept = string.Format(GlobalStrings.DeleteConceptos, GlobalStrings.comprobanteIdUpdate);
                        LimpiaNodoConcepto(deleteconcept);
                        //

                   
                        //Extraccion conceptos
                        sNodoConceptosInput = string.Format(GlobalStrings.SelectNodoConcepto, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                        sNodoConceptosOutput = string.Format(GlobalStrings.InsertNodoConcepto, GlobalStrings.comprobanteIdUpdate, GlobalStrings.SiteRef);
                        listConceptos = NodoConceptos(sNodoConceptosInput, sNodoConceptosOutput);
                        //

                        //Extraccion nodo impuestos
                        NodoImpuestos(listConceptos);
                        //
                        if (GlobalStrings.ERP != "XA")
                        {
                            ///Validacion de tablas parte
                            var sValidateTableKITS = string.Format(GlobalStrings.ValidateExistTable, "MXEIPTS");

                            if (ValidateExistTableERP(sValidateTableKITS) == "1")
                            {
                                //Validar si hay registros relacionados a la factura
                                var sKitsInput = string.Format(GlobalStrings.SelectMXEIKits, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                                var sKitsOutput = string.Format(GlobalStrings.InsertKitsParts, GlobalStrings.SiteRef, GlobalStrings.comprobanteIdUpdate);
                                Console.WriteLine("Inicia extraccion nodo kits");
                             
                                //Eliminar kits si hay reimpresion
                                ExNihiloNodoKits NodoKits = new ExNihiloNodoKits(sKitsInput, sKitsOutput);
                                NodoKits.UseTransaction = false;
                                NodoKits.Execute();
                                if (NodoKits.GetAllErrors().Count() > 0)
                                {
                                    foreach (Exception ex in NodoKits.GetAllErrors())
                                    {
                                        Console.WriteLine(ex.Message);
                                    }
                                }

                                Console.WriteLine("Finaliza extraccion nodo kits");

                            }
                        }
                        else
                        {
                            if (GlobalStrings.ExtraerKitsXA)
                            {
                                //Validar si hay registros relacionados a la factura
                                var sKitsInput = string.Format(GlobalStrings.SelectMXEIKits, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                                var sKitsOutput = string.Format(GlobalStrings.InsertKitsParts, GlobalStrings.SiteRef, GlobalStrings.comprobanteId);
                                Console.WriteLine("Inicia extraccion nodo kits");
                                sDeleteImpuestos = string.Format(GlobalStrings.DeleteKits, GlobalStrings.comprobanteId);
                                LimpiaNodoKits(sDeleteImpuestos);
                                //Eliminar kits si hay reimpresion
                                ExNihiloNodoKits NodoKits = new ExNihiloNodoKits(sKitsInput, sKitsOutput);
                                NodoKits.UseTransaction = false;
                                NodoKits.Execute();
                                if (NodoKits.GetAllErrors().Count() > 0)
                                {
                                    foreach (Exception ex in NodoKits.GetAllErrors())
                                    {
                                        Console.WriteLine(ex.Message);
                                    }
                                }

                                Console.WriteLine("Finaliza extraccion nodo kits");
                            }
                        }
                        //Extraccion pedimentos
                        //if (GlobalStrings.ERP != "XA")
                        PedimentosConceptos(listConceptos);
                        //
                        //Extraccion addendas
                        sNodoAddendas = string.Format(GlobalStrings.InsertAddenda, GlobalStrings.comprobanteIdUpdate, GlobalStrings.SiteRef);
                        NodoAddendas(sNodoAddendas);
                        //
                        //Extraccion comentarios
                        Comentarios();
                        //
                        //Extraccion documentos relacionados
                        if (GlobalStrings.ERP != "XA")
                        {
                            GlobalStrings.ValidateCol = "0";
                            var InputDataCol1 = string.Format(GlobalStrings.ValidarExistenciaColumna, "amount", "ZMX_CfdiRelated");
                            var ExitsColumn1 = new ExNihiloValidarExistenciaColumna(InputDataCol1);
                            ExitsColumn1.UseTransaction = false;
                            ExitsColumn1.Execute();
                            if (ExitsColumn1.GetAllErrors().Count() > 0)
                            {
                                foreach (Exception ex in ExitsColumn1.GetAllErrors())
                                {
                                    Console.WriteLine(ex.Message);
                                    VoucherLog("CountryPack", 5, ex.Message, 0, 10);
                                    //Rollback();
                                }
                            }
                            if (GlobalStrings.ValidateCol == "1")
                            {
                                GlobalStrings.ValidateCol = "0";
                                ///Validar si existe columna del lado sl
                                var InputDataColSL = string.Format(GlobalStrings.ValidarExistenciaColumna, "VJIIMP", "MXEIRC");
                                var ExitsColumnSL = new ExNihiloValidarExistenciaColumnaSL(InputDataColSL);
                                ExitsColumnSL.UseTransaction = false;
                                ExitsColumnSL.Execute();
                                if (ExitsColumnSL.GetAllErrors().Count() > 0)
                                {
                                    foreach (Exception ex in ExitsColumnSL.GetAllErrors())
                                    {
                                        Console.WriteLine(ex.Message);
                                        VoucherLog("CountryPack", 5, ex.Message, 0, 10);
                                        //Rollback();
                                    }
                                }
                                if (GlobalStrings.ValidateCol == "1")
                                {

                                    //Validar si existe la columna del lado sl typerelation
                                    string ColCS = ",AMOUNT";
                                    string ColSL = ",isnull(@VJIIMP,0)";
                                    GlobalStrings.ValidateCol = "0";
                                    var InputDataCol2 = string.Format(GlobalStrings.ValidarExistenciaColumna, "VJRELTYPE", "MXEIRC");
                                    var ExitsColumn2 = new ExNihiloValidarExistenciaColumnaSL(InputDataCol2);
                                    ExitsColumn2.UseTransaction = false;
                                    ExitsColumn2.Execute();
                                    if (ExitsColumn2.GetAllErrors().Count() > 0)
                                    {
                                        foreach (Exception ex in ExitsColumn2.GetAllErrors())
                                        {
                                            Console.WriteLine(ex.Message);
                                            VoucherLog("CountryPack", 5, ex.Message, 0, 10);
                                            //Rollback();
                                        }
                                    }
                                    if (GlobalStrings.ValidateCol == "1")
                                    {
                                        ColCS += ",relationType";
                                        ColSL += ",@VJRELTYPE";
                                        GlobalStrings.ValidateCol = "0";
                                    }
                                    sCfdiRelacionados = string.Format(GlobalStrings.InsertCfdiRelacionadosAmount, GlobalStrings.comprobanteIdUpdate.ToString().ToUpper(), GlobalStrings.Parm_Site, ColCS, ColSL);
                                }
                                else
                                {
                                    string ColCS = string.Empty;
                                    string ColSL = string.Empty;
                                    GlobalStrings.ValidateCol = "0";
                                    var InputDataCol2 = string.Format(GlobalStrings.ValidarExistenciaColumna, "VJRELTYPE", "MXEIRC");
                                    var ExitsColumn2 = new ExNihiloValidarExistenciaColumnaSL(InputDataCol2);
                                    ExitsColumn2.UseTransaction = false;
                                    ExitsColumn2.Execute();
                                    if (ExitsColumn2.GetAllErrors().Count() > 0)
                                    {
                                        foreach (Exception ex in ExitsColumn2.GetAllErrors())
                                        {
                                            Console.WriteLine(ex.Message);
                                            VoucherLog("CountryPack", 5, ex.Message, 0, 10);
                                            //Rollback();
                                        }
                                    }
                                    if (GlobalStrings.ValidateCol == "1")
                                    {
                                        ColCS += ",relationType";
                                        ColSL += ",@VJRELTYPE";
                                        GlobalStrings.ValidateCol = "0";
                                        sCfdiRelacionados = string.Format(GlobalStrings.InsertCfdiRelacionadosAmount, GlobalStrings.comprobanteIdUpdate.ToString().ToUpper(), GlobalStrings.Parm_Site, ColCS, ColSL);
                                    }
                                    else
                                        sCfdiRelacionados = string.Format(GlobalStrings.InsertCfdiRelacionados, GlobalStrings.comprobanteIdUpdate.ToString().ToUpper(), GlobalStrings.Parm_Site);
                                }
                                CfdiRelacionados(sCfdiRelacionados);
                            }
                            else
                            {
                                //Validar si existe la columna de tipo de relacion en la intermedia
                                string ColCS = "";
                                string ColSL = "";
                                var InputDataCol2 = string.Format(GlobalStrings.ValidarExistenciaColumna, "VJRELTYPE", "MXEIRC");
                                var ExitsColumn2 = new ExNihiloValidarExistenciaColumnaSL(InputDataCol2);
                                ExitsColumn2.UseTransaction = false;
                                ExitsColumn2.Execute();
                                if (ExitsColumn2.GetAllErrors().Count() > 0)
                                {
                                    foreach (Exception ex in ExitsColumn2.GetAllErrors())
                                    {
                                        Console.WriteLine(ex.Message);
                                        VoucherLog("CountryPack", 5, ex.Message, 0, 10);
                                        //Rollback();
                                    }
                                }
                                if (GlobalStrings.ValidateCol == "1")
                                {
                                    ColCS += ",relationType";
                                    ColSL += ",@VJRELTYPE";
                                    GlobalStrings.ValidateCol = "0";
                                    sCfdiRelacionados = string.Format(GlobalStrings.InsertCfdiRelacionadosAmount, GlobalStrings.comprobanteIdUpdate.ToString().ToUpper(), GlobalStrings.Parm_Site, ColCS, ColSL);
                                }
                                else
                                    sCfdiRelacionados = string.Format(GlobalStrings.InsertCfdiRelacionadosAmount, GlobalStrings.comprobanteIdUpdate.ToString().ToUpper(), GlobalStrings.Parm_Site, "");
                            }
                            CfdiRelacionados(sCfdiRelacionados);
                        }
                        else
                        {
                            sCfdiRelacionados = string.Format(GlobalStrings.InsertCfdiRelacionados, GlobalStrings.comprobanteIdUpdate.ToString().ToUpper(), GlobalStrings.Parm_Site);
                            Console.WriteLine(sCfdiRelacionados);
                            CfdiRelacionados(sCfdiRelacionados);
                        }

                        //Extraccion puntos de venta
                        if (GlobalStrings.ERP != "XA")
                        { 
                        sPos = string.Format(GlobalStrings.InsertPOS, GlobalStrings.comprobanteIdUpdate);
                        POS(sPos); //utilizado para en XA para obbtener Shipto
                         }
                        else
                        {
                            sPos = string.Format(GlobalStrings.InsertPOSXA, GlobalStrings.comprobanteIdUpdate);
                            POS(sPos); //utilizado para en XA para obbtener Shipto
                        }
                       //
                        if (GlobalStrings.ERP != "XA")
                        {
                            //Agregado por JL - 24102019 para validar la existencia de las tablas
                            var sValidateTableERPCompDet = string.Format(GlobalStrings.ValidateExistTable, "MXADHD");
                            if (ValidateExistTableERP(sValidateTableERPCompDet) == "1")
                            {
                                Detallista();                               
                            }
                        }
                        else
                        {
                            Detallista();                           
                        }
                        //----------------------------------------------------
                        if (GlobalStrings.ERP != "XA")
                        {
                            //Agregado por JL - 24102019 para validar la existencia de las tablas
                            var sValidateTableERPImpueLoc = string.Format(GlobalStrings.ValidateExistTable, "MXEIXL");

                            if (ValidateExistTableERP(sValidateTableERPImpueLoc) == "1")
                            {
                                sDeleteImpuestosLocales = string.Format(GlobalStrings.DeleteImpuestosLocales, GlobalStrings.comprobanteIdUpdate);
                                sInsertImpuestosLocales = string.Format(GlobalStrings.InsertImpuestosLocales, GlobalStrings.comprobanteIdUpdate);
                                ImpuestosLocales(sDeleteImpuestosLocales, sInsertImpuestosLocales);                               
                            }
                        }
                        else
                        {
                            sDeleteImpuestosLocales = string.Format(GlobalStrings.DeleteImpuestosLocales, GlobalStrings.comprobanteIdUpdate);
                            sInsertImpuestosLocales = string.Format(GlobalStrings.InsertImpuestosLocales, GlobalStrings.comprobanteIdUpdate);
                            ImpuestosLocales(sDeleteImpuestosLocales, sInsertImpuestosLocales);
                           
                        }
                        if (GlobalStrings.ERP != "XA")
                        {
                            //
                            //Validar existencia de la tabla del complemenento de vehiculos
                            var sValidateTableERP = string.Format(GlobalStrings.ValidateExistTable, "MXEICV");
                            var sValidateTableCS = string.Format(GlobalStrings.ValidateExistTable, "ZMX_MXEICV");
                            if (ValidateExistTableERP(sValidateTableERP) == "1" && ValidateExistTableCS(sValidateTableCS) == "1")
                            {
                                Console.WriteLine("Inicia extraccion complemento Vehiculo");
                                //Proceso para inserta el encabezado del complemento de Vehiculos
                                var sInsertDHCV = string.Format(GlobalStrings.InsertComplementVehiHD, GlobalStrings.comprobanteIdUpdate);
                                var sDeleteDHCV = string.Format(GlobalStrings.DeleteComplementVehiHD, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                                ComplementoVehiculoHD(sDeleteDHCV, sInsertDHCV);
                                Console.WriteLine("Complemento vehiculos hd");
                                //
                                //Proceso para insertar el detalle del complemento de Vehiculos   
                                var sInsertDTCV = string.Format(GlobalStrings.InsertComplementVehiDT, GlobalStrings.comprobanteIdUpdate);
                                var sDeleteDTCV = string.Format(GlobalStrings.DeleteComplementVehiDT, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                                ComplementoVehiculoDT(sDeleteDTCV, sInsertDTCV);
                                Console.WriteLine("Complemento vehiculos dt");
                                //
                                //Proceso para insertar la info del detalle de la aduana del complemento
                                var sInsertDTADCV = string.Format(GlobalStrings.InsertComplementVehiDTAD, GlobalStrings.comprobanteIdUpdate);
                                var sDeleteDTADCV = string.Format(GlobalStrings.DeleteComplementVehiDTAD, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                                ComplementoVehiculoDTAD(sDeleteDTADCV, sInsertDTADCV);
                                Console.WriteLine("Complemento vehiculos ad");
                                //
                                Console.WriteLine("Finaliza extraccion complemento Vehiculo");
                            }
                        }

                        //----------------------------------------------------
                        Console.WriteLine("Inicia registro  en voucherLog countrypack");
                        VoucherLog("CountryPack", 10, "Extracción finalizada exitosamente", 0, 10);
                        Console.WriteLine("Finaliza registro  en voucherLog countrypack");

                        //CXA
                        
                        if (GlobalStrings.ERP != "XA")
                        {
                            Console.WriteLine("Inicia registro en voucherLog erp");
                            VoucherLog("ERP", 10, "Extracción finalizada exitosamente", 0, 10);
                            Console.WriteLine("Finaliza registro en voucherLog erp");
                        }

                        if (GlobalStrings.intRollback == 1)
                        {
                            Console.WriteLine("Inicia proceso de rollback");
                            Rollback(listConceptos);
                            VoucherLog("CountryPack", 5, GlobalStrings.msgNotExistUM, 0, 10);
                            Console.WriteLine("Finaliza proceso de rollback");
                        }
                        else
                        {
                            Console.WriteLine("Inicia llamado a ZMX_VoucherGenSP");
                            sEXEC_SP = @"Exec ZMX_VoucherGenSP  '" + GlobalStrings.V0SERIE + "',  '" + GlobalStrings.V0FOLIO + "',null ";
                            EXEC_SP(sEXEC_SP);
                            Console.WriteLine("Finaliza llamado a ZMX_VoucherGenSP");
                        }
                    }
                }
                catch (Exception ex)
                {

                    Console.WriteLine("Error por Catch Exception");
                    Console.WriteLine("Inicia registro  en voucherLog countrypack");
                    VoucherLog("CountryPack", 5, ex.Message, 0, 10);
                    Console.WriteLine("Finaliza registro  en voucherLog countrypack");
                    Console.WriteLine("Inicia registro en voucherLog erp");
                    VoucherLog("ERP", 5, ex.Message, 0, 10);
                    Console.WriteLine("Finaliza registro en voucherLog erp");
                }
                Console.WriteLine("Finaliza proceso de actualizacion del comprobante");
            }
            if (status == "TIMBRADA")
            {
                Console.WriteLine("Inicia proceso de actualizacion por factura timbrada");
                GlobalStrings.comprobanteIdUpdate = GlobalStrings.comprobanteId;
                //Extraccion addendad
                sNodoAddendas = string.Format(GlobalStrings.InsertAddenda, GlobalStrings.comprobanteIdUpdate, GlobalStrings.SiteRef);
                NodoAddendas(sNodoAddendas);
                //
                //Extraccion comentarios
                Comentarios();
                //
                //Actualizar ufs
                UpdateVoucherUFS();
                //
                //Extraccion punto de venta
                //Agregado por jl para la reimpresion de direcciones

                if (GlobalStrings.ERP != "XA")
                {
                    sPos = string.Format(GlobalStrings.InsertPOS, GlobalStrings.comprobanteId);
                    POS(sPos); //utilizado para en XA para obbtener Shipto
                }
                else
                {
                    sPos = string.Format(GlobalStrings.InsertPOSXA, GlobalStrings.comprobanteId);
                    POS(sPos); //utilizado para en XA para obbtener Shipto
                }
               // sPos = string.Format(GlobalStrings.InsertPOS, GlobalStrings.comprobanteId);
                //POS(sPos); //utilizado para en XA para obbtener Shipto
                //               
               
                if (GlobalStrings.intRollback == 1)
                {
                    Console.WriteLine("Inicia proceso de rollback");
                    Rollback(listConceptos);
                    VoucherLog("CountryPack", 5, GlobalStrings.msgNotExistUM, 0, 10);
                    Console.WriteLine("Finaliza proceso de rollback");
                }
                else
                {
                    Console.WriteLine("Inicia llamado a ZMX_VoucherGen_Reprint_SP");
                    sEXEC_SP = @"Exec ZMX_VoucherGen_Reprint_SP  '" + GlobalStrings.V0SERIE + "',  '" + GlobalStrings.V0FOLIO + "',null ";
                    EXEC_SP(sEXEC_SP);
                    Console.WriteLine("Finaliza llamado a ZMX_VoucherGen_Reprint_SP");
                }
            }
            Console.WriteLine("Finaliza proceso de actualizacion por factura timbrada");
        }
        void PaymentsInvoice()//(string cono, string serie, string folio)
        {
            Console.WriteLine("Inicia proceso de extraccion complemento de pagos" );
            //Extraccion de sitio
            Console.WriteLine("Inicia recuperando del sitio");

            GetSite();
            Console.WriteLine("Finaliza recuperando del sitio");
            //
            Console.WriteLine("Inicia proceso de validaciones");
            PaymentValidation();
            Console.WriteLine("Finaliza proceso de validaciones");

            if (msg.bShowMsg != true)
            {
                //Agregado por JL para la insercion en hd cuando no haya registros en rq para xa
                string InputData = "";
                string OutputData = "";
                if (GlobalStrings.ERP != "XA")
                {

                    //Agregado por JL -28102019 para eliminar px y py
                    if (GlobalStrings.UseMultisite)
                    {
                        var sValidateTableERPImpueLoc = string.Format(GlobalStrings.ValidateExistTable, "ZMX_TaxConcept_Multi_All");
                        if (ValidateExistTableERP(sValidateTableERPImpueLoc) == "1")
                        {
                            var eliminarTaxMulti = string.Format(GlobalStrings.DELETETAXSMULTI, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                            Console.WriteLine("Inicia proceso de eliminacion de los impuestos");
                            LimpiaImpuestosMulti(eliminarTaxMulti);
                            Console.WriteLine("Finaliza proceso de eliminacion de los impuestos");
                        }
                    }
                    var eliminapy = string.Format(GlobalStrings.DELETEPY, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                    Console.WriteLine("Inicia proceso de eliminacion del encabezado del pago");
                    LimpiaPXPY(eliminapy);
                    Console.WriteLine("Finaliza proceso de eliminacion del encabezado del pago");
                    var eliminapX = string.Format(GlobalStrings.DELETEPX, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                    Console.WriteLine("Inicia proceso de eliminacion del detalle del pago");
                    LimpiaPXPY(eliminapX);
                    Console.WriteLine("Finaliza proceso de eliminacion del detalle del pago");
                    //
                    Console.WriteLine("Inicia proceso de creacion del comprobante");
                    if (GlobalStrings.UseMultisite)
                    {
                        var InputDataColSL = string.Format(GlobalStrings.ValidarExistenciaColumna, "V0MLSITE", "MXEIHD");
                        ExNihiloValidarExistenciaColumnaSL ExitsColumnSL = new ExNihiloValidarExistenciaColumnaSL(InputDataColSL);
                        ExitsColumnSL.UseTransaction = false;
                        ExitsColumnSL.Execute();
                        if (ExitsColumnSL.GetAllErrors().Count() > 0)
                        {
                            foreach (Exception ex in ExitsColumnSL.GetAllErrors())
                            {
                                Console.WriteLine(ex.Message);
                                VoucherLog("CountryPack", 5, ex.Message, 0, 10);
                                //Rollback();
                            }
                        }
                        if (GlobalStrings.ValidateCol == "1")
                        {

                            GlobalStrings.ValidateCol = "0";
                            GlobalStrings.ParamsPaymentSelect += ",V0MLSITE AS MLSITE,";
                        }
                    }


                    GlobalStrings.ParamsPaymentSelect = !string.IsNullOrEmpty(GlobalStrings.ParamsPaymentSelect) ? GlobalStrings.ParamsPaymentSelect.Remove(GlobalStrings.ParamsPaymentSelect.Length - 1, 1) : GlobalStrings.ParamsPaymentSelect;
                    InputData = string.Format(GlobalStrings.SelectPaymentsInvoice, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO, GlobalStrings.ParamsPaymentSelect);

                   

                    string InputDataCol = string.Format(GlobalStrings.ValidarExistenciaColumna, "VQUUT1", "ZMX_MXEIPY");
                    ExNihiloValidarExistenciaColumna ExitsColumn = new ExNihiloValidarExistenciaColumna(InputDataCol);
                    ExitsColumn.UseTransaction = false;
                    ExitsColumn.Execute();
                    if (ExitsColumn.GetAllErrors().Count() > 0)
                    {
                        foreach (Exception ex in ExitsColumn.GetAllErrors())
                        {
                            Console.WriteLine(ex.Message);
                            VoucherLog("CountryPack", 5, ex.Message, 0, 10);
                            //Rollback();
                        }
                    }

                    //Agregado por JL para la validacion de la existencia de las columnas
                    if (GlobalStrings.UseMultisite)
                    {
                        if (GlobalStrings.ValidateCol == "1")
                        {
                            GlobalStrings.ValidateCol = "0";
                            InputDataCol = string.Format(GlobalStrings.ValidarExistenciaColumna, "MLSITE", "ZMX_Voucher");
                            ExitsColumn = new ExNihiloValidarExistenciaColumna(InputDataCol);
                            ExitsColumn.UseTransaction = false;
                            ExitsColumn.Execute();
                            if (ExitsColumn.GetAllErrors().Count() > 0)
                            {
                                foreach (Exception ex in ExitsColumn.GetAllErrors())
                                {
                                    Console.WriteLine(ex.Message);
                                    VoucherLog("CountryPack", 5, ex.Message, 0, 10);
                                    //Rollback();
                                }
                            }
                            if (GlobalStrings.ValidateCol == "1")
                            {
                                GlobalStrings.ValidateCol = "0";
                                //GlobalStrings.ParamsPaymentInsert += ",@MLSITE,";
                                GlobalStrings.ParamsPaymentInsert += ",'1',";
                                GlobalStrings.ValuesPaymentInsert += ",MLSITE,";
                                GlobalStrings.ParamsPaymentUpdate += ",MLSITE='1',";
                                //GlobalStrings.ParamsPaymentUpdate += ",MLSITE=@MLSITE,";
                            }

                            GlobalStrings.ValuesPaymentInsert = !string.IsNullOrEmpty(GlobalStrings.ValuesPaymentInsert) ? GlobalStrings.ValuesPaymentInsert.Remove(GlobalStrings.ValuesPaymentInsert.Length - 1, 1) : GlobalStrings.ValuesPaymentInsert;
                            GlobalStrings.ParamsPaymentInsert = !string.IsNullOrEmpty(GlobalStrings.ParamsPaymentInsert) ? GlobalStrings.ParamsPaymentInsert.Remove(GlobalStrings.ParamsPaymentInsert.Length - 1, 1) : GlobalStrings.ParamsPaymentInsert;
                            GlobalStrings.ParamsPaymentUpdate = !string.IsNullOrEmpty(GlobalStrings.ParamsPaymentUpdate) ? GlobalStrings.ParamsPaymentUpdate.Remove(GlobalStrings.ParamsPaymentUpdate.Length - 1, 1) : GlobalStrings.ParamsPaymentUpdate;
                            OutputData = string.Format(GlobalStrings.InsertPaymentsInvoiceWhitCol, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO, GlobalStrings.pathCFDI, GlobalStrings.Parm_Site, GlobalStrings.ValuesPaymentInsert, GlobalStrings.ParamsPaymentInsert, GlobalStrings.ParamsPaymentUpdate);
                           
                        }
                        else
                        {
                            OutputData = string.Format(GlobalStrings.InsertPaymentsInvoice, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO, GlobalStrings.pathCFDI, GlobalStrings.Parm_Site);                            
                        }
                    }
                    else
                        {
                            OutputData = string.Format(GlobalStrings.InsertPaymentsInvoice, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO, GlobalStrings.pathCFDI, GlobalStrings.Parm_Site);                         
                        }
                    }
                else
                {
                    // Console.WriteLine("SelectPaymentsInvoice");
                    InputData = string.Format(GlobalStringsXA.SelectPaymentsInvoice, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                    // Console.WriteLine(InputData);
                    OutputData = string.Format(GlobalStrings.InsertPaymentsInvoice, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO, GlobalStrings.pathCFDI, GlobalStrings.Parm_Site);
                   
                }
                //

                /*string InputData = string.Format(GlobalStrings.SelectPaymentsInvoice, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                string OutputData = string.Format(GlobalStrings.InsertPaymentsInvoice, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO, GlobalStrings.pathCFDI,GlobalStrings.Parm_Site);*/
              //  Console.WriteLine("Multisite select");
                //Console.WriteLine(InputData);
                //Console.WriteLine("Multisite Insert");
                //Console.WriteLine(OutputData);
                ExNihiloGenericExtraction PaymentsInvoice = new ExNihiloGenericExtraction(InputData, OutputData);
                PaymentsInvoice.UseTransaction = false;
                PaymentsInvoice.Execute();
                if (PaymentsInvoice.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in PaymentsInvoice.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                        VoucherLog("CountryPack", 5, ex.Message, 0, 10);
                        VoucherLog("ERP", 5, ex.Message, 0, 10);
                        //Rollback();
                    }
                }
                Console.WriteLine("Finaliza proceso de creacion del comprobante");

                Console.WriteLine("Inicia proceso de verificacion de estatus del voucher");
                VerificaStatusVoucher();//Obtiene el comprobanteId para actulizar el Log
                Console.WriteLine("Finaliza proceso de verificacion de estatus del voucher");

                Console.WriteLine("Inicia registro  en voucherLog countrypack");
                VoucherLog("CountryPack", 10, "Extracción finalizada exitosamente", 0, 10);
                Console.WriteLine("Finaliza registro  en voucherLog countrypack");

                if (GlobalStrings.ERP != "XA")
                {
                    Console.WriteLine("Inicia registro en voucherLog erp");
                    VoucherLog("ERP", 10, "Extracción finalizada exitosamente", 0, 10);
                    Console.WriteLine("Finaliza registro en voucherLog erp");
                }
                else
                {
                    Console.WriteLine("Inicia registro en voucherLog erp");
                    VoucherLog("ERP", 20, "Extracción finalizada exitosamente", 0, 10);
                    Console.WriteLine("Finaliza registro en voucherLog erp");
                }
                //Bug de XA no regresar estados 10 para no recibir registros duplicados.
              
                if (GlobalStrings.ERP != "XA")
                {
                    
                    string sCfdiRelacionados = string.Format(GlobalStrings.InsertCfdiRelacionadosPagos, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                    CfdiRelacionados(sCfdiRelacionados);
                  
                    //Enviar Tabla de impuestos Multisite
                    //if (GlobalStrings.UseMultisite)
                    //{
                        Console.WriteLine("Inicia llamado a Impuestos MultiSite");
                        //Validar existencia de tabla de lado SL
                        var sValidateTableERPImpueLoc = string.Format(GlobalStrings.ValidateExistTable, "ZMX_TaxConcept_Multi_All");
                        if (ValidateExistTableERP(sValidateTableERPImpueLoc) == "1")
                        {
                        Console.WriteLine("iNSERT");
                            //Si existe la tabla crear proceso para la extraccion
                            var InputInvoiceTaxes = string.Format(GlobalStrings.SelectTaxesMulti, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                            var OutputInvoiceTaxes = string.Format(GlobalStrings.InsertTaxesMulti, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
                            ExNihiloGenericExtraction PaymentsInvoiceTaxes = new ExNihiloGenericExtraction(InputInvoiceTaxes, OutputInvoiceTaxes);
                       
                        PaymentsInvoiceTaxes.UseTransaction = false;
                            PaymentsInvoiceTaxes.Execute();
                            if (PaymentsInvoiceTaxes.GetAllErrors().Count() > 0)
                            {
                                foreach (Exception ex in PaymentsInvoiceTaxes.GetAllErrors())
                                {
                                    Console.WriteLine(ex.Message);
                                    VoucherLog("CountryPack", 5, ex.Message, 0, 10);
                                    VoucherLog("ERP", 5, ex.Message, 0, 10);
                                    //Rollback();
                                }
                            }
                        }
                        Console.WriteLine("Finaliza llamado a Impuestos MultiSite");
                    //}
                }
                
                Console.WriteLine("Inicia llamado a ZMX_VoucherGenSP");
                string sEXEC_SP = @"Exec ZMX_VoucherGenSP  '" + GlobalStrings.V0SERIE + "',  '" + GlobalStrings.V0FOLIO + "',null ";
                EXEC_SP(sEXEC_SP);
                Console.WriteLine("Finaliza llamado a ZMX_VoucherGenSP");

                Console.WriteLine("Finaliza proceso de extraccion complemento de pagos");
            }
            
        }
        public void ValidateInvoiceType(string cono, string serie, string folio)
        {
            Console.WriteLine("Inicia proceso de validacion de tipo documento");
            GlobalStrings.V0CONO = cono;
            GlobalStrings.V0SERIE = serie;
            GlobalStrings.V0FOLIO = folio;
            string invoiceType = String.Empty;
            GlobalStrings.pathCFDI = getpathCFDI(serie);//Para redireccionar las Facturas dependiendo de la Serie
            string InputData = string.Format(GlobalStrings.SelectInvoiceType, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
         
            //Llamar al setsite
            if (GlobalStrings.ERP != "XA")
            {
                if (GlobalStrings.UseMultisite)
                {
                    var setstring = string.Format(GlobalStrings.SetSite, GlobalStrings.V0CONO);
                    ExNihiloSetSite SetSite = new ExNihiloSetSite(setstring);
                    SetSite.UseTransaction = false;
                    SetSite.Execute();
                    if (SetSite.GetAllErrors().Count() > 0)
                    {

                        foreach (Exception ex in SetSite.GetAllErrors())
                        {
                            Console.WriteLine(ex.Message);
                        }
                    }
                }
            }         
            ExNihiloInvoiceType InvoiceType = new ExNihiloInvoiceType(InputData);
            InvoiceType.UseTransaction = false;
            InvoiceType.Execute();
            if (InvoiceType.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in InvoiceType.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                }
            }


            if (InvoiceType.ObjgetType.getInvoiceType() != null)
            {               
                invoiceType = InvoiceType.ObjgetType.getInvoiceType();
                if (invoiceType == "P")
                    PaymentsInvoice();
                else
                    InvoiceExtraction();
            }
            Console.WriteLine("Finaliza proceso de validacion de tipo documento");
        }
        public bool TestConnection(string BDTest, string ERP)
        {
            bool exito = true;
            string InputData;
            if (ERP == "XA" && BDTest != "CountryPack")
                InputData = GlobalStringsXA.TestConnection;
            else
                InputData = GlobalStrings.TestConnection;
            try
            {
                ExNihiloTestConnection cn = new ExNihiloTestConnection(InputData, BDTest);
                cn.UseTransaction = false;
                cn.Execute();
                if (cn.GetAllErrors().Count() > 0)
                {
                    exito = false;
                    foreach (Exception ex in cn.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
            }
            catch (Exception e)
            {
                exito = false;
            }           
            return exito;
        }
        void SetStringsXA()
        {
            //Guid newid = Guid.NewGuid();
            //if (ERP == "XA")
            //{
            //GlobalStrings.TestConnection = GlobalStringsXA.TestConnection;

            //GlobalStrings.SelectrfcEmisorERP = GlobalStringsXA.SelectrfcEmisorERP.Replace("NEWID()", newid.ToString());
            GlobalStrings.SelectrfcEmisorERP = GlobalStringsXA.SelectrfcEmisorERP;
            GlobalStrings.SelectEmisorErp = GlobalStringsXA.SelectEmisorErp;

            if (!GlobalStrings.UseShiptoXA)
            {
                GlobalStrings.SelectReceptorAddressErp = GlobalStringsXA.SelectReceptorAddressErp;
                GlobalStrings.SelectReceptorShiptopErp = GlobalStringsXA.SelectReceptorShiptopErp;
            }
            else
            {
                GlobalStrings.SelectReceptorAddressErp = GlobalStringsXA.SelectReceptorAddressErpXA;
                GlobalStrings.SelectReceptorShiptopErp = GlobalStringsXA.SelectReceptorShiptoXAErp;
            }
           

            //Agregado por JL-29102019 para agregar el subtotal de xa
            if (GlobalStrings.GetSubTotalXA)
                GlobalStrings.SelectComprobante = GlobalStringsXA.SelectComprobante;
            else
                GlobalStrings.SelectComprobante = GlobalStringsXA.SelectComprobante0;

            GlobalStrings.SelectExistsPedimentos = GlobalStringsXA.SelectExistsPedimentos;
            GlobalStrings.SelectPedimentos = GlobalStringsXA.SelectPedimentos;
            GlobalStrings.SelectImpuestosERP = GlobalStringsXA.SelectImpuestosERP;
            GlobalStrings.SelectExistsAddenda = GlobalStringsXA.SelectExistsAddenda;
            GlobalStrings.SelectAddenda = GlobalStringsXA.SelectAddenda;
            GlobalStrings.InicializaRegistroERP = GlobalStringsXA.InicializaRegistroERP;
            GlobalStrings.ValidaVoucherMXEIRQ = GlobalStringsXA.ValidaVoucherMXEIRQ.Replace("TIMESTAMP", GlobalStrings.V9DTTM);
            GlobalStrings.UpdateVoucherMXEIRQUUID = GlobalStringsXA.UpdateVoucherMXEIRQUUID.Replace("TIMESTAMP", GlobalStrings.V9DTTM);
            GlobalStrings.SelectInvoiceType = GlobalStringsXA.SelectInvoiceType;
            GlobalStrings.SelectPaymentsInvoice = GlobalStringsXA.SelectPaymentsInvoice;
            GlobalStrings.SelectComments = GlobalStringsXA.SelectComments;
            GlobalStrings.SelectCfdiRelacionados = GlobalStringsXA.SelectCfdiRelacionados;
            ValStrings.getRFCs = GlobalStringsXA.getRFCs;
            //Para manejo de IndustualNumber en XA//
            if (GlobalStrings.industrialNumber == 0)
            {
                ValStrings.getItems = GlobalStringsXA.getItems;
                GlobalStrings.SelectNodoConcepto = GlobalStringsXA.SelectNodoConcepto;
                GlobalStrings.SelectNodoConceptoUpdate = GlobalStringsXA.SelectNodoConceptoUpdate;
            }
            else
            {
                ValStrings.getItems = GlobalStringsXA.getItems_IndustrialNumber;
                ValStrings.getItemsUpdate = GlobalStringsXA.getItemsUpdate_IndustrialNumber;
                GlobalStrings.SelectNodoConcepto = GlobalStringsXA.SelectConcepto_IndustrialNumber;
                GlobalStrings.SelectNodoConceptoUpdate = GlobalStringsXA.SelectConceptoUpdate_IndustrialNumber;
                GlobalStrings.InsertNodoConcepto = GlobalStringsXA.InsertNodoConcepto;
                GlobalStrings.UpdateNodoConcepto = GlobalStringsXA.UpdateNodoConcepto;
            }
            ValStrings.getTaxes = GlobalStringsXA.getTaxes;
            GlobalStrings.InsertVoucherMXEIRQ = GlobalStringsXA.InsertVoucherMXEIRQ;
            GlobalStrings.UpdateVoucherMXEIRQ = GlobalStringsXA.UpdateVoucherMXEIRQ.Replace("TIMESTAMP", GlobalStrings.V9DTTM );
            GlobalStrings.SelectVerificaPOS = GlobalStringsXA.SelectVerificaPOS;
            GlobalStrings.SelectPOS = GlobalStringsXA.SelectPOS;
            GlobalStrings.InsertPOS = GlobalStringsXA.InsertPOS;
            GlobalStrings.SelectExistsDetallista = GlobalStringsXA.SelectExistsDetallista;
            GlobalStrings.SelectMXADHD = GlobalStringsXA.SelectMXADHD;
            GlobalStrings.SelectMXADSI = GlobalStringsXA.SelectMXADSI;
            GlobalStrings.SelectMXADCU = GlobalStringsXA.SelectMXADCU;
            GlobalStrings.SelectMXADPD = GlobalStringsXA.SelectMXADPD;
            GlobalStrings.SelectMXADCG = GlobalStringsXA.SelectMXADCG;
            GlobalStrings.SelectMXADDT = GlobalStringsXA.SelectMXADDT;
            GlobalStrings.SelectMXADRF = GlobalStringsXA.SelectMXADRF;
            GlobalStrings.SelectMXADAD = GlobalStringsXA.SelectMXADAD;
            GlobalStrings.SelectMXADII = GlobalStringsXA.SelectMXADII;
            GlobalStrings.SelectMXADIQ = GlobalStringsXA.SelectMXADIQ;
            GlobalStrings.SelectMXADEA = GlobalStringsXA.SelectMXADEA;
            GlobalStrings.SelectMXADTX = GlobalStringsXA.SelectMXADTX;
            GlobalStrings.SelectMXADXC = GlobalStringsXA.SelectMXADXC;
            GlobalStrings.SelectExistsImpuestosLocales = GlobalStringsXA.SelectExistsImpuestosLocales;
            GlobalStrings.SelectImpuestosLocales = GlobalStringsXA.SelectImpuestosLocales;
            //}

        }
        string GetSchema()
        {
            string schema = String.Empty;
            string InputData = string.Format(GlobalStringsXA.SelectGetSchema, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
            ExNihiloGetSchema getSchema = new ExNihiloGetSchema(InputData);
          
            getSchema.UseTransaction = false;
            getSchema.Execute();
            if (getSchema.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in getSchema.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                    return ConnectionData.SchemaDefault;
                }
            }

            //Se obtiene el TimeStamp de la factura
            if (getSchema != null)
            {
                if (getSchema.ObjgetSchema != null)
                {
                    if (getSchema.ObjgetSchema.getTimeStamp() != null)
                        GlobalStrings.V9DTTM = getSchema.ObjgetSchema.getTimeStamp();
                    if (getSchema.ObjgetSchema.getSchemaXA() != null)
                        schema = getSchema.ObjgetSchema.getSchemaXA();
                    else
                        schema = ConnectionData.SchemaDefault;
                }
                else
                    schema = ConnectionData.SchemaDefault;
            }
            else
                schema = ConnectionData.SchemaDefault;          

            Console.WriteLine(schema);
            return schema;
        }
        public void PreviousXA(string cono, string serie, string folio)
        {
            string schema= String.Empty;
            GlobalStrings.V0CONO = cono;
            GlobalStrings.V0SERIE = serie;
            GlobalStrings.V0FOLIO = folio;
            Configuration customConfig = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);
            //Consigo el schema
            schema = GetSchema();
            if (schema.Equals(null)) //Se agrega por error de actualizacion en MXEIRQ
            {
                Console.WriteLine("Schema NULL");
                schema = ConnectionData.SchemaDefault;
            }
            else
                Console.WriteLine("Schema recuperado");

            Console.WriteLine("Schema");
            Console.WriteLine(schema);
            UpdateConexionERP(customConfig,schema);
            ConfigurationManager.RefreshSection("connectionStrings");
         
            SetStringsXA(); 
        }
        public string getpathCFDI(string serie)
        {
            Console.WriteLine("Inicia recuperacion de configuraciones del app.config");
            // new xdoc instance 
            string query = String.Empty;
            XmlDocument xml = new XmlDocument();
            string path = Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
            //Console.WriteLine(path + "\\path_config.xml");
            if (File.Exists(path + "\\path_config.xml"))
            {
                xml.Load(path + "\\path_config.xml");
                XmlNodeList nodelist = xml.SelectNodes("/config[@active='true']/path[@serie='" + serie + "']");


                foreach (XmlNode node in nodelist)
                {
                    // first node is the url ... have to go to nexted loc node 
                    foreach (XmlNode locNode in node)
                    {
                        Console.WriteLine(locNode.InnerText);
                        query = locNode.InnerText;
                    }

                }
                //Agregado por jl para la configuracion de si guarda en hd cuando no hay en rq, JL -29102019 - cambio para leer si el subtotal se toma en xa o se guarda com 0
                XElement xelement = XElement.Load(path + "\\path_config.xml");

                var AttributeFound = xelement.Attributes().Select(x => x).ToList();
                if (AttributeFound.Where(o => o.Name == "directhd").ToList().Count > 0)
                    GlobalStrings.Config = xelement.Attribute("directhd").Value;
                else
                    GlobalStrings.Config = "0";

                Console.WriteLine("recuperando config hd");


                if (AttributeFound.Where(o => o.Name == "getSubtotal").ToList().Count > 0)
                    GlobalStrings.GetSubTotalXA = Boolean.Parse(xelement.Attribute("getSubtotal").Value);
                else
                    GlobalStrings.GetSubTotalXA = false;

                Console.WriteLine("recuperando config subtotal");


                if (AttributeFound.Where(o => o.Name == "UseConfigDecimals").ToList().Count > 0)
                    GlobalStrings.UseConfigDecimals = Boolean.Parse(xelement.Attribute("UseConfigDecimals").Value);
                else
                    GlobalStrings.UseConfigDecimals = false;
                Console.WriteLine("recuperando config UseConfigDecimals");
                //Configuracion multisite

                if (AttributeFound.Where(o => o.Name == "UseMultisite").ToList().Count > 0)
                    GlobalStrings.UseMultisite = Boolean.Parse(xelement.Attribute("UseMultisite").Value);
                else
                    GlobalStrings.UseMultisite = false;

                Console.WriteLine("recuperando config UseMultisite");

                if (AttributeFound.Where(o => o.Name == "UseShiptoXA").ToList().Count > 0)
                    GlobalStrings.UseShiptoXA = Boolean.Parse(xelement.Attribute("UseShiptoXA").Value);
                else
                    GlobalStrings.UseShiptoXA = false;

                Console.WriteLine("recuperando use shipto XA");

                if (AttributeFound.Where(o => o.Name == "SyncTableTaxSL").ToList().Count > 0)
                    GlobalStrings.SyncTableTaxSL = Boolean.Parse(xelement.Attribute("SyncTableTaxSL").Value);
                else
                    GlobalStrings.SyncTableTaxSL = false;
                Console.WriteLine("recuperando use SyncTableTaxSL");
              
               
                if (AttributeFound.Where(o => o.Name == "DocumentCFDIRelated").ToList().Count > 0)
                    GlobalStrings.DocumentCFDIRelated = Boolean.Parse(xelement.Attribute("DocumentCFDIRelated").Value);
                else
                    GlobalStrings.DocumentCFDIRelated = false;
                Console.WriteLine("recuperando DocumentCFDIRelated");
              

              

                if (AttributeFound.Where(o => o.Name == "UpdateAditionalDataHD").ToList().Count > 0)
                    GlobalStrings.UpdateAditionalDataHD = Boolean.Parse(xelement.Attribute("UpdateAditionalDataHD").Value);
                else
                    GlobalStrings.UpdateAditionalDataHD = false;

                Console.WriteLine("recuperando UpdateAditionalDataHD");

                if (AttributeFound.Where(o => o.Name == "RecalculateImports").ToList().Count > 0)
                    GlobalStrings.RecalculateImports = Boolean.Parse(xelement.Attribute("RecalculateImports").Value);
                else
                    GlobalStrings.RecalculateImports = true;
                Console.WriteLine("recuperando RecalculateImports");

                if (AttributeFound.Where(o => o.Name == "ExtraerKitsXA").ToList().Count > 0)
                    GlobalStrings.ExtraerKitsXA = Boolean.Parse(xelement.Attribute("ExtraerKitsXA").Value);
                else
                    GlobalStrings.ExtraerKitsXA = false;
                Console.WriteLine("recuperando ExtraerKitsXA");

            

            }
           else
            {
                Console.WriteLine("Inicia proceso de recuperacion configuraciones default");
                GlobalStrings.Config = "0";
                GlobalStrings.GetSubTotalXA = false;
                GlobalStrings.UseConfigDecimals = false;
                GlobalStrings.UseMultisite = false;
                GlobalStrings.UseShiptoXA = false;
                GlobalStrings.SyncTableTaxSL = false;
                GlobalStrings.DocumentCFDIRelated = false;
                GlobalStrings.UpdateAditionalDataHD = false;
                GlobalStrings.ExtraerKitsXA = false;
                Console.WriteLine("Fin proceso recuperacion configuraciones default");
            }




            Console.WriteLine("Finaliza recuperacion de configuraciones del app.config");

            //

            
            return query;
        }

        //Metodos    

        //Agregado por JL para la validacion del cliente en complementos de pagos
        public static List<Rfc> validarRfcPayment()
        {
            List<Rfc> listRfcs = new List<Rfc>();
            string InputData = string.Format(ValStrings.getRFCPayment, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
            ExNihiloValidaRFC ValidaRFC = new ExNihiloValidaRFC(InputData, listRfcs);
            ValidaRFC.UseTransaction = false;
            ValidaRFC.Execute();
            if (ValidaRFC.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in ValidaRFC.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                }
            }
            listRfcs = ValidaRFC.Getlista();
            return listRfcs;
        }


        public static List<Rfc> validarRfcs()
        {
            List<Rfc> listRfcs = new List<Rfc>();
            string InputData = string.Format(ValStrings.getRFCs, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
            ExNihiloValidaRFC ValidaRFC = new ExNihiloValidaRFC(InputData, listRfcs);
            ValidaRFC.UseTransaction = false;
            ValidaRFC.Execute();
            if (ValidaRFC.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in ValidaRFC.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                }
            }
            listRfcs = ValidaRFC.Getlista();
            return listRfcs;
        }

        //Agregado por JL para la validacion de unidades
        public static List<um> validarum;

        public static List<Items> validarItems()
        {
            string InputData = string.Format(ValStrings.getItems, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
            //string OutputData = string.Format(GlobalStrings.InsertNodoConcepto, GlobalStrings.comprobanteId, GlobalStrings.SiteRef);
            List<Items> listItems = new List<Items>();
            //Agregado por JL para los conceptos faltantes
            List<Items> listNewItems = new List<Items>();
            //
            ExNihiloValidaItems ValidaItems = new ExNihiloValidaItems(InputData, listItems, listNewItems);
            ValidaItems.UseTransaction = false;
            ValidaItems.Execute();
            if (ValidaItems.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in ValidaItems.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                }
            }           

            listItems = ValidaItems.Getlista();
            listNewItems = ValidaItems.GetlistaNewConceptos();

            //Agregado por JL para la validacion de unidades     
            Console.WriteLine("Inicia validacion de um");
            var exist = "";
            validarum = new List<um>();
            var unidades = listItems.Select(p => p.unidad).Distinct();
            foreach (var um in unidades)
            {
                ExNihiloExistum Existum = new ExNihiloExistum(um);
                Existum.UseTransaction = false;
                Existum.Execute();
                if (Existum.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in Existum.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                    }
                }

                if (Existum.Objgetum.getIfExistum() != null)
                {
                    exist = Existum.Objgetum.getIfExistum();
                    if (exist == "0")
                        msg.bShowMsg = true;
                }
                else
                    msg.bShowMsg = true;

                validarum.Add(new um() { unidad = um, existe = exist });
            }
            Console.WriteLine("Finaliza validacion de um");
          
           //
            return listItems;
        }
        public static List<Taxes> validarTaxes()
        {
            string InputData = string.Format(ValStrings.getTaxes, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);
            List<Taxes> listTaxes = new List<Taxes>();
            ExNihiloValidaTaxes ValidaTaxes = new ExNihiloValidaTaxes(InputData, listTaxes);
            ValidaTaxes.UseTransaction = false;
            ValidaTaxes.Execute();
            if (ValidaTaxes.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in ValidaTaxes.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                }
            }
            listTaxes = ValidaTaxes.Getlista();
            return listTaxes;
        }

        //Agregado por JL para la validacion de los datos del pago
        void PaymentValidation()
        {
            string logMessage = String.Empty;
            List<Rfc> listRfcs = new List<Rfc>();
            Console.WriteLine("Inicia validacion RFC");
            listRfcs = validarRfcs();
            Console.WriteLine("Finaliza validacion RFC:" + msg.bShowMsg);        

         
            if (msg.bShowMsg == true)
            {
                foreach (Rfc obj in listRfcs)
                {
                    if (obj.existe == "0" & obj.tipoRfc == "Company")
                        logMessage = logMessage + string.Format(msg.msgcompany, obj.rfc);

                    if (obj.existe == "0" & obj.tipoRfc == "Customer")
                        logMessage = logMessage + string.Format(msg.msgcustomer, obj.rfc, obj.cliente);

                }             
              
                Console.WriteLine("Inicia registro de validaciones en voucherlog contrypack");
                VoucherLog("CountryPack", 5, logMessage, 0, 10);
                Console.WriteLine("Finaliza registro de validaciones en voucherlog contrypack");

                Console.WriteLine("Inicia registro de validaciones en voucherlog ERP");
                VoucherLog("ERP", 5, logMessage, 0, 10);
                Console.WriteLine("Finaliza registro de validaciones en voucherlog ERP");
            }

        }
        void validaciones()
        {
            string logMessage = String.Empty;
            List<Rfc> listRfcs = new List<Rfc>();
            Console.WriteLine("Inicia validacion RFC");
            listRfcs = validarRfcs();
            Console.WriteLine("Finaliza validacion RFC:" + msg.bShowMsg);

            Console.WriteLine("Inicia validacion de items");          
            List<Items> listItems = new List<Items>();
            listItems = validarItems();
            Console.WriteLine("Finaliza validacion de items:" + msg.bShowMsg);

            Console.WriteLine("Inicia validacion de impuestos");
            List<Taxes> listTaxes = new List<Taxes>();
            listTaxes = validarTaxes();
            Console.WriteLine("Finaliza validacion de impuestos:" + msg.bShowMsg);

            if (msg.bShowMsg == true)
            {
                foreach (Rfc obj in listRfcs)
                {
                    if (obj.existe == "0" & obj.tipoRfc == "Company")
                        logMessage = logMessage + string.Format(msg.msgcompany,obj.rfc);
                   
                    if (obj.existe == "0" & obj.tipoRfc == "Customer")
                        logMessage = logMessage + string.Format(msg.msgcustomer,obj.rfc, obj.cliente);
                   
                }
                foreach (Items obj in listItems)
                {
                   if (obj.existe == "0")
                        logMessage = logMessage + string.Format(msg.msgitems, obj.item);
                   
                }
                foreach (Taxes obj in listTaxes)
                {
                    if (obj.existe == "0")
                        logMessage = logMessage + string.Format(msg.msgtaxes, obj.impuesto);
                   
                }
                foreach (um obj in validarum)
                {
                    if (obj.existe == "0")
                        logMessage = logMessage + string.Format(msg.msgunidades, obj.unidad);
                   
                }
                Console.WriteLine("Inicia registro de validaciones en voucherlog contrypack");
                VoucherLog("CountryPack",5 ,logMessage,0,10);
                Console.WriteLine("Finaliza registro de validaciones en voucherlog contrypack");

                Console.WriteLine("Inicia registro de validaciones en voucherlog ERP");
                VoucherLog("ERP",5,logMessage,0,10);
                Console.WriteLine("Finaliza registro de validaciones en voucherlog ERP");
            }

        }
        //Para Update de conceptos
        public static List<Conceptos> validarItems(string tipoValidacion,out List<Items> listaNewItems)
        {            
            string InputData = string.Format(ValStrings.getItemsUpdate, GlobalStrings.V0CONO, GlobalStrings.V0SERIE, GlobalStrings.V0FOLIO);           
            List<Conceptos> listItems = new List<Conceptos>();

            //Agregado por JL para los conceptos faltantes
            List<Items> listNewItems = new List<Items>();
            //
            
            ExNihiloValidaItems ValidaItems = new ExNihiloValidaItems(InputData, listItems, listNewItems, tipoValidacion);
            ValidaItems.UseTransaction = false;
            ValidaItems.Execute();
            if (ValidaItems.GetAllErrors().Count() > 0)
            {
                foreach (Exception ex in ValidaItems.GetAllErrors())
                {
                    Console.WriteLine(ex.Message);
                }
            }
            listItems = ValidaItems.GetlistaConceptos();
            listaNewItems = ValidaItems.GetlistaNewConceptos();//ValidaItems.GetlistaNewConceptos();
            //Agregado por JL para la validacion de unidades 
                var exist = "";
                validarum = new List<um>();
                var unidades = listItems.Select(p => p.unidad).Distinct();
                foreach (var um in unidades)
                {
                    ExNihiloExistum Existum = new ExNihiloExistum(um);
                    Existum.UseTransaction = false;
                    Existum.Execute();
                    if (Existum.GetAllErrors().Count() > 0)
                    {
                        foreach (Exception ex in Existum.GetAllErrors())
                        {
                            Console.WriteLine(ex.Message);
                        }
                    }

                    validarum.Add(new um() { unidad = um, existe = exist });
                }
          
          
            ///
            return listItems;
        }

    }
    
    //Operations
    public class ValidaRFC : AbstractOperation
    {
        private List<Rfc> list;
        private string rfcCompania;
        private string rfcCliente;
        private string clienteId;
        private string exist;
        public ValidaRFC(List<Rfc> ListRfc)
        {
            this.list = ListRfc;
        }
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {

            foreach (Row row in rows)
            {              
             
                clienteId = (string)row["clienteId"];
                ExNihiloExistRFCPayment ExistRFCCliente = new ExNihiloExistRFCPayment(clienteId);
                ExistRFCCliente.UseTransaction = false;
                ExistRFCCliente.Execute();
                if (ExistRFCCliente.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in ExistRFCCliente.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
                if (ExistRFCCliente.ObjgetRFC.getIfExistRFC() != null)
                {                  
                    exist = ExistRFCCliente.ObjgetRFC.getIfExistRFC();
                    if (exist == "0")
                        msg.bShowMsg = true;                    
                }
                else
                    msg.bShowMsg = true;

                this.list.Add(new Rfc() { tipoRfc = "Customer", rfc = "", existe = exist,cliente = clienteId });
                yield return row;
            }
        }
    }
    public class ExisteRFC : AbstractOperation
    {
        //private string tipoRFC;
        public string ExistRFC;
        //public ExisteRFC(string tipoRfc)
        //{
        //    this.tipoRFC = tipoRfc;
        //}
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {
            foreach (Row row in rows)
            {
                ExistRFC = (string)row["Exist"];
                yield return row;
            }
        }
        public string getIfExistRFC()
        {
            return this.ExistRFC;
        }
    }

    public class ValidaItems : AbstractOperation
    {
        private List<Items> list;    
        private List<Conceptos> list2;    
        private string noIdentification;
        //Agregado por JL para la validacion de unidades    
        private string um;
        //
        private string tipoValid = "EXISTS";
        private string clienteId;
        private string exist;
        private Guid ConceptoId;
        //Agregado por JL para los conceptos faltantes
        private List<Items> listNewItem;
       // private List<Conceptos> listNewConcept;
        //
        //Agregado por JL para los conceptos faltantes, se agrega listaNewItem
        public ValidaItems(List<Items> ListItems, List<Items> listaNewItem)
        {
            this.list = ListItems;
            //Agregado por JL para los conceptos faltantes
            this.listNewItem = listaNewItem;
        }
        //Agregado por JL para los conceptos faltantes, se agrega listaNewConcept
        public ValidaItems(List<Conceptos> ListConceptos, List<Items> listaNewConcept, string tipoValidacion)
        {
            this.list2 = ListConceptos;
            this.tipoValid = tipoValidacion;
            //Agregado por JL para los conceptos faltantes
            // this.listNewConcept = new List<ETLCountryPack.Conceptos>();//listaNewConcept;
            this.list = new List<Items>() ;
            this.listNewItem = listaNewConcept;
        }
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {

            foreach (Row row in rows)
            {
                if (tipoValid == "UPDATE")
                {
                    noIdentification = (string)row["noIdentification"];
                    clienteId = (string)row["clienteId"];
                    var descripcion = (string)row["description"];
                    //Agregado por JL para la validacion de unidades    
                    um = (string)row["unitId"];
                    //
                    string sequence = row["sequence"].ToString();
                    Console.WriteLine("UPDATEValidaItems-----");
                    Console.WriteLine(noIdentification);
                    ExNihiloExistItems ExistItems = new ExNihiloExistItems(noIdentification, sequence, tipoValid);
                    ExistItems.UseTransaction = false;
                    ExistItems.Execute();
                    if (ExistItems.GetAllErrors().Count() > 0)
                    {
                        foreach (Exception ex in ExistItems.GetAllErrors())
                        {
                            Console.WriteLine(ex.Message);
                        }
                    }
                    if (ExistItems.ObjgetItems != null)
                    {
                        if (ExistItems.ObjgetItems.getIfExistItem() != null)
                        {
                            exist = ExistItems.ObjgetItems.getIfExistItem();                          

                            if (exist=="1")
                            { 
                            this.list.Add(new Items() { item = noIdentification, existe = exist, unidad = um, descripcion = descripcion,sequence=sequence });
                            // this.list.Add(new Conceptos() { conceptoId = (Guid)row["conceptId"], item = (string)row["noIdentification"], linea = int.Parse(row["sequence"].ToString()), dbase = (decimal)row["total"] });
                            Console.WriteLine("existe item");
                            if (ExistItems.ObjgetItems.getConceptoId() != null)
                            {
                                ConceptoId = ExistItems.ObjgetItems.getConceptoId();
                            }
                            this.list.Add(new Items() { item = noIdentification, existe = exist, unidad = um, descripcion = descripcion , sequence = sequence });
                            
                            }
                            else
                            {
                                //Agregado por JL para la actualizacion de los conceptos
                                //Realizar la insercion de los conceptos que no esten registrados                        
                                //this.listNewConcept.Add(new Conceptos() { item = noIdentification, conceptoId = ConceptoId, linea = int.Parse(sequence), dbase = decimal.Parse("0") });
                                Console.WriteLine("Agregando nuevos items");
                                this.listNewItem.Add(new Items() { item = noIdentification, existe = "1", unidad = um, descripcion = descripcion,sequence=sequence});
                            }
                            this.list2.Add(new Conceptos() { item = noIdentification, conceptoId = ConceptoId, linea = int.Parse(sequence), dbase = decimal.Parse(exist) });
                        }                        
                    }
                    
                }
                else
                {
                    noIdentification = (string)row["noIdentification"];
                    clienteId = (string)row["clienteId"];
                    //Agregado por JL para la validacion de unidades    
                    um = (string)row["unitId"];
                    //
                    ExNihiloExistItems ExistItems = new ExNihiloExistItems(noIdentification, clienteId);
                    ExistItems.UseTransaction = false;
                    ExistItems.Execute();
                    if (ExistItems.GetAllErrors().Count() > 0)
                    {
                        foreach (Exception ex in ExistItems.GetAllErrors())
                        {
                            Console.WriteLine(ex.Message);
                        }
                    }

                  
                    if (ExistItems.ObjgetItems.getIfExistItem() != null)
                    {
                        exist = ExistItems.ObjgetItems.getIfExistItem();
                        if (exist == "0")
                            msg.bShowMsg = true;
                    }
                    else
                        msg.bShowMsg = true;

                    //Agregado por jl para la validacion de unidad              
                    this.list.Add(new Items() { item = noIdentification, existe = exist , unidad = um});
                    
                }

                yield return row;
            }
        }
    }
    //Agregado por jl para la validacion de las unidades
    public class Existum : AbstractOperation
    {
        public string Existuni;
    
        private string tipoValid;
        public Existum(string tipoValidacion)
        {
            this.tipoValid = tipoValidacion;            
        }
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {
            foreach (Row row in rows)
            {               
                    Existuni = (string)row["Exist"];
                yield return row;
            }
        }
        public string getIfExistum()
        {
            return this.Existuni;
        }
      

    }
    public class ExistItems : AbstractOperation
    {
        public string ExistItem;
        public Guid conceptoId;
        private string tipoValid;
        public ExistItems(string tipoValidacion)
        {
            this.tipoValid = tipoValidacion;
        }
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {
            foreach (Row row in rows)
            {
                if (tipoValid == "UPDATE")
                {
                    ExistItem = (string)row["Exist"];
                    conceptoId = (Guid)row["conceptId"];
                }
                else
                    ExistItem = (string)row["Exist"];
                yield return row;
            }
        }
        public string getIfExistItem()
        {
            return this.ExistItem;
        }
        public Guid getConceptoId()
        {
            return this.conceptoId;
        }

    }
    public class ValidaTaxes : AbstractOperation
    {
        private List<Taxes> list;
        private string claveImpuesto;

        //private string clienteId;
        private string exist;
        public ValidaTaxes(List<Taxes> ListTaxes)
        {
            this.list = ListTaxes;
        }
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {

            foreach (Row row in rows)
            {
                claveImpuesto = (string)row["claveImpuesto"];
                ExNihiloExistTaxes ExistTaxes = new ExNihiloExistTaxes(claveImpuesto);
                ExistTaxes.UseTransaction = false;
                ExistTaxes.Execute();
                if (ExistTaxes.GetAllErrors().Count() > 0)
                {
                    foreach (Exception ex in ExistTaxes.GetAllErrors())
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
                if (ExistTaxes.ObjgetTaxes.getIfExistTaxe() != null)
                {
                    exist = ExistTaxes.ObjgetTaxes.getIfExistTaxe();
                    if (exist == "0")
                        msg.bShowMsg = true;
                }
                else
                    msg.bShowMsg = true;
                this.list.Add(new Taxes { impuesto = claveImpuesto, existe = exist });

                yield return row;
            }
        }
    }

    //Agregado por jl para recuperar el sitio
    public class GetSite : AbstractOperation
    {       
       
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {
            foreach (Row row in rows)
            {
                GlobalStrings.SiteRef = (string)row["Site"];
                GlobalStrings.Parm_Site=(string)row["Site"];               
                yield return row;
            }
        }
        public string getSite()
        {
           
            return GlobalStrings.Parm_Site;
        }
    }

    //Agregado por JL para la recuperacion del sitio
    public class ExNihiloGetSitio : EtlProcess
    {
        private string querySelect;
        public ExNihiloGetSitio(string InputData)
        {
            this.querySelect = InputData;

        }
        protected override void Initialize()
        {
            Register(new ConventionInputCommandOperation(ConnectionData.ConexionSettingCFDI)
            {
                Command = querySelect,
                Timeout=9000000
            });
            
            Register(new GetSite());           
          
        }
    }
    //Process
    public class ExNihiloValidaRFC : EtlProcess
    {
        private string querySelect;
        private List<Rfc> list;
        public ExNihiloValidaRFC(string InputData, List<Rfc> listRfcs)
        {
            this.querySelect = InputData;
            this.list = listRfcs;
        }
        protected override void Initialize()
        {
            Register(new ExtractData(querySelect));
            Register(new ValidaRFC(list));
        }
        public List<Rfc> Getlista()
        {
            return this.list;
        }
    }

    //Agregado por JL para la validacion rfc pagos
    public class ExNihiloExistRFCPayment : EtlProcess
    {       
        private string idCliente;
        private string querySelect;
        public ExisteRFC ObjgetRFC;
        public ExNihiloExistRFCPayment(string clienteId)
        {       
        this.idCliente = clienteId;
        }
        protected override void Initialize()
        {          
            querySelect = string.Format(ValStrings.validaRfcCustomerPayment, idCliente);
            Register(new ConventionInputCommandOperation(ConnectionData.ConexionSettingCFDI)
            {
                Command = querySelect,
                Timeout = 900000
            });
            this.ObjgetRFC = new ExisteRFC();
            Register(ObjgetRFC);
        }
    }

    public class ExNihiloExistRFC : EtlProcess
    {
        private string tipoRfc;
        private string RFC;
        private string idCliente;
        private string querySelect;
        public ExisteRFC ObjgetRFC;

      
        public ExNihiloExistRFC(string tiporfc, string rfc, string clienteId)
        {
            this.tipoRfc = tiporfc;
            this.RFC = rfc;
            this.idCliente = clienteId;
        }
        protected override void Initialize()
        {
            if (tipoRfc == "Company")
                querySelect = string.Format(ValStrings.validaRfcCompany, RFC);
            if (tipoRfc == "Customer")
                querySelect = string.Format(ValStrings.validaRfcCustomer, RFC, idCliente);
            Register(new ConventionInputCommandOperation(ConnectionData.ConexionSettingCFDI)
            {
                Command = querySelect,
                Timeout=900000
            });
            this.ObjgetRFC = new ExisteRFC();
            Register(ObjgetRFC);
        }
    }

    
    //Agragado por JL para la validacion de las unidades
    public class ExNihiloExistum : EtlProcess
    {
        private string querySelect;
        public Existum Objgetum;
        private string tipoValid = "EXISTS";
     
        public ExNihiloExistum(string item)
        {
           
            this.querySelect = string.Format(ValStrings.validaum, item);
            
        }       
        protected override void Initialize()
        {
          
            Register(new ConventionInputCommandOperation(ConnectionData.ConexionSettingCFDI)
            {

                Command = querySelect,
                Timeout = 900000
            });
         
            this.Objgetum = new Existum(tipoValid);
            Register(Objgetum);
        }
    }
    public class ExNihiloValidaItems : EtlProcess
    {
        private string querySelect;
        private List<Items> list;       
        private List<Conceptos> list2;
        private string tipoValid = "EXISTS";
        //Agregado por JL para los conceptos faltantes
        private List<Items> listNewItem;
        private List<Items> listNewConcept;
        //
        //Agregado por JL para los conceptos faltantes,se agrega ListaNewItems
        public ExNihiloValidaItems(string InputData, List<Items> ListaItems, List<Items> ListaNewItems)
        {
            this.querySelect = InputData;
            this.list = ListaItems;
            //Agregado por JL para los conceptos faltantes
            this.listNewItem = ListaNewItems;
        }
        //Agregado por JL para los conceptos faltantes, se agrega listaNewConcept
        public ExNihiloValidaItems(string InputData, List<Conceptos> ListaConceptos, List<Items> ListaNewConceptos, string tipoValidacion)
        {
            this.querySelect = InputData;
            this.list2 = ListaConceptos;
            this.tipoValid = tipoValidacion;
            //Agregado por JL para los conceptos faltantes
            this.listNewConcept = ListaNewConceptos;
        }
        protected override void Initialize()
        {
           
            Register(new ExtractData(querySelect));
            if (tipoValid == "UPDATE")
            {
              Register(new ValidaItems(list2, listNewConcept, tipoValid)); //Para obtener los Item para el UPDATE            
            }
            else
                Register(new ValidaItems(list,listNewItem));
        }
        public List<Items> Getlista()
        {
            return this.list;
        }
       
        public List<Conceptos> GetlistaConceptos()
        {
            return this.list2;
        }
        //Agregado por JL para los conceptos faltantes
        //public List<Items> GetNewlista()
        //{
        //    return this.listNewItem;
        //}
        public List<Items> GetlistaNewConceptos()
        {
            return this.listNewConcept;
        }
        //

    }
    public class ExNihiloExistItems : EtlProcess
    {
        private string querySelect;
        public ExistItems ObjgetItems;
        private string tipoValid = "EXISTS";
        public ExNihiloExistItems(string item, string customerId)
        {
            this.querySelect = string.Format(ValStrings.validaItems, item, customerId);
        }
        public ExNihiloExistItems(string item, string sequence, string tipoValidacion)//Update
        {
            this.querySelect = string.Format(ValStrings.validaConceptos, item, GlobalStrings.comprobanteId, sequence);
            this.tipoValid = tipoValidacion;
        }
        protected override void Initialize()
        {
            Register(new ConventionInputCommandOperation(ConnectionData.ConexionSettingCFDI)
            {
                Command = querySelect,
                Timeout = 900000
            });
            this.ObjgetItems = new ExistItems(tipoValid);
            Register(ObjgetItems);
        }
    }
    public class ExNihiloValidaTaxes : EtlProcess
    {
        private string querySelect;
        private List<Taxes> list;
        public ExNihiloValidaTaxes(string InputData, List<Taxes> ListaImpuestos)
        {
            this.querySelect = InputData;
            this.list = ListaImpuestos;
        }
        protected override void Initialize()
        {

            Register(new ExtractData(querySelect));
            Register(new ValidaTaxes(list));
        }
        public List<Taxes> Getlista()
        {
            return this.list;
        }
    }
    public class ExistTaxes : AbstractOperation
    {
        public string ExistTaxe;
        //public ExistItems(string tipoRfc)
        //{
        //    this.tipoRFC = tipoRfc;
        //}
        public override IEnumerable<Row> Execute(IEnumerable<Row> rows)
        {
            foreach (Row row in rows)
            {
                ExistTaxe = (string)row["Exist"];
                yield return row;
            }
        }
        public string getIfExistTaxe()
        {
            return this.ExistTaxe;
        }
    }
    public class ExNihiloExistTaxes : EtlProcess
    {
        private string querySelect;
        public ExistTaxes ObjgetTaxes;
        public ExNihiloExistTaxes(string claveImpuesto)
        {
            this.querySelect = string.Format(ValStrings.validaTaxes, claveImpuesto);
        }
        protected override void Initialize()
        {
            Register(new ConventionInputCommandOperation(ConnectionData.ConexionSettingCFDI)
            {
                Command = querySelect,
                Timeout = 900000
            });
            this.ObjgetTaxes = new ExistTaxes();
            Register(ObjgetTaxes);
        }
    }
}
